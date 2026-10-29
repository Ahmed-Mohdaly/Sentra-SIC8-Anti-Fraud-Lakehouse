"""Remediated Production Fraud Detection ML Training Pipeline.

Fixes all data leakages and train-serve skews:
1. Strict 3-Way Chronological Split:
   - Training: <= 2016
   - Validation: 2017 (Tuning threshold & hyperparameters only)
   - Held-Out Test: 2018-2019 (Untouched until final frozen evaluation)
2. Leakage-Free Target Encoding:
   - mcc_risk_score fitted strictly on <= 2016 with Empirical Bayes smoothing.
3. Leakage-Free User Spend Baseline:
   - User avg and std spend computed sequentially using prior transactions only.
4. Active, Zero-Skew Velocity Features:
   - True rolling sliding-window velocity features (time_since_last_tx_sec, travel_speed_kmh,
     tx_count_last_1h, tx_count_last_24h, spend_sum_last_1h) matching StateStore definitions.
5. Preserves original baseline artifacts in models/ and writes new artifacts to models/v2/.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
from lightgbm import LGBMClassifier
from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    matthews_corrcoef,
    precision_recall_curve,
    roc_auc_score,
)

from ml.feature_engineering import (
    FEATURE_NAMES,
    GLOBAL_DEFAULTS,
    FeatureExtractor,
    clean_currency,
    haversine_distance,
)
from synthetic_streaming.classification_and_geo import CityResolver, sample_entry_method

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("train_fraud_remediated")


def load_city_resolver(base_dir: Path) -> CityResolver:
    resolver = CityResolver()
    city_file = base_dir / "city_reference.csv"
    if not city_file.exists():
        city_file = base_dir / "data" / "city_reference.csv"
    if city_file.exists():
        resolver.load_from_csv(city_file)
        logger.info("Loaded %d city coordinates from %s", len(resolver), city_file)
    return resolver


def load_reference_data(base_dir: Path) -> Tuple[Dict[int, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    users_dict: Dict[int, Dict[str, Any]] = {}
    cards_dict: Dict[str, Dict[str, Any]] = {}

    users_file = base_dir / "sd254_users.csv"
    if not users_file.exists():
        users_file = base_dir / "data" / "sd254_users.csv"
    if users_file.exists():
        with open(users_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                users_dict[idx] = {
                    "user_id": idx,
                    "Current Age": float(row.get("Current Age", 40)),
                    "FICO Score": float(row.get("FICO Score", 700)),
                    "Yearly Income - Person": row.get("Yearly Income - Person", "$50000"),
                    "Total Debt": row.get("Total Debt", "$20000"),
                    "Num Credit Cards": float(row.get("Num Credit Cards", 3)),
                    "Latitude": float(row.get("Latitude", 0.0) or 0.0),
                    "Longitude": float(row.get("Longitude", 0.0) or 0.0),
                    "City": row.get("City", ""),
                    "State": row.get("State", ""),
                }
        logger.info("Loaded %d reference users", len(users_dict))

    cards_file = base_dir / "sd254_cards.csv"
    if not cards_file.exists():
        cards_file = base_dir / "data" / "sd254_cards.csv"
    if cards_file.exists():
        with open(cards_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    u_id = int(row.get("User", 0))
                    c_idx = int(row.get("CARD INDEX", 0))
                except (ValueError, TypeError):
                    continue
                cards_dict[f"{u_id}:{c_idx}"] = {
                    "user_id": u_id,
                    "card_index": c_idx,
                    "Card Brand": row.get("Card Brand", "Visa"),
                    "Card Type": row.get("Card Type", "Debit"),
                    "Has Chip": row.get("Has Chip", "YES"),
                    "Credit Limit": row.get("Credit Limit", "$5000"),
                    "Card on Dark Web": row.get("Card on Dark Web", "No"),
                    "Acct Open Date": row.get("Acct Open Date", "01/2000"),
                    "Year PIN last Changed": row.get("Year PIN last Changed", 2000),
                }
        logger.info("Loaded %d reference cards", len(cards_dict))

    return users_dict, cards_dict


def parse_epoch(year: int, month: int, day: int, time_str: str) -> float:
    """Compute epoch seconds accurately."""
    try:
        parts = time_str.split(":")
        hh = int(parts[0])
        mm = int(parts[1]) if len(parts) > 1 else 0
        dt = datetime(year, month, day, hh, mm)
        return dt.timestamp()
    except Exception:
        # Fallback approximation: 86400 seconds per day
        return float(year * 31536000 + month * 2592000 + day * 86400)


def stream_sample_and_extract_features(
    csv_path: Path,
    city_resolver: CityResolver,
    users_dict: Dict[int, Dict[str, Any]],
    cards_dict: Dict[str, Dict[str, Any]],
    sample_train_normal: int = 275000,
    sample_val_normal: int = 30000,
    sample_test_normal: int = 54181,
    seed: int = 42,
) -> Tuple[
    Tuple[np.ndarray, np.ndarray],
    Tuple[np.ndarray, np.ndarray],
    Tuple[np.ndarray, np.ndarray],
    Dict[int, float],
    Dict[str, Any]
]:
    """Single-pass streaming across full CSV with exact zero-skew velocity & leakage-safe feature calculation."""
    logger.info("Starting single-pass streaming across %s ...", csv_path)
    t0 = time.time()

    rng = random.Random(seed)

    # Approximate sampling probabilities based on known population counts:
    # Train (<=2016): ~18.85M normals -> target 275,000 (~0.0146)
    # Val (2017): ~1.72M normals -> target 30,000 (~0.0174)
    # Test (2018-2019): ~3.44M normals -> target 54,181 (~0.01575)
    prob_train = sample_train_normal / 18856563.0
    prob_val = sample_val_normal / 1723105.0
    prob_test = sample_test_normal / 3440975.0

    # Intermediate storage for sampled records
    train_records: List[Tuple[Dict[str, Any], Dict[str, Any], int]] = []
    val_records: List[Tuple[Dict[str, Any], Dict[str, Any], int]] = []
    test_records: List[Tuple[Dict[str, Any], Dict[str, Any], int]] = []

    # Historical MCC counters strictly for Training era (<= 2016)
    train_mcc_counts: Dict[int, int] = {}
    train_mcc_frauds: Dict[int, int] = {}
    train_total_tx = 0
    train_total_fraud = 0

    # Per-card sliding window state
    curr_card: Optional[Tuple[int, int]] = None
    card_history: List[Tuple[float, float]] = []  # list of (epoch, amount)
    last_tx_info: Optional[Dict[str, Any]] = None  # {epoch, lat, lon}

    # Running user spend state: user_id -> [count, sum, sum_squares]
    user_spend_state: Dict[int, List[float]] = {}

    total_scanned = 0
    frauds_by_era = {"train": 0, "val": 0, "test": 0, "other": 0}

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_scanned += 1

            u_id = int(row.get("User", 0))
            c_idx = int(row.get("Card", 0))
            card_key = (u_id, c_idx)

            # Detect card boundaries
            if card_key != curr_card:
                curr_card = card_key
                card_history.clear()
                last_tx_info = None

            # Parse date & time
            year = int(row.get("Year", 2015))
            month = int(row.get("Month", 1))
            day = int(row.get("Day", 1))
            time_str = str(row.get("Time", "12:00")).strip()
            epoch_sec = parse_epoch(year, month, day, time_str)
            amount = clean_currency(row.get("Amount"), 0.0)

            # Determine fraud label
            is_fraud_val = str(row.get("Is Fraud?", "No")).strip().lower()
            is_fraud = 1 if is_fraud_val in ("yes", "1", "true") else 0

            # Determine split
            if year <= 2016:
                era = "train"
                train_total_tx += 1
                if is_fraud:
                    train_total_fraud += 1
                try:
                    mcc = int(row.get("MCC", 0))
                    train_mcc_counts[mcc] = train_mcc_counts.get(mcc, 0) + 1
                    if is_fraud:
                        train_mcc_frauds[mcc] = train_mcc_frauds.get(mcc, 0) + 1
                except (ValueError, TypeError):
                    pass
            elif year == 2017:
                era = "val"
            elif year in (2018, 2019):
                era = "test"
            else:
                era = "other"

            if is_fraud:
                frauds_by_era[era] += 1

            # Resolve merchant coordinates
            m_city = row.get("Merchant City")
            m_state = row.get("Merchant State")
            m_lat, m_lon = city_resolver.resolve(m_city, m_state)

            # 1. Compute exact velocity state matching StateStore
            if last_tx_info:
                time_since_last_tx_sec = max(0.1, epoch_sec - last_tx_info["epoch"])
                prev_m_lat = last_tx_info.get("lat")
                prev_m_lon = last_tx_info.get("lon")
            else:
                time_since_last_tx_sec = 86400.0
                prev_m_lat = None
                prev_m_lon = None

            cutoff_1h = epoch_sec - 3600.0
            cutoff_24h = epoch_sec - 86400.0

            # Prune transactions older than 24h
            card_history = [e for e in card_history if e[0] >= cutoff_24h]
            c_1h = sum(1 for e in card_history if e[0] >= cutoff_1h)
            s_1h = sum(e[1] for e in card_history if e[0] >= cutoff_1h) + amount

            tx_count_1h = float(c_1h + 1)
            tx_count_24h = float(len(card_history) + 1)
            spend_sum_1h = s_1h

            # 2. Compute running user spend baseline prior to current transaction
            ustats = user_spend_state.get(u_id)
            if ustats and ustats[0] > 0:
                u_avg = ustats[1] / ustats[0]
                if ustats[0] >= 2:
                    u_var = (ustats[2] - (ustats[1] ** 2) / ustats[0]) / (ustats[0] - 1)
                    u_std = math.sqrt(max(u_var, 1.0))
                else:
                    u_std = GLOBAL_DEFAULTS["user_std_spend"]
            else:
                u_avg = GLOBAL_DEFAULTS["user_avg_spend"]
                u_std = GLOBAL_DEFAULTS["user_std_spend"]

            # 3. Sampling Decision
            retain = False
            if era in ("train", "val", "test"):
                if is_fraud:
                    retain = True  # Retain 100% of frauds
                else:
                    # Bernoulli sampling with era-specific probability
                    p = prob_train if era == "train" else (prob_val if era == "val" else prob_test)
                    if rng.random() < p:
                        retain = True

            if retain:
                state_dict = {
                    "time_since_last_tx_sec": time_since_last_tx_sec,
                    "prev_merchant_lat": prev_m_lat,
                    "prev_merchant_lon": prev_m_lon,
                    "tx_count_last_1h": tx_count_1h,
                    "tx_count_last_24h": tx_count_24h,
                    "spend_sum_last_1h": spend_sum_1h,
                    "user_avg_spend": u_avg,
                    "user_std_spend": u_std,
                }
                payload_dict = dict(row)
                if m_lat is not None and m_lon is not None:
                    payload_dict["Merchant Latitude"] = m_lat
                    payload_dict["Merchant Longitude"] = m_lon

                if era == "train":
                    train_records.append((payload_dict, state_dict, is_fraud))
                elif era == "val":
                    val_records.append((payload_dict, state_dict, is_fraud))
                elif era == "test":
                    test_records.append((payload_dict, state_dict, is_fraud))

            # 4. Update rolling history after computing features for current transaction
            card_history.append((epoch_sec, amount))
            last_tx_info = {"epoch": epoch_sec, "lat": m_lat, "lon": m_lon}

            # Update running user spend statistics
            if u_id not in user_spend_state:
                user_spend_state[u_id] = [1.0, amount, amount * amount]
            else:
                user_spend_state[u_id][0] += 1.0
                user_spend_state[u_id][1] += amount
                user_spend_state[u_id][2] += amount * amount

            if total_scanned % 4000000 == 0:
                logger.info(
                    "Scanned %d / 24,386,900 rows (%.1f%%) in %.1fs | Retained: train=%d, val=%d, test=%d",
                    total_scanned, (total_scanned / 24386900.0) * 100, time.time() - t0,
                    len(train_records), len(val_records), len(test_records)
                )

    logger.info("Scanning completed! Total scanned: %d rows in %.1fs", total_scanned, time.time() - t0)
    logger.info("Frauds found across eras: %s", frauds_by_era)
    logger.info("Retained records: Train=%d, Val=%d, Test=%d", len(train_records), len(val_records), len(test_records))

    # Compute Leakage-Free MCC Risk Map strictly from Training era (<= 2016)
    global_train_rate = train_total_fraud / max(train_total_tx, 1)
    smoothing = 50.0  # Empirical Bayes smoothing parameter
    mcc_risk_map: Dict[int, float] = {}
    for mcc, count in train_mcc_counts.items():
        fr = train_mcc_frauds.get(mcc, 0)
        smoothed = (fr + smoothing * global_train_rate) / (count + smoothing)
        mcc_risk_map[mcc] = float(round(smoothed, 5))

    logger.info(
        "Computed leakage-free MCC risk scores for %d categories strictly on <=2016 (global train rate: %.5f)",
        len(mcc_risk_map), global_train_rate
    )

    # Feature Extractor initialized strictly with train MCC map
    extractor = FeatureExtractor(
        mcc_risk_map=mcc_risk_map,
        global_stats={**GLOBAL_DEFAULTS, "global_fraud_rate": global_train_rate}
    )

    def records_to_matrix(recs: List[Tuple[Dict[str, Any], Dict[str, Any], int]]) -> Tuple[np.ndarray, np.ndarray]:
        X_list: List[List[float]] = []
        y_list: List[int] = []
        for idx, (payload, state, y_val) in enumerate(recs):
            u_id = int(payload.get("User", 0))
            c_idx = int(payload.get("Card", 0))
            card_key = f"{u_id}:{c_idx}"
            user_p = users_dict.get(u_id, {})
            card_p = cards_dict.get(card_key, {})

            use_chip = payload.get("Use Chip", "Swipe Transaction")
            try:
                mcc_int = int(payload.get("MCC", 0))
            except (ValueError, TypeError):
                mcc_int = 0
            entry_method = sample_entry_method(
                use_chip=use_chip,
                source_system="pos_terminal",
                mcc=mcc_int,
                rng=random.Random(idx),
            )
            payload["Transaction Entry Method"] = entry_method

            fdict = extractor.extract_online_features(
                payload=payload,
                user_profile=user_p,
                card_profile=card_p,
                state=state,
            )
            X_list.append(extractor.to_feature_vector(fdict))
            y_list.append(y_val)

        return np.array(X_list, dtype=np.float32), np.array(y_list, dtype=np.int32)

    logger.info("Converting retained records to aligned 34-feature matrices...")
    X_train, y_train = records_to_matrix(train_records)
    X_val, y_val = records_to_matrix(val_records)
    X_test, y_test = records_to_matrix(test_records)

    logger.info("X_train shape: %s | Frauds: %d (%.2f%%)", X_train.shape, int(np.sum(y_train)), (np.sum(y_train) / len(y_train)) * 100)
    logger.info("X_val shape:   %s | Frauds: %d (%.2f%%)", X_val.shape, int(np.sum(y_val)), (np.sum(y_val) / len(y_val)) * 100)
    logger.info("X_test shape:  %s | Frauds: %d (%.2f%%)", X_test.shape, int(np.sum(y_test)), (np.sum(y_test) / len(y_test)) * 100)

    dataset_summary = {
        "train_samples": len(y_train),
        "train_frauds": int(np.sum(y_train)),
        "val_samples": len(y_val),
        "val_frauds": int(np.sum(y_val)),
        "test_samples": len(y_test),
        "test_frauds": int(np.sum(y_test)),
        "global_train_rate": global_train_rate,
        "mcc_categories_encoded": len(mcc_risk_map),
    }

    return (X_train, y_train), (X_val, y_val), (X_test, y_test), mcc_risk_map, dataset_summary


def train_and_optimize(
    train_data: Tuple[np.ndarray, np.ndarray],
    val_data: Tuple[np.ndarray, np.ndarray],
    test_data: Tuple[np.ndarray, np.ndarray],
    mcc_risk_map: Dict[int, float],
    output_dir: Path,
    baseline_model_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Train candidate model on Train (<=2016), tune threshold strictly on Val (2017), and evaluate on frozen Test (2018-2019)."""
    X_train, y_train = train_data
    X_val, y_val = val_data
    X_test, y_test = test_data

    output_dir.mkdir(parents=True, exist_ok=True)

    n_pos_train = int(np.sum(y_train))
    n_neg_train = len(y_train) - n_pos_train
    scale_pos = max(1.0, float(n_neg_train) / max(1.0, float(n_pos_train)))

    logger.info("Fitting LightGBM candidate on Train (<=2016)... scale_pos_weight=%.2f", scale_pos)
    clf = LGBMClassifier(
        n_estimators=450,
        learning_rate=0.035,
        num_leaves=95,
        max_depth=9,
        min_child_samples=35,
        subsample=0.85,
        colsample_bytree=0.85,
        scale_pos_weight=scale_pos,
        random_state=42,
        n_jobs=-1,
        importance_type="gain",
    )

    t0 = time.time()
    # Train using Validation set (2017) strictly for monitoring convergence (NOT test set!)
    clf.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        eval_metric="average_precision",
    )
    train_duration = time.time() - t0
    logger.info("Training completed in %.2fs", train_duration)

    # --------------------------------------------------------------------------
    # Step 1: Decision Threshold Optimization STRICTLY on Validation Set (2017)
    # --------------------------------------------------------------------------
    logger.info("Tuning decision threshold strictly on Validation Set (2017)...")
    val_probs = clf.predict_proba(X_val)[:, 1]
    val_roc_auc = float(roc_auc_score(y_val, val_probs))
    val_pr_auc = float(average_precision_score(y_val, val_probs))
    logger.info("Validation ROC-AUC: %.4f | Validation PR-AUC: %.4f", val_roc_auc, val_pr_auc)

    # Cost Model Parameters
    c_fraud = 120.00
    c_review = 3.50
    c_friction = 1.50
    c_fp = c_review + c_friction  # $5.00 per false alarm

    precisions_val, recalls_val, thresholds_val = precision_recall_curve(y_val, val_probs)

    candidate_results = []
    # Test candidate thresholds from 0.01 to 0.95
    scan_thresholds = np.unique(np.concatenate([
        np.linspace(0.01, 0.20, 40),
        np.linspace(0.20, 0.80, 60),
        thresholds_val[::max(1, len(thresholds_val) // 50)]
    ]))

    for th in sorted(scan_thresholds):
        preds = (val_probs >= th).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_val, preds, labels=[0, 1]).ravel()
        rec = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
        prec = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
        fpr = float(fp / (fp + tn)) if (fp + tn) > 0 else 0.0
        f1 = float(f1_score(y_val, preds, zero_division=0))
        # Expected financial loss on validation cohort
        cost = (fn * c_fraud) + (fp * c_fp)
        candidate_results.append({
            "threshold": float(round(th, 4)),
            "recall": rec,
            "precision": prec,
            "fpr": fpr,
            "f1": f1,
            "expected_cost": cost,
            "tp": int(tp), "fp": int(fp), "fn": int(fn), "tn": int(tn)
        })

    # Strategy A: Minimum Expected Cost on Validation
    min_cost_cand = min(candidate_results, key=lambda x: x["expected_cost"])

    # Strategy B: Target Recall >= 80% with minimal FPR on Validation
    high_rec_cands = [c for c in candidate_results if c["recall"] >= 0.80]
    if high_rec_cands:
        balanced_cand = min(high_rec_cands, key=lambda x: (x["fpr"], -x["f1"]))
    else:
        balanced_cand = min_cost_cand

    # Recommended operational threshold: balanced candidate targeting high fraud recall with controlled FPR
    optimal_threshold = balanced_cand["threshold"]
    logger.info(
        "Optimal threshold selected on Validation Set: %.4f (Val Recall: %.2f%%, Val FPR: %.2f%%, Val Cost: $%.2f)",
        optimal_threshold, balanced_cand["recall"] * 100, balanced_cand["fpr"] * 100, balanced_cand["expected_cost"]
    )

    # --------------------------------------------------------------------------
    # Step 2: Final Evaluation on Held-Out Test Set (2018-2019)
    # --------------------------------------------------------------------------
    logger.info("Executing final locked evaluation on untouched Held-Out Test Set (2018-2019)...")
    test_probs = clf.predict_proba(X_test)[:, 1]
    test_roc_auc = float(roc_auc_score(y_test, test_probs))
    test_pr_auc = float(average_precision_score(y_test, test_probs))

    # Evaluate at optimal_threshold
    test_preds = (test_probs >= optimal_threshold).astype(int)
    cm = confusion_matrix(y_test, test_preds, labels=[0, 1])
    tn, fp, fn, tp = cm.ravel()

    recall = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
    precision = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    specificity = float(tn / (tn + fp)) if (tn + fp) > 0 else 0.0
    fpr = float(fp / (fp + tn)) if (fp + tn) > 0 else 0.0
    fnr = float(fn / (fn + tp)) if (fn + tp) > 0 else 0.0
    f1 = float(f1_score(y_test, test_preds, zero_division=0))
    bal_acc = float(balanced_accuracy_score(y_test, test_preds))
    mcc_val = float(matthews_corrcoef(y_test, test_preds))

    # Real-World Production Prevalence Adjustment (Population Prevalence = 0.1220%)
    pop_prevalence = 0.0012202
    adj_tp_rate = pop_prevalence * recall
    adj_fp_rate = (1.0 - pop_prevalence) * fpr
    adj_precision = adj_tp_rate / max(adj_tp_rate + adj_fp_rate, 1e-9)
    adj_f1 = (2.0 * adj_precision * recall) / max(adj_precision + recall, 1e-9)
    false_alert_ratio = adj_fp_rate / max(adj_tp_rate, 1e-9)

    logger.info("=== HELD-OUT TEST EVALUATION RESULTS (Sampled Prevalence: %.2f%%) ===", (np.sum(y_test) / len(y_test)) * 100)
    logger.info("  Optimal Threshold:      %.4f", optimal_threshold)
    logger.info("  ROC-AUC:                %.4f", test_roc_auc)
    logger.info("  PR-AUC (Avg Precision): %.4f", test_pr_auc)
    logger.info("  Recall (Catch Rate):    %.2f%% (%d of %d caught)", recall * 100, tp, tp + fn)
    logger.info("  Precision:              %.2f%%", precision * 100)
    logger.info("  False Positive Rate:    %.2f%% (%d false alarms)", fpr * 100, fp)
    logger.info("  Specificity:            %.2f%%", specificity * 100)
    logger.info("  F1-Score:               %.4f", f1)
    logger.info("  Balanced Accuracy:      %.4f", bal_acc)
    logger.info("  Matthews Corr Coeff:    %.4f", mcc_val)
    logger.info("  Confusion Matrix:       TN=%d, FP=%d, FN=%d, TP=%d", tn, fp, fn, tp)
    logger.info("=== PRODUCTION PREVALENCE ESTIMATES (Base Rate: 0.1220%) ===")
    logger.info("  Production Precision:   %.3f%%", adj_precision * 100)
    logger.info("  Production F1-Score:    %.4f", adj_f1)
    logger.info("  False Alarm Ratio:      %.1f false alarms per 1 confirmed fraud", false_alert_ratio)

    # --------------------------------------------------------------------------
    # Step 3: Feature Importance Analysis (Verifying Active Velocity Features)
    # --------------------------------------------------------------------------
    importances = clf.feature_importances_
    sorted_idx = np.argsort(importances)[::-1]
    top_features = []
    logger.info("Feature Importance Ranking (All 34 features):")
    for rank, idx in enumerate(sorted_idx, start=1):
        fname = FEATURE_NAMES[idx]
        gain = float(importances[idx])
        top_features.append({"rank": rank, "feature": fname, "gain": gain})
        logger.info("  %2d. %-28s (Gain: %.2f)", rank, fname, gain)

    # Check that previously inactive features now have non-zero gain
    velocity_check = {}
    for vfeat in ["time_since_last_tx_sec", "tx_count_last_1h", "tx_count_last_24h", "travel_speed_kmh", "spend_sum_last_1h"]:
        v_idx = FEATURE_NAMES.index(vfeat)
        v_gain = float(importances[v_idx])
        velocity_check[vfeat] = v_gain
        logger.info("  -> Active Velocity Check: %s Gain = %.2f", vfeat, v_gain)

    # --------------------------------------------------------------------------
    # Step 4: Compare with Baseline Model if available
    # --------------------------------------------------------------------------
    baseline_comparison = {}
    if baseline_model_path and baseline_model_path.exists():
        try:
            logger.info("Loading preserved baseline model from %s for side-by-side test evaluation...", baseline_model_path)
            base_clf = joblib.load(baseline_model_path)
            base_probs = base_clf.predict_proba(X_test)[:, 1]
            base_roc_auc = float(roc_auc_score(y_test, base_probs))
            base_pr_auc = float(average_precision_score(y_test, base_probs))
            base_preds = (base_probs >= 0.066).astype(int)
            b_tn, b_fp, b_fn, b_tp = confusion_matrix(y_test, base_preds, labels=[0, 1]).ravel()
            b_rec = float(b_tp / (b_tp + b_fn))
            b_prec = float(b_tp / (b_tp + b_fp))
            b_fpr = float(b_fp / (b_fp + b_tn))
            b_f1 = float(f1_score(y_test, base_preds, zero_division=0))

            baseline_comparison = {
                "baseline_model_path": str(baseline_model_path),
                "baseline_threshold": 0.066,
                "baseline_test_roc_auc": base_roc_auc,
                "baseline_test_pr_auc": base_pr_auc,
                "baseline_test_recall": b_rec,
                "baseline_test_precision": b_prec,
                "baseline_test_fpr": b_fpr,
                "baseline_test_f1": b_f1,
                "baseline_confusion_matrix": {"tn": int(b_tn), "fp": int(b_fp), "fn": int(b_fn), "tp": int(b_tp)},
            }
            logger.info("Baseline Model on Test Set: ROC-AUC=%.4f, PR-AUC=%.4f, Recall=%.2f%%, Precision=%.2f%%, FPR=%.2f%%",
                        base_roc_auc, base_pr_auc, b_rec * 100, b_prec * 100, b_fpr * 100)
        except Exception as ex:
            logger.warning("Could not evaluate baseline model: %s", ex)

    # --------------------------------------------------------------------------
    # Step 5: Serialize Candidate Artifacts to models/v2/
    # --------------------------------------------------------------------------
    candidate_model_path = output_dir / "fraud_lgbm_model.joblib"
    joblib.dump(clf, candidate_model_path)
    logger.info("Saved candidate model artifact to %s", candidate_model_path)

    metadata = {
        "model_version": "v2_remediated",
        "trained_timestamp": datetime.now().isoformat(),
        "training_duration_seconds": train_duration,
        "feature_names": FEATURE_NAMES,
        "temporal_protocol": {
            "train_period": "<= 2016",
            "validation_period": "2017",
            "test_period": "2018-2019",
            "threshold_selection_source": "validation_set_2017",
        },
        "optimal_threshold": optimal_threshold,
        "validation_metrics": {
            "roc_auc": val_roc_auc,
            "pr_auc": val_pr_auc,
            "optimal_threshold": optimal_threshold,
            "recall": balanced_cand["recall"],
            "precision": balanced_cand["precision"],
            "fpr": balanced_cand["fpr"],
            "f1": balanced_cand["f1"],
            "expected_cost": balanced_cand["expected_cost"],
        },
        "test_metrics_sampled": {
            "roc_auc": test_roc_auc,
            "pr_auc": test_pr_auc,
            "recall": recall,
            "precision": precision,
            "specificity": specificity,
            "false_positive_rate": fpr,
            "false_negative_rate": fnr,
            "f1_score": f1,
            "balanced_accuracy": bal_acc,
            "mcc": mcc_val,
            "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
        },
        "test_metrics_production_prevalence": {
            "assumed_prevalence": pop_prevalence,
            "adjusted_precision": adj_precision,
            "adjusted_f1": adj_f1,
            "false_alarm_ratio": false_alert_ratio,
        },
        "velocity_feature_gains": velocity_check,
        "top_features": top_features,
        "baseline_comparison": baseline_comparison,
        "mcc_risk_map": mcc_risk_map,
        "global_defaults": GLOBAL_DEFAULTS,
    }

    meta_path = output_dir / "model_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    logger.info("Saved candidate model metadata to %s", meta_path)

    return metadata


def main():
    parser = argparse.ArgumentParser(description="Train Remediated Leakage-Free LightGBM Fraud Model.")
    parser.add_argument("--data-dir", type=str, default=".", help="Base directory with CSV files")
    parser.add_argument("--tx-file", type=str, default="credit_card_transactions-ibm_v2.csv", help="Transactions CSV")
    parser.add_argument("--output-dir", type=str, default="models/v2", help="Target directory for versioned artifacts")
    parser.add_argument("--baseline-model", type=str, default="models/fraud_lgbm_model.joblib", help="Preserved baseline artifact")
    parser.add_argument("--sample-train-normal", type=int, default=275000, help="Number of normal train records to sample")
    parser.add_argument("--sample-val-normal", type=int, default=30000, help="Number of normal val records to sample")
    parser.add_argument("--sample-test-normal", type=int, default=54181, help="Number of normal test records to sample")
    args = parser.parse_args()

    base_dir = Path(args.data_dir)
    tx_path = base_dir / args.tx_file
    if not tx_path.exists():
        logger.error("Transaction file not found: %s", tx_path)
        sys.exit(1)

    city_resolver = load_city_resolver(base_dir)
    users_dict, cards_dict = load_reference_data(base_dir)

    train_data, val_data, test_data, mcc_risk_map, summary = stream_sample_and_extract_features(
        csv_path=tx_path,
        city_resolver=city_resolver,
        users_dict=users_dict,
        cards_dict=cards_dict,
        sample_train_normal=args.sample_train_normal,
        sample_val_normal=args.sample_val_normal,
        sample_test_normal=args.sample_test_normal,
    )

    metadata = train_and_optimize(
        train_data=train_data,
        val_data=val_data,
        test_data=test_data,
        mcc_risk_map=mcc_risk_map,
        output_dir=Path(args.output_dir),
        baseline_model_path=Path(args.baseline_model),
    )

    logger.info("Remediation training and validation pipeline completed successfully!")


if __name__ == "__main__":
    main()
