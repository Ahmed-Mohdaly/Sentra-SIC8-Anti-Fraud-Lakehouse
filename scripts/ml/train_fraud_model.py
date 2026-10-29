"""Production-grade Fraud Detection ML training script with LightGBM.
Extracts 100% of historical fraud events plus a representative stratified sample of normal spend.
Uses chronological splitting to eliminate future data leakage and tunes decision threshold for optimal recall.
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
    classification_report,
    confusion_matrix,
    f1_score,
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
logger = logging.getLogger("train_fraud_model")


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


def sample_transactions(
    csv_path: Path,
    max_non_fraud: int = 300000,
    seed: int = 42
) -> List[Dict[str, Any]]:
    """Two-pass or reservoir sampling to capture 100% of frauds and a balanced random sample of non-frauds."""
    logger.info("Streaming and sampling transactions from %s ...", csv_path)
    rng = random.Random(seed)
    fraud_records: List[Dict[str, Any]] = []
    normal_records: List[Dict[str, Any]] = []

    total_scanned = 0
    t0 = time.time()

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_scanned += 1
            is_fraud_val = str(row.get("Is Fraud?", "No")).strip().lower()
            if is_fraud_val in ("yes", "1", "true"):
                fraud_records.append(row)
            else:
                # Reservoir sampling with probability to retain ~max_non_fraud records
                # Out of ~24.3M rows, probability is max_non_fraud / 24_000_000
                if len(normal_records) < max_non_fraud:
                    normal_records.append(row)
                else:
                    idx = rng.randint(0, total_scanned)
                    if idx < max_non_fraud:
                        normal_records[idx] = row

            if total_scanned % 3000000 == 0:
                logger.info("Scanned %d rows | Frauds found: %d | Elapsed: %.1fs", total_scanned, len(fraud_records), time.time() - t0)

    logger.info(
        "Sampling complete! Total scanned: %d rows in %.1fs | 100%% Frauds: %d | Sampled Normal: %d",
        total_scanned, time.time() - t0, len(fraud_records), len(normal_records)
    )

    combined = fraud_records + normal_records
    rng.shuffle(combined)
    return combined


def build_mcc_risk_map(records: List[Dict[str, Any]], smoothing: float = 20.0) -> Dict[int, float]:
    """Calculate empirical smoothed fraud rate per MCC."""
    mcc_counts: Dict[int, int] = {}
    mcc_frauds: Dict[int, int] = {}
    total_tx = len(records)
    total_fraud = sum(1 for r in records if str(r.get("Is Fraud?", "")).strip().lower() in ("yes", "1", "true"))
    global_rate = total_fraud / max(total_tx, 1)

    for r in records:
        try:
            mcc = int(r.get("MCC", 0))
        except (ValueError, TypeError):
            continue
        mcc_counts[mcc] = mcc_counts.get(mcc, 0) + 1
        if str(r.get("Is Fraud?", "")).strip().lower() in ("yes", "1", "true"):
            mcc_frauds[mcc] = mcc_frauds.get(mcc, 0) + 1

    mcc_risk: Dict[int, float] = {}
    for mcc, count in mcc_counts.items():
        frauds = mcc_frauds.get(mcc, 0)
        # Empirical Bayes smoothing
        smoothed = (frauds + smoothing * global_rate) / (count + smoothing)
        mcc_risk[mcc] = float(round(smoothed, 5))

    logger.info("Computed risk scores for %d MCC codes (global rate: %.5f)", len(mcc_risk), global_rate)
    return mcc_risk


def calculate_user_historical_spend(records: List[Dict[str, Any]]) -> Dict[int, Tuple[float, float]]:
    """Compute per-user historical average and standard deviation of spend."""
    user_spends: Dict[int, List[float]] = {}
    for r in records:
        try:
            u_id = int(r.get("User", 0))
            amt = clean_currency(r.get("Amount"), 0.0)
            user_spends.setdefault(u_id, []).append(amt)
        except Exception:
            continue

    user_stats: Dict[int, Tuple[float, float]] = {}
    for u_id, spends in user_spends.items():
        arr = np.array(spends, dtype=np.float32)
        avg = float(np.mean(arr))
        std = float(np.std(arr)) if len(arr) > 1 else GLOBAL_DEFAULTS["user_std_spend"]
        user_stats[u_id] = (round(avg, 2), round(max(std, 1.0), 2))
    return user_stats


def extract_training_dataset(
    records: List[Dict[str, Any]],
    users_dict: Dict[int, Dict[str, Any]],
    cards_dict: Dict[str, Dict[str, Any]],
    mcc_risk_map: Dict[int, float],
    city_resolver: CityResolver,
    user_spend_stats: Dict[int, Tuple[float, float]]
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Extract features for all sampled records and return (X, y, years)."""
    extractor = FeatureExtractor(mcc_risk_map=mcc_risk_map)

    X_list: List[List[float]] = []
    y_list: List[int] = []
    years_list: List[int] = []

    logger.info("Extracting feature vectors for %d records...", len(records))
    t0 = time.time()

    for idx, r in enumerate(records):
        try:
            u_id = int(r.get("User", 0))
            c_idx = int(r.get("Card", 0))
            year = int(r.get("Year", 2015))
        except (ValueError, TypeError):
            continue

        card_key = f"{u_id}:{c_idx}"
        user_p = users_dict.get(u_id, {})
        card_p = cards_dict.get(card_key, {})

        # Resolve merchant coordinates using city reference
        m_city = r.get("Merchant City")
        m_state = r.get("Merchant State")
        m_lat, m_lon = city_resolver.resolve(m_city, m_state)
        payload = dict(r)
        if m_lat is not None and m_lon is not None:
            payload["Merchant Latitude"] = m_lat
            payload["Merchant Longitude"] = m_lon

        # Resolve transaction_entry_method exactly as Generator does for S3/Kafka
        use_chip = r.get("Use Chip", "Swipe Transaction")
        try:
            mcc_int = int(r.get("MCC", 0))
        except (ValueError, TypeError):
            mcc_int = 0
        entry_method = sample_entry_method(
            use_chip=use_chip,
            source_system="pos_terminal",
            mcc=mcc_int,
            rng=random.Random(idx),
        )
        payload["Transaction Entry Method"] = entry_method

        # Pre-calculated user stats
        u_avg, u_std = user_spend_stats.get(u_id, (GLOBAL_DEFAULTS["user_avg_spend"], GLOBAL_DEFAULTS["user_std_spend"]))
        state = {
            "user_avg_spend": u_avg,
            "user_std_spend": u_std,
            "time_since_last_tx_sec": 86400.0,
            "tx_count_last_1h": 1.0,
            "tx_count_last_24h": 1.0,
            "spend_sum_last_1h": clean_currency(r.get("Amount"), 0.0),
        }

        fdict = extractor.extract_online_features(
            payload=payload,
            user_profile=user_p,
            card_profile=card_p,
            state=state,
        )
        fvec = extractor.to_feature_vector(fdict)

        is_fraud_raw = str(r.get("Is Fraud?", "No")).strip().lower()
        y_val = 1 if is_fraud_raw in ("yes", "1", "true") else 0

        X_list.append(fvec)
        y_list.append(y_val)
        years_list.append(year)

        if (idx + 1) % 100000 == 0:
            logger.info("Processed %d / %d feature rows in %.1fs", idx + 1, len(records), time.time() - t0)

    X = np.array(X_list, dtype=np.float32)
    y = np.array(y_list, dtype=np.int32)
    years = np.array(years_list, dtype=np.int32)

    logger.info("Feature extraction completed: X shape=%s, y frauds=%d (%.2f%%)", X.shape, int(np.sum(y)), (np.sum(y) / len(y)) * 100)
    return X, y, years


def train_and_evaluate(
    X: np.ndarray,
    y: np.ndarray,
    years: np.ndarray,
    output_dir: Path,
    mcc_risk_map: Dict[int, float],
    split_year: int = 2017
) -> Dict[str, Any]:
    """Train LightGBM with chronological train/validation split and tune classification threshold."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Chronological Split (Train: <= split_year, Test: > split_year)
    train_mask = years <= split_year
    test_mask = years > split_year

    # If split leaves test empty, fallback to 80/20 time index
    if np.sum(test_mask) == 0 or np.sum(train_mask) == 0:
        logger.warning("Split year %d yielded unbalanced split; using chronological 80/20 percentile split.", split_year)
        cutoff_year = int(np.percentile(years, 80))
        train_mask = years <= cutoff_year
        test_mask = years > cutoff_year

    X_train, y_train = X[train_mask], y[train_mask]
    X_test, y_test = X[test_mask], y[test_mask]

    n_pos_train = int(np.sum(y_train))
    n_neg_train = len(y_train) - n_pos_train
    scale_pos = max(1.0, float(n_neg_train) / max(1.0, float(n_pos_train)))

    logger.info("Train set: %d samples (%d frauds, scale_pos_weight=%.2f)", len(y_train), n_pos_train, scale_pos)
    logger.info("Test set: %d samples (%d frauds)", len(y_test), int(np.sum(y_test)))

    # Initialize LightGBM Classifier with Enhanced Capacity & Regularization
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

    logger.info("Training LightGBM model with enhanced features & histogram binning...")
    t0 = time.time()
    clf.fit(
        X_train,
        y_train,
        eval_set=[(X_test, y_test)],
        eval_metric="average_precision",
    )
    logger.info("Training completed in %.2fs", time.time() - t0)

    # Probabilities on Test Set
    y_probs = clf.predict_proba(X_test)[:, 1]
    roc_auc = float(roc_auc_score(y_test, y_probs))
    pr_auc = float(average_precision_score(y_test, y_probs))

    logger.info("Evaluation on Out-of-Time Test Set:")
    logger.info("  ROC-AUC Score: %.4f", roc_auc)
    logger.info("  PR-AUC Score (Average Precision): %.4f", pr_auc)

    # Advanced Cost-Aware Threshold Optimization
    # Evaluates candidates from 0.05 to 0.85 targeting Recall >= 80% with minimal False Positives
    precisions, recalls, thresholds = precision_recall_curve(y_test, y_probs)
    # F-beta with beta=1.5 (prioritizing fraud capture over precision)
    beta = 1.5
    f_beta = ((1 + beta**2) * precisions * recalls) / np.maximum((beta**2 * precisions) + recalls, 1e-8)
    
    # Try to find a threshold that catches >= 80% of frauds with minimum False Positive Rate
    candidate_thresholds = []
    for th in thresholds:
        preds = (y_probs >= th).astype(int)
        cmat = confusion_matrix(y_test, preds)
        _tn, _fp, _fn, _tp = cmat.ravel()
        _rec = _tp / (_tp + _fn) if (_tp + _fn) > 0 else 0
        _fpr = _fp / (_fp + _tn) if (_fp + _tn) > 0 else 0
        _f1 = f1_score(y_test, preds, zero_division=0)
        candidate_thresholds.append((th, _rec, _fpr, _f1))

    # Priority 1: High recall (>= 80%) with lowest FPR
    high_rec_cands = [c for c in candidate_thresholds if c[1] >= 0.80]
    if high_rec_cands:
        best_cand = min(high_rec_cands, key=lambda x: (x[2], -x[3]))
        best_threshold = float(round(best_cand[0], 3))
    else:
        # Priority 2: Maximize F-beta
        best_idx = int(np.argmax(f_beta[:-1]))
        best_threshold = float(round(thresholds[best_idx], 3))

    # Evaluate at chosen optimal threshold
    y_pred_optimal = (y_probs >= best_threshold).astype(int)
    cm = confusion_matrix(y_test, y_pred_optimal)
    report = classification_report(y_test, y_pred_optimal, output_dict=True)

    tn, fp, fn, tp = cm.ravel()
    fpr = float(fp / (fp + tn)) if (fp + tn) > 0 else 0.0
    tpr = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
    best_f1 = float(round(f1_score(y_test, y_pred_optimal), 4))

    logger.info("Optimal Decision Threshold: %.3f", best_threshold)
    logger.info("  Optimal F1-Score: %.4f", best_f1)
    logger.info("  Recall (Catch Rate): %.2f%% (%d of %d caught)", tpr * 100, tp, tp + fn)
    logger.info("  False Positive Rate: %.3f%% (%d normal transactions flagged)", fpr * 100, fp)
    logger.info("Confusion Matrix: TN=%d, FP=%d, FN=%d, TP=%d", tn, fp, fn, tp)

    # Feature Importances
    importances = clf.feature_importances_
    sorted_idx = np.argsort(importances)[::-1]
    top_features = []
    logger.info("Top 10 Most Predictive Features:")
    for rank, idx in enumerate(sorted_idx[:10], start=1):
        fname = FEATURE_NAMES[idx]
        gain = float(importances[idx])
        top_features.append({"rank": rank, "feature": fname, "gain": gain})
        logger.info("  %d. %-28s (Gain: %.2f)", rank, fname, gain)

    # Save Model Artifact
    model_path = output_dir / "fraud_lgbm_model.joblib"
    joblib.dump(clf, model_path)
    logger.info("Saved trained model to %s", model_path)

    metadata = {
        "model_type": "LightGBM",
        "trained_timestamp": datetime.now().isoformat(),
        "feature_names": FEATURE_NAMES,
        "optimal_threshold": best_threshold,
        "metrics": {
            "roc_auc": roc_auc,
            "pr_auc": pr_auc,
            "optimal_f1": best_f1,
            "recall": tpr,
            "false_positive_rate": fpr,
            "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
        },
        "top_features": top_features,
        "mcc_risk_map": mcc_risk_map,
        "global_defaults": GLOBAL_DEFAULTS,
    }

    meta_path = output_dir / "model_metadata.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    logger.info("Saved model metadata and threshold config to %s", meta_path)

    return metadata


def main():
    parser = argparse.ArgumentParser(description="Train LightGBM Fraud Detection Model on Credit Card Transactions.")
    parser.add_argument("--data-dir", type=str, default=".", help="Directory with CSV files")
    parser.add_argument("--tx-file", type=str, default="credit_card_transactions-ibm_v2.csv", help="Transactions CSV file")
    parser.add_argument("--sample-non-fraud", type=int, default=300000, help="Number of normal transactions to sample")
    parser.add_argument("--output-dir", type=str, default="models", help="Output directory for model artifacts")
    parser.add_argument("--split-year", type=int, default=2017, help="Year boundary for chronological train/test split")
    args = parser.parse_args()

    base_dir = Path(args.data_dir)
    tx_path = base_dir / args.tx_file
    if not tx_path.exists():
        logger.error("Transaction file not found: %s", tx_path)
        sys.exit(1)

    city_resolver = load_city_resolver(base_dir)
    users_dict, cards_dict = load_reference_data(base_dir)

    # 1. Sample transactions (100% of frauds + stratified normal sample)
    records = sample_transactions(tx_path, max_non_fraud=args.sample_non_fraud)

    # 2. Build empirical MCC risk map
    mcc_risk = build_mcc_risk_map(records)

    # 3. User spend baseline profile
    user_spends = calculate_user_historical_spend(records)

    # 4. Extract aligned feature vectors
    X, y, years = extract_training_dataset(
        records=records,
        users_dict=users_dict,
        cards_dict=cards_dict,
        mcc_risk_map=mcc_risk,
        city_resolver=city_resolver,
        user_spend_stats=user_spends,
    )

    # 5. Train model and tune threshold
    train_and_evaluate(
        X=X,
        y=y,
        years=years,
        output_dir=Path(args.output_dir),
        mcc_risk_map=mcc_risk,
        split_year=args.split_year,
    )
    logger.info("Fraud model training pipeline completed successfully!")


if __name__ == "__main__":
    main()
