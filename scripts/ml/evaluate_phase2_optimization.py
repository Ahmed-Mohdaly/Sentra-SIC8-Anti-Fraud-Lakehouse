"""Phase 2: Cost-Sensitive Fraud Detection Optimization and Multi-Tier Policy Evaluation.

Evaluates the existing V2 candidate model (models/v2/fraud_lgbm_model.joblib) across:
1. Evaluation setup audit & causal feature validation.
2. Full threshold sweep strictly on Validation data (2017).
3. Configurable cost scenarios (missed fraud, false alert, review, authentication friction).
4. Score calibration analysis (Brier score, reliability diagrams, Platt/Isotonic assessment).
5. 3-tier routing policy evaluation (Auto-Approve, Step-Up 2FA/Challenge, Senior Review).
6. Single final evaluation on untouched Test data (2018-2019).
7. Exports deliverables to reports/phase2/.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
from pathlib import Path
import random
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    matthews_corrcoef,
    roc_auc_score,
)

from ml.feature_engineering import (
    FEATURE_NAMES,
    GLOBAL_DEFAULTS,
    FeatureExtractor,
    clean_currency,
)
from synthetic_streaming.classification_and_geo import CityResolver, sample_entry_method

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("phase2_optimization")


def parse_epoch(year: int, month: int, day: int, time_str: str) -> float:
    try:
        from datetime import datetime
        parts = time_str.split(":")
        hh = int(parts[0])
        mm = int(parts[1]) if len(parts) > 1 else 0
        dt = datetime(year, month, day, hh, mm)
        return dt.timestamp()
    except Exception:
        return float(year * 31536000 + month * 2592000 + day * 86400)


def load_reference_data(base_dir: Path) -> Tuple[CityResolver, Dict[int, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    resolver = CityResolver()
    city_file = base_dir / "city_reference.csv"
    if not city_file.exists():
        city_file = base_dir / "data" / "city_reference.csv"
    if city_file.exists():
        resolver.load_from_csv(city_file)
        logger.info("Loaded %d cities from %s", len(resolver), city_file)

    users_dict: Dict[int, Dict[str, Any]] = {}
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
        logger.info("Loaded %d users", len(users_dict))

    cards_dict: Dict[str, Dict[str, Any]] = {}
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
        logger.info("Loaded %d cards", len(cards_dict))

    return resolver, users_dict, cards_dict


def get_or_extract_eval_data(
    csv_path: Path,
    base_dir: Path,
    cache_path: Path,
    metadata_path: Path,
    seed: int = 42,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Retrieve cached val/test matrices or stream-extract them identically to Phase 1.1."""
    if cache_path.exists():
        logger.info("Loading cached evaluation matrices from %s ...", cache_path)
        data = np.load(cache_path)
        return data["X_val"], data["y_val"], data["X_test"], data["y_test"]

    logger.info("Cache not found at %s. Streaming transactions CSV to extract Val and Test sets...", cache_path)
    city_resolver, users_dict, cards_dict = load_reference_data(base_dir)

    with open(metadata_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    mcc_risk_map = {int(k): float(v) for k, v in meta.get("mcc_risk_map", {}).items()}

    rng = random.Random(seed)
    prob_train = 275000 / 18856563.0
    prob_val = 30000 / 1723105.0
    prob_test = 54181 / 3440975.0

    val_records: List[Tuple[Dict[str, Any], Dict[str, Any], int]] = []
    test_records: List[Tuple[Dict[str, Any], Dict[str, Any], int]] = []

    curr_card: Optional[Tuple[int, int]] = None
    card_history: List[Tuple[float, float]] = []
    last_tx_info: Optional[Dict[str, Any]] = None
    user_spend_state: Dict[int, List[float]] = {}

    total_scanned = 0
    t0 = time.time()

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_scanned += 1
            u_id = int(row.get("User", 0))
            c_idx = int(row.get("Card", 0))
            card_key = (u_id, c_idx)

            if card_key != curr_card:
                curr_card = card_key
                card_history.clear()
                last_tx_info = None

            year = int(row.get("Year", 2015))
            month = int(row.get("Month", 1))
            day = int(row.get("Day", 1))
            time_str = str(row.get("Time", "12:00")).strip()
            epoch_sec = parse_epoch(year, month, day, time_str)
            amount = clean_currency(row.get("Amount"), 0.0)

            is_fraud_val = str(row.get("Is Fraud?", "No")).strip().lower()
            is_fraud = 1 if is_fraud_val in ("yes", "1", "true") else 0

            if year <= 2016:
                era = "train"
            elif year == 2017:
                era = "val"
            elif year in (2018, 2019):
                era = "test"
            else:
                era = "other"

            m_city = row.get("Merchant City")
            m_state = row.get("Merchant State")
            m_lat, m_lon = city_resolver.resolve(m_city, m_state)

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
            card_history = [e for e in card_history if e[0] >= cutoff_24h]
            c_1h = sum(1 for e in card_history if e[0] >= cutoff_1h)
            s_1h = sum(e[1] for e in card_history if e[0] >= cutoff_1h) + amount

            tx_count_1h = float(c_1h + 1)
            tx_count_24h = float(len(card_history) + 1)
            spend_sum_1h = s_1h

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

            # Advance RNG state identically to maintain exact seed trajectory
            p = prob_train if era == "train" else (prob_val if era == "val" else prob_test)
            rnd_val = rng.random()

            retain = False
            if era in ("val", "test"):
                if is_fraud or rnd_val < p:
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

                if era == "val":
                    val_records.append((payload_dict, state_dict, is_fraud))
                elif era == "test":
                    test_records.append((payload_dict, state_dict, is_fraud))

            card_history.append((epoch_sec, amount))
            last_tx_info = {"epoch": epoch_sec, "lat": m_lat, "lon": m_lon}

            if u_id not in user_spend_state:
                user_spend_state[u_id] = [1.0, amount, amount * amount]
            else:
                user_spend_state[u_id][0] += 1.0
                user_spend_state[u_id][1] += amount
                user_spend_state[u_id][2] += amount * amount

            if total_scanned % 5000000 == 0:
                logger.info("Scanned %d / 24.38M (%.1f%%) in %.1fs", total_scanned, (total_scanned / 24386900.0) * 100, time.time() - t0)

    logger.info("Finished streaming in %.1fs. Converting %d val and %d test records...", time.time() - t0, len(val_records), len(test_records))

    extractor = FeatureExtractor(
        mcc_risk_map=mcc_risk_map,
        global_stats={**GLOBAL_DEFAULTS, "global_fraud_rate": 0.00132}
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

    X_val, y_val = records_to_matrix(val_records)
    X_test, y_test = records_to_matrix(test_records)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(cache_path, X_val=X_val, y_val=y_val, X_test=X_test, y_test=y_test)
    logger.info("Saved extracted matrices to cache: %s (Val: %s, Test: %s)", cache_path, X_val.shape, X_test.shape)

    return X_val, y_val, X_test, y_test


def evaluate_threshold_grid(
    y_true: np.ndarray,
    probs: np.ndarray,
    thresholds: np.ndarray,
    prevalence_pop: float = 0.0012202,
) -> pd.DataFrame:
    """Evaluate performance across a threshold grid, computing empirical and Bayes population metrics."""
    rows = []
    total_samples = len(y_true)
    n_pos = int(np.sum(y_true))
    n_neg = total_samples - n_pos

    for th in thresholds:
        preds = (probs >= th).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, preds, labels=[0, 1]).ravel()

        recall = float(tp / (tp + fn)) if (tp + fn) > 0 else 0.0
        sample_precision = float(tp / (tp + fp)) if (tp + fp) > 0 else 0.0
        fpr = float(fp / (fp + tn)) if (fp + tn) > 0 else 0.0
        specificity = float(tn / (tn + fp)) if (tn + fp) > 0 else 0.0
        f1_sample = float(f1_score(y_true, preds, zero_division=0))
        mcc = float(matthews_corrcoef(y_true, preds))

        # Sample alert rates
        sample_alert_rate = (tp + fp) / total_samples
        alerts_per_1k_sample = sample_alert_rate * 1000.0
        alerts_per_1m_sample = sample_alert_rate * 1000000.0

        # Bayes Population Projections (Prevalence = prevalence_pop)
        pop_tp_rate = prevalence_pop * recall
        pop_fp_rate = (1.0 - prevalence_pop) * fpr
        pop_alert_rate = pop_tp_rate + pop_fp_rate
        pop_precision = pop_tp_rate / max(pop_alert_rate, 1e-12)
        pop_f1 = (2.0 * pop_precision * recall) / max(pop_precision + recall, 1e-12)
        pop_alerts_per_1k = pop_alert_rate * 1000.0
        pop_alerts_per_1m = pop_alert_rate * 1000000.0
        false_alarms_per_fraud = pop_fp_rate / max(pop_tp_rate, 1e-12)

        rows.append({
            "threshold": round(float(th), 4),
            "tp": int(tp),
            "fp": int(fp),
            "tn": int(tn),
            "fn": int(fn),
            "recall": round(recall, 6),
            "sample_precision": round(sample_precision, 6),
            "fpr": round(fpr, 6),
            "specificity": round(specificity, 6),
            "sample_f1": round(f1_sample, 6),
            "mcc": round(mcc, 6),
            "sample_alerts_per_1k": round(alerts_per_1k_sample, 2),
            "sample_alerts_per_1m": round(alerts_per_1m_sample, 0),
            "pop_prevalence": prevalence_pop,
            "pop_precision": round(pop_precision, 6),
            "pop_f1": round(pop_f1, 6),
            "pop_alerts_per_1k": round(pop_alerts_per_1k, 2),
            "pop_alerts_per_1m": round(pop_alerts_per_1m, 0),
            "false_alarms_per_fraud": round(false_alarms_per_fraud, 2),
        })

    return pd.DataFrame(rows)


def evaluate_cost_scenarios(
    th_df: pd.DataFrame,
    cost_scenarios: Dict[str, Dict[str, float]],
    prevalence_pop: float = 0.0012202,
) -> pd.DataFrame:
    """Compute financial costs per 100k and per 1M transactions under diverse cost scenarios."""
    cost_rows = []

    for name, params in cost_scenarios.items():
        c_fn = params["c_fraud"]
        c_fp = params["c_false_alarm"]
        desc = params.get("description", "")

        for _, row in th_df.iterrows():
            th = row["threshold"]
            rec = row["recall"]
            fpr = row["fpr"]

            # Expected cost per transaction:
            # P(Fraud) * P(Miss|Fraud) * C_FN + P(Normal) * P(Alarm|Normal) * C_FP
            cost_per_tx = (prevalence_pop * (1.0 - rec) * c_fn) + ((1.0 - prevalence_pop) * fpr * c_fp)
            cost_per_100k = cost_per_tx * 100000.0
            cost_per_1m = cost_per_tx * 1000000.0

            cost_rows.append({
                "scenario_name": name,
                "description": desc,
                "c_fraud_fn": c_fn,
                "c_false_alarm_fp": c_fp,
                "threshold": th,
                "recall": rec,
                "fpr": fpr,
                "pop_precision": row["pop_precision"],
                "pop_alerts_per_1k": row["pop_alerts_per_1k"],
                "expected_cost_per_tx": round(cost_per_tx, 6),
                "expected_cost_per_100k": round(cost_per_100k, 2),
                "expected_cost_per_1m": round(cost_per_1m, 2),
            })

    return pd.DataFrame(cost_rows)


def evaluate_multi_tier_policies(
    y_true: np.ndarray,
    probs: np.ndarray,
    candidate_pairs: List[Tuple[float, float]],
    cost_scenarios: Dict[str, Dict[str, float]],
    prevalence_pop: float = 0.0012202,
) -> pd.DataFrame:
    """Evaluate 3-tier routing: Low (Auto-Approve), Med (Step-Up 2FA/Challenge), High (Senior Investigation)."""
    rows = []
    n_pos = int(np.sum(y_true))
    n_neg = len(y_true) - n_pos

    for tau_low, tau_high in candidate_pairs:
        # Tier masks
        mask_low = probs < tau_low
        mask_med = (probs >= tau_low) & (probs < tau_high)
        mask_high = probs >= tau_high

        # Empirical sample counts
        pos_low = int(np.sum(y_true[mask_low]))
        pos_med = int(np.sum(y_true[mask_med]))
        pos_high = int(np.sum(y_true[mask_high]))

        neg_low = int(np.sum((1 - y_true)[mask_low]))
        neg_med = int(np.sum((1 - y_true)[mask_med]))
        neg_high = int(np.sum((1 - y_true)[mask_high]))

        rec_low = pos_low / n_pos  # Missed fraud recall
        rec_med = pos_med / n_pos  # Caught via 2FA / challenge
        rec_high = pos_high / n_pos  # Caught in senior review
        total_catch_rate = rec_med + rec_high

        fpr_med = neg_med / n_neg  # Legitimate tx sent to 2FA challenge
        fpr_high = neg_high / n_neg  # Legitimate tx escalated to manual review
        total_fpr = fpr_med + fpr_high

        # Population routing proportions
        pop_p_low = (1.0 - prevalence_pop) * (neg_low / n_neg) + prevalence_pop * rec_low
        pop_p_med = (1.0 - prevalence_pop) * fpr_med + prevalence_pop * rec_med
        pop_p_high = (1.0 - prevalence_pop) * fpr_high + prevalence_pop * rec_high

        challenges_per_1k = pop_p_med * 1000.0
        reviews_per_1k = pop_p_high * 1000.0
        challenges_per_1m = pop_p_med * 1000000.0
        reviews_per_1m = pop_p_high * 1000000.0

        # Precision within each tier under population prevalence
        prec_med = (prevalence_pop * rec_med) / max(pop_p_med, 1e-12)
        prec_high = (prevalence_pop * rec_high) / max(pop_p_high, 1e-12)

        for s_name, s_params in cost_scenarios.items():
            c_fraud = s_params["c_fraud"]
            c_challenge = s_params.get("c_challenge", 0.10)
            c_review = s_params.get("c_review", 4.50)

            # In the 3-tier model:
            # - Fraud in Low: Uncaught -> Cost = C_fraud
            # - Fraud in Med: Challenged -> 95% caught via 2FA, 5% fraud bypass + challenge cost
            #   Expected fraud loss in med: 0.05 * C_fraud + C_challenge
            # - Fraud in High: Senior reviewed -> 98% caught + C_review
            #   Expected fraud loss in high: 0.02 * C_fraud + C_review
            # - Normal in Med: Challenged -> Cost = C_challenge (friction)
            # - Normal in High: Escalated -> Cost = C_review
            cost_fraud_part = prevalence_pop * (
                rec_low * c_fraud +
                rec_med * (c_challenge + 0.05 * c_fraud) +
                rec_high * (c_review + 0.02 * c_fraud)
            )
            cost_normal_part = (1.0 - prevalence_pop) * (
                fpr_med * c_challenge +
                fpr_high * c_review
            )
            tier_cost_per_tx = cost_fraud_part + cost_normal_part
            tier_cost_per_100k = tier_cost_per_tx * 100000.0
            tier_cost_per_1m = tier_cost_per_tx * 1000000.0

            rows.append({
                "scenario_name": s_name,
                "tau_low": round(tau_low, 4),
                "tau_high": round(tau_high, 4),
                "pct_low_allow": round(pop_p_low * 100, 2),
                "pct_med_challenge": round(pop_p_med * 100, 2),
                "pct_high_review": round(pop_p_high * 100, 2),
                "fraud_recall_low_missed": round(rec_low * 100, 2),
                "fraud_recall_med_challenge": round(rec_med * 100, 2),
                "fraud_recall_high_review": round(rec_high * 100, 2),
                "total_fraud_catch_rate": round(total_catch_rate * 100, 2),
                "fpr_med_challenge": round(fpr_med * 100, 2),
                "fpr_high_review": round(fpr_high * 100, 2),
                "total_fpr": round(total_fpr * 100, 2),
                "pop_prec_med": round(prec_med, 6),
                "pop_prec_high": round(prec_high, 6),
                "challenges_per_1k": round(challenges_per_1k, 1),
                "reviews_per_1k": round(reviews_per_1k, 1),
                "challenges_per_1m": round(challenges_per_1m, 0),
                "reviews_per_1m": round(reviews_per_1m, 0),
                "tier_cost_per_100k": round(tier_cost_per_100k, 2),
                "tier_cost_per_1m": round(tier_cost_per_1m, 2),
            })

    return pd.DataFrame(rows)


def analyze_calibration(y_true: np.ndarray, probs: np.ndarray) -> Dict[str, Any]:
    """Inspect score calibration and compute Brier score & reliability curve."""
    brier = float(brier_score_loss(y_true, probs))
    prob_true, prob_pred = calibration_curve(y_true, probs, n_bins=10, strategy="quantile")

    # Quantile bins report
    bins_data = []
    for pt, pp in zip(prob_true, prob_pred):
        bins_data.append({
            "mean_pred_prob": float(round(pp, 4)),
            "fraction_positives": float(round(pt, 4)),
            "calibration_gap": float(round(pt - pp, 4)),
        })

    return {
        "brier_score": round(brier, 6),
        "mean_predicted_probability": round(float(np.mean(probs)), 4),
        "actual_sample_prevalence": round(float(np.mean(y_true)), 4),
        "calibration_bins": bins_data,
    }


def main():
    parser = argparse.ArgumentParser(description="Phase 2 Optimization Evaluation")
    parser.add_argument("--base-dir", type=str, default=".", help="Base project directory")
    parser.add_argument("--model-path", type=str, default="models/v2/fraud_lgbm_model.joblib", help="Candidate model")
    parser.add_argument("--metadata-path", type=str, default="models/v2/model_metadata.json", help="Model metadata")
    parser.add_argument("--cache-path", type=str, default="reports/phase2/eval_cache.npz", help="Cache file")
    parser.add_argument("--out-dir", type=str, default="reports/phase2", help="Output directory")
    args = parser.parse_args()

    base_dir = Path(args.base_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = base_dir / "credit_card_transactions-ibm_v2.csv"
    meta_path = base_dir / args.metadata_path
    model_path = base_dir / args.model_path
    cache_path = base_dir / args.cache_path

    logger.info("=== Phase 2 Optimization Evaluation Starting ===")

    # 1. Load or extract evaluation datasets
    X_val, y_val, X_test, y_test = get_or_extract_eval_data(
        csv_path=csv_path,
        base_dir=base_dir,
        cache_path=cache_path,
        metadata_path=meta_path,
    )
    logger.info("Loaded matrices: Val=%s (Frauds: %d), Test=%s (Frauds: %d)",
                X_val.shape, int(np.sum(y_val)), X_test.shape, int(np.sum(y_test)))

    # 2. Load frozen V2 model
    logger.info("Loading candidate model from %s ...", model_path)
    model = joblib.load(model_path)

    val_probs = model.predict_proba(X_val)[:, 1]
    test_probs = model.predict_proba(X_test)[:, 1]

    # Verify sanity match with Phase 1.1 at threshold 0.0453
    val_preds_453 = (val_probs >= 0.0453).astype(int)
    cm_val_453 = confusion_matrix(y_val, val_preds_453, labels=[0, 1]).ravel()
    logger.info("Validation at 0.0453: TN=%d, FP=%d, FN=%d, TP=%d (Recall: %.2f%%)",
                cm_val_453[0], cm_val_453[1], cm_val_453[2], cm_val_453[3],
                (cm_val_453[3] / (cm_val_453[3] + cm_val_453[2])) * 100)

    test_preds_453 = (test_probs >= 0.0453).astype(int)
    cm_test_453 = confusion_matrix(y_test, test_preds_453, labels=[0, 1]).ravel()
    logger.info("Test at 0.0453: TN=%d, FP=%d, FN=%d, TP=%d (Recall: %.2f%%)",
                cm_test_453[0], cm_test_453[1], cm_test_453[2], cm_test_453[3],
                (cm_test_453[3] / (cm_test_453[3] + cm_test_453[2])) * 100)

    # 3. Calibration Diagnostics on Validation Data
    logger.info("Analyzing score calibration on Validation data...")
    cal_results = analyze_calibration(y_val, val_probs)
    with open(out_dir / "calibration_summary.json", "w", encoding="utf-8") as f:
        json.dump(cal_results, f, indent=2)

    # 4. Step 2: Threshold Evaluation on Validation Data
    logger.info("Running threshold evaluation across validation grid...")
    # Granular sweep from 0.005 to 0.95
    th_grid = np.unique(np.concatenate([
        np.linspace(0.005, 0.030, 26),
        np.linspace(0.030, 0.150, 41),
        np.linspace(0.150, 0.500, 36),
        np.linspace(0.500, 0.950, 19),
        np.array([0.0453, 0.0660, 0.1000, 0.1500, 0.2000, 0.3000, 0.5000])
    ]))
    th_grid = np.sort(th_grid)

    th_val_df = evaluate_threshold_grid(y_val, val_probs, th_grid)
    th_val_csv = out_dir / "threshold_comparison.csv"
    th_val_df.to_csv(th_val_csv, index=False)
    logger.info("Saved threshold comparison to %s (%d rows)", th_val_csv, len(th_val_df))

    # 5. Step 3: Cost-Sensitive Analysis
    logger.info("Evaluating configurable cost scenarios...")
    cost_scenarios = {
        "Scenario_A_Balanced_Banking": {
            "c_fraud": 120.00,
            "c_false_alarm": 5.00,
            "c_review": 3.50,
            "c_challenge": 0.10,
            "description": "Standard retail banking: $120 avg fraud loss, $5 alert cost ($3.50 manual review + $1.50 cardholder friction), $0.10 automated 2FA challenge.",
        },
        "Scenario_B_High_Fraud_Loss": {
            "c_fraud": 300.00,
            "c_false_alarm": 3.00,
            "c_review": 2.50,
            "c_challenge": 0.05,
            "description": "Commercial / High-value cards: $300 avg fraud loss, $3 alert cost, $0.05 2FA challenge.",
        },
        "Scenario_C_Low_Friction_Premium": {
            "c_fraud": 80.00,
            "c_false_alarm": 15.00,
            "c_review": 5.00,
            "c_challenge": 0.20,
            "description": "Premium VIP portfolio: $80 avg fraud loss, high $15 false alarm cost (heavy customer churn penalty), $0.20 challenge.",
        },
        "Scenario_D_Automated_Ops": {
            "c_fraud": 100.00,
            "c_false_alarm": 2.00,
            "c_review": 1.50,
            "c_challenge": 0.05,
            "description": "Fintech / Automated operations: $100 fraud loss, $2 low overhead alert handling, $0.05 2FA challenge.",
        },
        "Scenario_E_Catastrophic_Loss": {
            "c_fraud": 500.00,
            "c_false_alarm": 10.00,
            "c_review": 7.00,
            "c_challenge": 0.15,
            "description": "High-risk corporate limits: $500 catastrophic fraud loss, $10 false alarm review.",
        },
    }

    cost_df = evaluate_cost_scenarios(th_val_df, cost_scenarios)
    cost_csv = out_dir / "cost_sensitivity.csv"
    cost_df.to_csv(cost_csv, index=False)
    logger.info("Saved cost sensitivity to %s (%d rows)", cost_csv, len(cost_df))

    # 6. Step 4: Multi-Tier Decision Policy Evaluation
    logger.info("Evaluating multi-tier decision policies on Validation data...")
    # Candidate threshold pairs: (tau_low, tau_high)
    candidate_pairs = [
        (0.015, 0.080),
        (0.020, 0.100),
        (0.025, 0.120),
        (0.030, 0.150),
        (0.040, 0.180),
        (0.0453, 0.200),
        (0.050, 0.250),
        (0.060, 0.300),
        (0.080, 0.350),
        (0.100, 0.400),
        (0.020, 0.250),
        (0.030, 0.300),
        (0.0453, 0.300),
    ]

    tier_df = evaluate_multi_tier_policies(y_val, val_probs, candidate_pairs, cost_scenarios)
    tier_csv = out_dir / "multi_tier_policy.csv"
    tier_df.to_csv(tier_csv, index=False)
    logger.info("Saved multi-tier policy comparison to %s (%d rows)", tier_csv, len(tier_df))

    # 7. Final Locked Test Evaluation
    logger.info("Running single frozen evaluation of selected policies on Held-Out Test Set (2018-2019)...")

    # Find optimal validation threshold for Baseline Scenario A
    sc_a_costs = cost_df[cost_df["scenario_name"] == "Scenario_A_Balanced_Banking"]
    best_row_a = sc_a_costs.loc[sc_a_costs["expected_cost_per_100k"].idxmin()]
    opt_th_val = best_row_a["threshold"]
    logger.info("Validation Minimum-Cost Threshold under Scenario A: %.4f ($%.2f per 100k tx)",
                opt_th_val, best_row_a["expected_cost_per_100k"])

    # High recall candidate targeting >= 75% recall on validation
    high_rec_sub = th_val_df[th_val_df["recall"] >= 0.75]
    best_high_rec = high_rec_sub.loc[high_rec_sub["fpr"].idxmin()]
    logger.info("Validation High-Recall (>=75%) with Minimal FPR: Threshold %.4f (Val Recall: %.2f%%, Val FPR: %.2f%%)",
                best_high_rec["threshold"], best_high_rec["recall"] * 100, best_high_rec["fpr"] * 100)

    # Multi-tier policy selection on validation
    tier_sc_a = tier_df[tier_df["scenario_name"] == "Scenario_A_Balanced_Banking"]
    best_tier_a = tier_sc_a.loc[tier_sc_a["tier_cost_per_100k"].idxmin()]
    logger.info("Validation Optimal 3-Tier Policy under Scenario A: tau_low=%.4f, tau_high=%.4f ($%.2f per 100k tx, Catch Rate: %.2f%%)",
                best_tier_a["tau_low"], best_tier_a["tau_high"],
                best_tier_a["tier_cost_per_100k"], best_tier_a["total_fraud_catch_rate"])

    # Evaluate these validation-selected thresholds on frozen Test Set (2018-2019)
    test_eval_thresholds = [0.0453, float(opt_th_val), float(best_high_rec["threshold"])]
    th_test_df = evaluate_threshold_grid(y_test, test_probs, np.array(test_eval_thresholds))
    th_test_df.to_csv(out_dir / "test_set_evaluation.csv", index=False)

    # Evaluate validation-selected 3-tier policies on Test Set
    test_tier_pairs = [
        (best_tier_a["tau_low"], best_tier_a["tau_high"]),
        (0.020, 0.150),
        (0.0453, 0.200),
    ]
    test_tier_df = evaluate_multi_tier_policies(y_test, test_probs, test_tier_pairs, cost_scenarios)
    test_tier_df.to_csv(out_dir / "test_set_multi_tier_evaluation.csv", index=False)

    logger.info("=== Phase 2 Optimization Evaluation Complete! ===")


if __name__ == "__main__":
    main()
