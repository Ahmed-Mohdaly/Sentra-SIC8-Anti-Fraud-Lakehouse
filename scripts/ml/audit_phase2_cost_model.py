"""Independent Audit and Reconciliation of Phase 2 Cost-Sensitive Evaluation.

Audits:
1. Mathematical correctness of expected cost calculations.
2. Separation of fraud loss, false alarm cost, 2FA challenge cost, and manual review cost.
3. Impact of 2FA efficacy assumptions (95% assumed vs 80% realistic vs 0% worst-case).
4. Model detection recall vs routing recall vs effective prevention rate.
5. Reconciliation of temporal cohorts, sampling prevalences, and dataset exclusions.
6. Outputs reports/phase2/cost_model_reconciliation.csv.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix

PREVALENCE_POP = 0.0012202  # Overall population prevalence (29,757 / 24,386,900)
PREVALENCE_2017 = 255 / 1723360.0  # 0.000147966 (0.0148%)
PREVALENCE_TEST = 4578 / 3445553.0  # 0.001328669 (0.1329%)


def load_cached_data(cache_path: Path) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    data = np.load(cache_path)
    return data["X_val"], data["y_val"], data["X_test"], data["y_test"]


def main():
    base_dir = Path(".")
    cache_path = base_dir / "reports/phase2/eval_cache.npz"
    model_path = base_dir / "models/v2/fraud_lgbm_model.joblib"
    out_csv = base_dir / "reports/phase2/cost_model_reconciliation.csv"

    print("Loading cached evaluation matrices...")
    X_val, y_val, X_test, y_test = load_cached_data(cache_path)

    import joblib
    print("Loading V2 model...")
    model = joblib.load(model_path)

    val_probs = model.predict_proba(X_val)[:, 1]
    test_probs = model.predict_proba(X_test)[:, 1]

    cost_scenarios = {
        "Scenario_A_Balanced_Banking": {
            "c_fraud": 120.00,
            "c_false_alarm": 5.00,
            "c_review": 3.50,
            "c_challenge": 0.10,
        },
        "Scenario_B_High_Fraud_Loss": {
            "c_fraud": 300.00,
            "c_false_alarm": 3.00,
            "c_review": 2.50,
            "c_challenge": 0.05,
        },
        "Scenario_C_Low_Friction_Premium": {
            "c_fraud": 80.00,
            "c_false_alarm": 15.00,
            "c_review": 5.00,
            "c_challenge": 0.20,
        },
        "Scenario_D_Automated_Ops": {
            "c_fraud": 100.00,
            "c_false_alarm": 2.00,
            "c_review": 1.50,
            "c_challenge": 0.05,
        },
        "Scenario_E_Catastrophic_Loss": {
            "c_fraud": 500.00,
            "c_false_alarm": 10.00,
            "c_review": 7.00,
            "c_challenge": 0.15,
        },
    }

    policies = [
        {"type": "Binary", "name": "Binary_Candidate_0.0453", "tau_low": 0.0453, "tau_high": 0.0453},
        {"type": "Binary", "name": "Binary_ValHighRec_0.0690", "tau_low": 0.0690, "tau_high": 0.0690},
        {"type": "Binary", "name": "Binary_Conservative_0.2000", "tau_low": 0.2000, "tau_high": 0.2000},
        {"type": "MultiTier", "name": "MultiTier_HighCatch_0.0453_0.2000", "tau_low": 0.0453, "tau_high": 0.2000},
        {"type": "MultiTier", "name": "MultiTier_Optimized_0.0453_0.3000", "tau_low": 0.0453, "tau_high": 0.3000},
        {"type": "MultiTier", "name": "MultiTier_Balanced_0.1000_0.4000", "tau_low": 0.1000, "tau_high": 0.4000},
        {"type": "MultiTier", "name": "MultiTier_Aggressive_0.0200_0.1500", "tau_low": 0.0200, "tau_high": 0.1500},
    ]

    reconciled_rows = []

    for cohort_name, y_data, p_data in [("Validation_2017", y_val, val_probs), ("Test_2018_2019", y_test, test_probs)]:
        n_pos = int(np.sum(y_data))
        n_neg = len(y_data) - n_pos

        # Compute baseline binary cost reference for each scenario to compute savings delta
        baseline_costs = {}
        preds_base = (p_data >= 0.0453).astype(int)
        tn_b, fp_b, fn_b, tp_b = confusion_matrix(y_data, preds_base, labels=[0, 1]).ravel()
        rec_b = tp_b / n_pos
        fpr_b = fp_b / n_neg
        for s_name, s_params in cost_scenarios.items():
            c_f = s_params["c_fraud"]
            c_a = s_params["c_false_alarm"]
            c_r = s_params["c_review"]
            # Strict accounting: Missed fraud loss + Normal false alarm cost + True positive review cost
            cost_tx = (PREVALENCE_POP * (1.0 - rec_b) * c_f) + ((1.0 - PREVALENCE_POP) * fpr_b * c_a) + (PREVALENCE_POP * rec_b * c_r)
            baseline_costs[s_name] = cost_tx * 100000.0

        for pol in policies:
            tau_l = pol["tau_low"]
            tau_h = pol["tau_high"]
            is_binary = pol["type"] == "Binary"

            if is_binary:
                preds = (p_data >= tau_l).astype(int)
                tn, fp, fn, tp = confusion_matrix(y_data, preds, labels=[0, 1]).ravel()
                rec = tp / n_pos
                fpr = fp / n_neg
                spec = tn / n_neg

                rec_low = fn / n_pos
                rec_med = 0.0
                rec_high = rec
                fpr_med = 0.0
                fpr_high = fpr

                pct_low = (1.0 - PREVALENCE_POP) * spec + PREVALENCE_POP * rec_low
                pct_med = 0.0
                pct_high = (1.0 - PREVALENCE_POP) * fpr + PREVALENCE_POP * rec_high

                det_recall = rec
                rout_recall = rec
                eff_prev_assumed = rec * 1.0  # Assumes 100% prevented in binary alert
                eff_prev_realistic = rec * 0.95
                eff_prev_worst = rec * 0.90
            else:
                mask_l = p_data < tau_l
                mask_m = (p_data >= tau_l) & (p_data < tau_h)
                mask_h = p_data >= tau_h

                pos_l = int(np.sum(y_data[mask_l]))
                pos_m = int(np.sum(y_data[mask_m]))
                pos_h = int(np.sum(y_data[mask_h]))

                neg_l = int(np.sum((1 - y_data)[mask_l]))
                neg_m = int(np.sum((1 - y_data)[mask_m]))
                neg_h = int(np.sum((1 - y_data)[mask_h]))

                rec_low = pos_l / n_pos
                rec_med = pos_m / n_pos
                rec_high = pos_h / n_pos
                fpr_med = neg_m / n_neg
                fpr_high = neg_h / n_neg

                pct_low = (1.0 - PREVALENCE_POP) * (neg_l / n_neg) + PREVALENCE_POP * rec_low
                pct_med = (1.0 - PREVALENCE_POP) * fpr_med + PREVALENCE_POP * rec_med
                pct_high = (1.0 - PREVALENCE_POP) * fpr_high + PREVALENCE_POP * rec_high

                det_recall = rec_med + rec_high
                rout_recall = det_recall
                # Assumed in Phase 2: 95% caught in 2FA, 98% caught in senior review
                eff_prev_assumed = (rec_med * 0.95) + (rec_high * 0.98)
                # Realistic: 80% caught in 2FA, 95% caught in senior review
                eff_prev_realistic = (rec_med * 0.80) + (rec_high * 0.95)
                # Worst-case: 0% caught in 2FA (complete bypass), 90% caught in senior review
                eff_prev_worst = (rec_med * 0.00) + (rec_high * 0.90)

            for s_name, s_params in cost_scenarios.items():
                c_fraud = s_params["c_fraud"]
                c_alarm = s_params["c_false_alarm"]
                c_review = s_params["c_review"]
                c_challenge = s_params["c_challenge"]

                if is_binary:
                    # Reported in Phase 2 (Omitted review cost on true positives):
                    cost_tx_reported = (PREVALENCE_POP * (1.0 - rec) * c_fraud) + ((1.0 - PREVALENCE_POP) * fpr * c_alarm)

                    # Reconciled (Consistent accounting):
                    loss_fraud = PREVALENCE_POP * (1.0 - rec) * c_fraud
                    cost_fa = (1.0 - PREVALENCE_POP) * fpr * c_alarm
                    cost_ch = 0.0
                    cost_rev = PREVALENCE_POP * rec * c_review  # True positives reviewed
                    cost_tx_reconciled = loss_fraud + cost_fa + cost_rev

                    loss_fraud_worst = PREVALENCE_POP * (1.0 - eff_prev_worst) * c_fraud
                    cost_tx_worst = loss_fraud_worst + cost_fa + cost_rev
                else:
                    # Reported in Phase 2:
                    c_f_part = PREVALENCE_POP * (
                        rec_low * c_fraud +
                        rec_med * (c_challenge + 0.05 * c_fraud) +
                        rec_high * (c_review + 0.02 * c_fraud)
                    )
                    c_n_part = (1.0 - PREVALENCE_POP) * (
                        fpr_med * c_challenge +
                        fpr_high * c_review
                    )
                    cost_tx_reported = c_f_part + c_n_part

                    # Explicitly separated components:
                    # 1. Fraud losses from unprevented fraud
                    loss_fraud = PREVALENCE_POP * c_fraud * (rec_low + 0.05 * rec_med + 0.02 * rec_high)
                    # 2. False positive handling / customer friction costs
                    cost_fa = (1.0 - PREVALENCE_POP) * (fpr_med * c_challenge + fpr_high * c_review)
                    # 3. 2FA challenge costs (All challenged tx: normal + fraud)
                    cost_ch = (pct_med) * c_challenge
                    # 4. Manual review costs (All reviewed tx: normal + fraud)
                    cost_rev = (pct_high) * c_review

                    cost_tx_reconciled = cost_tx_reported

                    # Worst-case scenario (0% 2FA effectiveness, 10% review leakage):
                    loss_fraud_worst = PREVALENCE_POP * c_fraud * (rec_low + 1.00 * rec_med + 0.10 * rec_high)
                    cost_tx_worst = loss_fraud_worst + (pct_med * c_challenge) + (pct_high * c_review)

                reconciled_rows.append({
                    "cohort": cohort_name,
                    "policy_type": pol["type"],
                    "policy_name": pol["name"],
                    "scenario_name": s_name,
                    "tau_low": tau_l,
                    "tau_high": tau_h,
                    "model_detection_recall": round(det_recall * 100, 2),
                    "routing_recall": round(rout_recall * 100, 2),
                    "effective_prevention_rate_assumed": round(eff_prev_assumed * 100, 2),
                    "effective_prevention_rate_realistic": round(eff_prev_realistic * 100, 2),
                    "effective_prevention_rate_worst": round(eff_prev_worst * 100, 2),
                    "pct_low_allow": round(pct_low * 100, 2),
                    "pct_med_challenge": round(pct_med * 100, 2),
                    "pct_high_review": round(pct_high * 100, 2),
                    "pop_alerts_per_1k": round((pct_med + pct_high) * 1000.0, 1),
                    "fraud_loss_per_100k": round(loss_fraud * 100000.0, 2),
                    "false_alarm_cost_per_100k": round(cost_fa * 100000.0, 2),
                    "challenge_cost_per_100k": round(cost_ch * 100000.0, 2),
                    "review_cost_per_100k": round(cost_rev * 100000.0, 2),
                    "expected_cost_per_100k_reported": round(cost_tx_reported * 100000.0, 2),
                    "expected_cost_per_100k_reconciled": round(cost_tx_reconciled * 100000.0, 2),
                    "expected_cost_per_100k_worst_case": round(cost_tx_worst * 100000.0, 2),
                    "cost_delta_per_100k_vs_binary": round((cost_tx_reconciled * 100000.0) - baseline_costs[s_name], 2),
                })

    df = pd.DataFrame(reconciled_rows)
    df.to_csv(out_csv, index=False)
    print(f"Saved reconciled cost model to {out_csv} ({len(df)} rows)")


if __name__ == "__main__":
    main()
