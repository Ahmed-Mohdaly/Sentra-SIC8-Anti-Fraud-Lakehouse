# Phase 2 — Cost-Sensitive Fraud Detection Optimization & Policy Evaluation Report

**Date:** October 28, 2026  
**Auditor / ML Systems Engineer:** Senior Machine Learning & Data Systems Engineer  
**Repository:** `SayedELMASRY2/generator` (`/home/syd/Downloads/archive`)  
**Git Baseline Commit:** `5e1b2ffe06494d4a6a2643a955586f8c7d8e895f` on `main`  
**Candidate Model Evaluated:** [`models/v2/fraud_lgbm_model.joblib`](file:///home/syd/Downloads/archive/models/v2/fraud_lgbm_model.joblib)  
**Model Metadata:** [`models/v2/model_metadata.json`](file:///home/syd/Downloads/archive/models/v2/model_metadata.json)  
**Execution Script:** [`scripts/evaluate_phase2_optimization.py`](file:///home/syd/Downloads/archive/scripts/evaluate_phase2_optimization.py)  
**Evaluation Scope:** Offline research and decision-policy evaluation only. Zero live blocking, zero model retraining, and zero changes to production routing or state.

---

## 1. Executive Summary & Core Insights

Phase 2 investigated the operational and economic viability of the candidate LightGBM fraud model (`models/v2/`). While Phase 1.3 verified offline-to-online feature parity across all 34 features, evaluating the candidate model under realistic bank operating constraints revealed a critical finding:

### 1.1 The Base Rate Fallacy in Single Binary Thresholding
* In the enriched test sample ($7.84\%$ fraud prevalence), the model operating at threshold `0.0453` achieves **$77.63\%$ recall** and **$21.32\%$ precision** with a **$24.17\%$ False Positive Rate (FPR)**.
* However, in live production where genuine fraud prevalence is approximately **$\pi = 0.1220\%$** (~1 fraud per 820 transactions), Bayes' theorem dictates that the true operational precision is **$0.391\%$**.
* **Operational Consequence:** For every 1 true fraud intercepted, the system generates **254.9 false alarms**. If implemented as a single binary "Approve vs. Decline" threshold, the bank would review or decline nearly **1 in every 4 legitimate customer transactions** ($242.4$ alerts per 1,000 transactions).
* **Economic Paradox:** At an alert cost of $\$5.00$ ($\$3.50$ manual review + $\$1.50$ customer friction), processing false alarms on 100,000 transactions costs **$\$120,700$**, which is **eight times higher** than the total fraud losses across the entire portfolio ($\$14,640$).

### 1.2 The Multi-Tier Decision Solution
By decomposing the decision space into a **3-Tier Policy** (Auto-Approve, Automated 2FA/3DS Step-Up Challenge, and Senior Manual Review):
1. **Low Risk ($p < 0.0453$):** **$75.76\%$** of transactions are auto-approved with zero customer friction and zero operational cost.
2. **Medium Risk ($0.0453 \le p < 0.2000$):** **$16.08\%$** of transactions are routed to low-cost automated step-up authentication (SMS OTP / 3DS challenge at $\$0.10$ each), capturing **$24.84\%$** of frauds without human intervention.
3. **High Risk ($p \ge 0.2000$):** Only **$8.16\%$** of transactions are escalated for manual investigation by fraud analysts, capturing **$52.80\%$** of frauds.
4. **Economic Impact:** Total fraud catch rate is preserved at **$77.63\%$**, while expected operational cost drops from **$\$123,980$ down to $\$33,774$ per 100,000 transactions**—an **$72.8\%$ cost reduction**.
5. Under a more conservative challenge policy ($\tau_{\text{low}} = 0.1000, \tau_{\text{high}} = 0.4000$), operational costs fall to **$\$19,975$ per 100,000 transactions** (**$83.9\%$ reduction**), with **$85.5\%$** auto-approved and a **$65.31\%$** fraud catch rate.

---

## 2. Audit of the Evaluation Setup (Step 1)

### 2.1 Split Logic & Validation Protocol
The dataset partitioning follows a strict chronological 3-way split:
* **Training Cohort:** $\le 2016$ (1991–2016). Population: 18,881,487 transactions.
* **Validation Cohort:** 2017 (Calendar Year 2017). Population: 1,723,360 transactions ($255$ frauds). Sampled: $30,284$ records ($255$ frauds, $30,029$ non-frauds).
* **Held-Out Test Cohort:** 2018–2019 (Calendar Years 2018–2019). Population: 3,445,553 transactions ($4,578$ frauds). Sampled: $58,824$ records ($4,578$ frauds, $54,246$ non-frauds).

```
[ Train: 1991-2016 ] ------------> [ Validation: 2017 ] ------------> [ Frozen Test: 2018-2019 ]
- Fit LGBM Candidate               - Tune Threshold Grid              - Evaluate Selected Policy
- Fit Empirical Bayes MCC Map      - Cost Optimization                  ONCE with frozen weights
- Compute User Spend Priming       - Calibration Diagnostics
```

### 2.2 Verification of Non-Contamination
1. **Validation-Only Selection:** In [`scripts/train_fraud_model_remediated.py`](file:///home/syd/Downloads/archive/scripts/train_fraud_model_remediated.py#L458-L514) and [`scripts/evaluate_phase2_optimization.py`](file:///home/syd/Downloads/archive/scripts/evaluate_phase2_optimization.py#L685-L730), candidate decision thresholds and multi-tier routing boundaries are derived exclusively on 2017 validation data.
2. **Frozen Test Set Isolation:** The 2018–2019 test set is evaluated exactly once after policy parameters are frozen. It is never used in grid searches, loss minimizations, or threshold tuning.

### 2.3 Mathematical Impact of Class Enrichment & Downsampling
Because negative transactions were sampled uniformly at random while 100% of frauds were retained:
* **True Positive Rate ($TPR$ / Recall):** Unbiased. $TPR = \frac{TP}{TP + FN}$ depends solely on positive instances.
* **False Positive Rate ($FPR$):** Unbiased. $FPR = \frac{FP}{FP + TN}$ represents the proportion of non-frauds alerted; uniform sampling across negatives leaves this expectation unchanged.
* **Specificity ($TNR$):** Unbiased ($1 - FPR$).
* **Precision ($PPV$):** Severely biased by enrichment. In sample: $\text{Prevalence} \approx 7.84\% \implies \text{Precision} \approx 21.3\%$. Under true population prevalence ($\pi = 0.0012202$), Bayes' rule is mandatory:
  $$\text{Precision}_{\text{pop}} = \frac{\pi \cdot TPR}{\pi \cdot TPR + (1 - \pi) \cdot FPR}$$

### 2.4 Estimability Matrix: Available vs. Production Data

| Metric / Dimension | Estimable from Dataset? | Methodology & Confidence | Requires Live Production Data? |
|---|---|---|---|
| **Recall / Catch Rate** | **YES** | Exact (100% test frauds evaluated) | No |
| **False Positive Rate (FPR)** | **YES** | Unbiased estimator from random negative sample | No |
| **Specificity** | **YES** | Unbiased estimator | No |
| **Bayes Population Precision** | **YES** | Analytical projection ($\pi = 0.0012202$) | Validated against historical population counts |
| **Alert / Challenge Rates** | **YES** | Computed per 1,000 and per 1,000,000 transactions | Validated |
| **Cardholder Churn Impact** | **NO** | Assumed unit friction cost ($\$1.50$–$\$10.00$) | **YES** (Requires customer support tickets & attrition telemetry) |
| **Adversarial Adaptation** | **NO** | Static historical transactions | **YES** (Requires live post-launch monitoring for feature evasion) |
| **Investigation Queue Time** | **NO** | Assumed fixed manual review cost | **YES** (Requires SOC/fraud team backlog & SLA tracking) |

### 2.5 Causality & Temporal Ordering
As audited in Phase 1.3:
* Customer spend baselines (`user_avg_spend`, `user_std_spend`) use strictly prior transactions ($t-1$).
* Velocity counters (`tx_count_last_1h`, `spend_sum_last_1h`, etc.) evaluate historical sliding windows prior to the current transaction.
* `mcc_risk_score` uses Empirical Bayes parameters estimated strictly from $\le 2016$.
* All dates are interpreted consistently in UTC seconds.

---

## 3. Threshold Evaluation on Validation Data (Step 2)

A granular sweep across 121 thresholds ($\tau \in [0.005, 0.950]$) was conducted on the 2017 Validation Set ($30,284$ transactions, $255$ frauds). Full results are archived in [`reports/phase2/threshold_comparison.csv`](file:///home/syd/Downloads/archive/reports/phase2/threshold_comparison.csv).

### Representative Validation Threshold Performance Table

| Threshold ($\tau$) | Val TP | Val FP | Val FN | Val Recall | Val FPR | Val Specificity | Sample Prec | Pop Prec ($\pi=0.122\%$) | Pop Alerts / 1k | False Alarms / Fraud |
|---|---|---|---|---|---|---|---|---|---|---|
| `0.0100` | 243 | 14,845 | 12 | **95.29%** | 49.44% | 50.56% | 1.61% | 0.235% | 495.0 | 425.0 |
| `0.0200` | 228 | 11,093 | 27 | **89.41%** | 36.94% | 63.06% | 2.01% | 0.295% | 370.1 | 338.4 |
| `0.0300` | 222 | 8,926 | 33 | **87.06%** | 29.72% | 70.28% | 2.43% | 0.357% | 297.9 | 279.1 |
| `0.0400` | 215 | 7,573 | 40 | **84.31%** | 25.22% | 74.78% | 2.76% | 0.407% | 252.9 | 244.7 |
| **`0.0453` (Candidate)** | **209** | **7,034** | **46** | **81.96%** | **23.42%** | **76.58%** | **2.88%** | **0.426%** | **235.0** | **234.0** |
| `0.0600` | 196 | 5,905 | 59 | **76.86%** | 19.66% | 80.34% | 3.21% | 0.476% | 197.3 | 209.4 |
| `0.0690` | 192 | 5,402 | 63 | **75.29%** | 17.99% | 82.01% | 3.43% | 0.509% | 180.6 | 195.5 |
| `0.1000` | 176 | 4,141 | 79 | **69.02%** | 13.79% | 86.21% | 4.08% | 0.609% | 138.6 | 163.6 |
| `0.1500` | 162 | 2,977 | 93 | **63.53%** | 9.91% | 90.09% | 5.16% | 0.780% | 99.8 | 127.5 |
| `0.2000` | 145 | 2,274 | 110 | **56.86%** | 7.57% | 92.43% | 5.99% | 0.909% | 76.4 | 109.1 |
| `0.3000` | 133 | 1,498 | 122 | **52.16%** | 4.99% | 95.01% | 8.15% | 1.261% | 50.5 | 78.3 |
| `0.5000` | 89 | 633 | 166 | **34.90%** | 2.11% | 97.89% | 12.33% | 1.980% | 21.5 | 49.5 |
| `0.7000` | 51 | 246 | 204 | **20.00%** | 0.82% | 99.18% | 17.17% | 2.887% | 8.4 | 33.6 |
| `0.9000` | 31 | 93 | 224 | **12.16%** | 0.31% | 99.69% | 25.00% | 4.568% | 3.2 | 20.9 |
| `0.9500` | 21 | 42 | 234 | **8.24%** | 0.14% | 99.86% | 33.33% | 6.711% | 1.5 | 13.9 |

---

## 4. Cost-Sensitive Analysis (Step 3)

### 4.1 Cost Model Formulation
To reflect diverse business operating environments, financial performance was evaluated across five configurable cost scenarios:

$$\text{Expected Cost per Transaction} = \pi \cdot (1 - \text{Recall}) \cdot C_{\text{FN}} + (1 - \pi) \cdot \text{FPR} \cdot C_{\text{FP}}$$

Where:
* $C_{\text{FN}}$: Financial loss from an unintercepted fraudulent transaction.
* $C_{\text{FP}}$: Total cost of a false alarm (Operational Review Cost $C_{\text{rev}}$ + Cardholder Friction & Attrition Cost $C_{\text{fric}}$).
* $\pi = 0.0012202$: Population base rate.

### 4.2 Definition of Evaluated Scenarios

1. **Scenario A (Balanced Retail Banking Baseline):**  
   $C_{\text{FN}} = \$120.00$, $C_{\text{FP}} = \$5.00$ ($C_{\text{rev}} = \$3.50$, $C_{\text{fric}} = \$1.50$), $C_{\text{challenge}} = \$0.10$.
2. **Scenario B (High Fraud Loss / Commercial Portfolios):**  
   $C_{\text{FN}} = \$300.00$, $C_{\text{FP}} = \$3.00$ ($C_{\text{rev}} = \$2.50$, $C_{\text{fric}} = \$0.50$), $C_{\text{challenge}} = \$0.05$.
3. **Scenario C (Low Friction / VIP Cardholders):**  
   $C_{\text{FN}} = \$80.00$, $C_{\text{FP}} = \$15.00$ ($C_{\text{rev}} = \$5.00$, $C_{\text{fric}} = \$10.00$ high attrition risk), $C_{\text{challenge}} = \$0.20$.
4. **Scenario D (Automated Operations / Fintech):**  
   $C_{\text{FN}} = \$100.00$, $C_{\text{FP}} = \$2.00$ ($C_{\text{rev}} = \$1.50$, $C_{\text{fric}} = \$0.50$), $C_{\text{challenge}} = \$0.05$.
5. **Scenario E (Catastrophic High-Risk Limits):**  
   $C_{\text{FN}} = \$500.00$, $C_{\text{FP}} = \$10.00$ ($C_{\text{rev}} = \$7.00$, $C_{\text{fric}} = \$3.00$), $C_{\text{challenge}} = \$0.15$.

### 4.3 Unconstrained vs. Recall-Constrained Cost Optimization

When single-threshold cost is minimized without constraint on retail banking (Scenario A), the mathematical minimum occurs at $\tau = 0.9500$. At this extreme threshold:
* Operational cost is minimized ($\$14,135$ per 100k tx) because false alarms are suppressed to $0.14\%$.
* **However, fraud catch rate collapses to $8.24\%$**, missing $91.76\%$ of fraud!

In practical banking, risk committees mandate minimum catch rates. When evaluating the optimal threshold subject to minimum recall targets:

| Target Recall Constraint | Optimal Threshold ($\tau^*$) | Achieved Recall | Achieved FPR | Pop Precision | Pop Alerts / 1k | Cost / 100k Transactions | Cost / 1M Transactions |
|---|---|---|---|---|---|---|---|
| **Unconstrained Minimum** | `0.9500` | 8.24% | 0.14% | 6.711% | 1.5 | **$\$14,135.20$** | $\$141,352.00$ |
| **Recall $\ge 50\%$** | `0.3200` | 50.98% | 4.64% | 1.324% | 47.0 | **$\$30,360.32$** | $\$303,603.20$ |
| **Recall $\ge 60\%$** | `0.1800` | 60.00% | 8.36% | 0.870% | 84.2 | **$\$47,582.48$** | $\$475,824.80$ |
| **Recall $\ge 70\%$** | `0.0960` | 70.59% | 14.20% | 0.604% | 142.7 | **$\$75,234.44$** | $\$752,344.40$ |
| **Recall $\ge 75\%$** | `0.0690` | 75.29% | 17.99% | 0.509% | 180.6 | **$\$93,454.28$** | $\$934,542.80$ |
| **Recall $\ge 80\%$ (V2 Default)**| `0.0453` | 81.96% | 23.42% | 0.426% | 235.0 | **$\$119,618.46$** | $\$1,196,184.60$ |

Full scenario comparisons are archived in [`reports/phase2/cost_sensitivity.csv`](file:///home/syd/Downloads/archive/reports/phase2/cost_sensitivity.csv).

---

## 5. Score Calibration Diagnostic

### 5.1 Empirical Findings
Calibration was inspected on the 2017 Validation Set using decile quantile binning:
* **Brier Score:** `0.024988`
* **Mean Predicted Probability:** `0.0602` ($6.02\%$)
* **Actual Validation Prevalence:** `0.0084` ($0.84\%$)

### 5.2 Decile Calibration Table

| Decile Bin | Mean Predicted Prob ($\bar{p}$) | Actual Fraction Positives ($y$) | Calibration Gap ($\Delta$) | Calibration Ratio ($\bar{p} / y$) |
|---|---|---|---|---|
| Bin 1 | 0.02% | 0.00% | +0.02% | N/A |
| Bin 2 | 0.13% | 0.00% | +0.13% | N/A |
| Bin 3 | 0.30% | 0.03% | +0.27% | 10.0x Overconfident |
| Bin 4 | 0.53% | 0.20% | +0.33% | 2.6x Overconfident |
| Bin 5 | 0.85% | 0.26% | +0.59% | 3.3x Overconfident |
| Bin 6 | 1.38% | 0.33% | +1.05% | 4.2x Overconfident |
| Bin 7 | 2.31% | 0.26% | +2.05% | 8.9x Overconfident |
| Bin 8 | 4.32% | 0.86% | +3.46% | 5.0x Overconfident |
| Bin 9 | 9.83% | 1.16% | +8.67% | 8.5x Overconfident |
| Bin 10 | 40.53% | 5.32% | +35.22% | 7.6x Overconfident |

### 5.3 Technical Diagnosis & Recommendation
* **Root Cause:** In [`scripts/train_fraud_model_remediated.py:427`](file:///home/syd/Downloads/archive/scripts/train_fraud_model_remediated.py#L427), the model was trained with `scale_pos_weight = 10.99` to overcome severe class imbalance. This intentionally dilates the log-odds by $\approx \ln(10.99) \approx 2.4$, causing the model to systematically output probabilities that are $7\times$ to $10\times$ higher than true posterior event rates.
* **Operational Recommendation:**
  1. **Do not interpret raw model scores as Bayesian probabilities.** They must be treated strictly as **ordinal risk rankings**.
  2. For downstream components that strictly require calibrated probabilities (e.g., expected loss calculations in core accounting ledgers), apply **Isotonic Regression** or **Platt Scaling** fitted on 2017 validation scores.
  3. For decision-policy thresholding, score calibration is unnecessary because threshold selection depends strictly on rank-order monotonic separation.

---

## 6. Multi-Tier Decision Policy Evaluation (Step 4)

To resolve the Base Rate Fallacy without sacrificing high fraud recall, a research-only 3-tier routing architecture was evaluated on validation data:

```
                                  [ Incoming Transaction ]
                                             |
                                  [ Score: p = P(Fraud) ]
                                             |
                   +-------------------------+-------------------------+
                   |                                                   |
             p < tau_low                                        p >= tau_low
                   |                                                   |
         [ TIER 1: AUTO-APPROVE ]                                      |
         - Frictionless Pass (Zero Cost)                               |
         - 75% to 85% of all traffic               +-------------------+-------------------+
                                                   |                                       |
                                       tau_low <= p < tau_high                      p >= tau_high
                                                   |                                       |
                                       [ TIER 2: STEP-UP 2FA ]                  [ TIER 3: SENIOR REVIEW ]
                                       - Automated 3DS / SMS OTP                - Priority Analyst Queue
                                       - Cost: $0.10 friction                   - Cost: $3.50 review
                                       - Catches 25% of frauds                  - Catches 52% of frauds
```

### 6.1 Multi-Tier Validation Comparison (Scenario A Costs)

| Policy Candidate $(\tau_{\text{low}}, \tau_{\text{high}})$ | % Auto Allow | % Step-Up 2FA | % Senior Review | Fraud Missed (Low) | Fraud Caught (2FA) | Fraud Caught (Review) | Total Fraud Catch Rate | Expected Cost / 100k Tx | Cost Reduction vs. Binary |
|---|---|---|---|---|---|---|---|---|---|
| **Binary Baseline (`0.0453`)** | 76.58% | 0.00% | 23.42% | 18.04% | 0.00% | 81.96% | **81.96%** | **$\$119,618.46$** | 0.0% (Reference) |
| **Policy 1: `(0.0453, 0.2000)`**| 76.50% | 15.87% | 7.63% | 18.04% | 25.10% | 56.86% | **81.96%** | **$\$31,281.48$** | **-73.9%** |
| **Policy 2: `(0.0453, 0.3000)`**| 76.50% | 18.45% | 5.05% | 18.04% | 29.80% | 52.16% | **81.96%** | **$\$22,518.48$** | **-81.2%** |
| **Policy 3: `(0.0600, 0.3000)`**| 80.27% | 14.68% | 5.05% | 23.14% | 24.71% | 52.16% | **76.86%** | **$\$22,851.17$** | **-80.9%** |
| **Policy 4: `(0.0800, 0.3500)`**| 83.62% | 12.15% | 4.23% | 26.67% | 24.31% | 49.02% | **73.33%** | **$\$20,237.25$** | **-83.1%** |
| **Policy 5: `(0.1000, 0.4000)`**| 86.15% | 10.34% | 3.51% | 30.98% | 23.92% | 45.10% | **69.02%** | **$\$18,153.95$** | **-84.8%** |

Full multi-scenario policy evaluations are archived in [`reports/phase2/multi_tier_policy.csv`](file:///home/syd/Downloads/archive/reports/phase2/multi_tier_policy.csv).

---

## 7. Single Final Evaluation on Held-Out Test Set (Step 5)

The policy parameters optimized on the 2017 Validation Set were applied exactly once to the untouched 2018–2019 Held-Out Test Set ($58,824$ transactions, $4,578$ frauds).

### 7.1 Single Binary Thresholds on Held-Out Test Data

| Threshold ($\tau$) | Status / Role | Test Recall | Test FPR | Test Specificity | Pop Precision | Pop Alerts / 1k | False Alarms / Fraud | Test Expected Cost / 100k Tx |
|---|---|---|---|---|---|---|---|---|
| `0.0453` | Candidate V2 Baseline | **77.63%** | 24.17% | 75.83% | 0.391% | 242.4 | 254.9 | **$\$123,980.00$** |
| `0.0690` | Val-Selected $\text{Rec} \ge 75\%$ | **70.86%** | 18.58% | 81.42% | 0.464% | 186.5 | 214.7 | **$\$96,720.00$** |
| `0.9500` | Val-Selected Min Cost | **7.21%** | 0.21% | 99.79% | 3.955% | 2.2 | 24.3 | **$\$14,640.00$** |

### 7.2 Multi-Tier Policies on Held-Out Test Data

| Policy Configuration | Auto-Approve % | 2FA Step-Up % | Senior Review % | Missed Fraud % | Total Fraud Catch Rate | Challenges / 1k | Reviews / 1k | Test Cost / 100k Tx | Test Cost / 1M Tx |
|---|---|---|---|---|---|---|---|---|---|
| **High-Catch Policy:** `(0.0453, 0.2000)` | 75.76% | 16.08% | 8.16% | 22.37% | **77.63%** | 160.8 | 81.6 | **$\$33,773.89$** | $\$337,738.89$ |
| **Balanced Policy:** `(0.1000, 0.4000)` | 85.50% | 10.64% | 3.87% | 34.69% | **65.31%** | 106.4 | 38.7 | **$\$19,975.14$** | $\$199,751.44$ |
| **Aggressive Policy:** `(0.0200, 0.1500)` | 62.36% | 27.07% | 10.58% | 13.30% | **86.70%** | 270.7 | 105.8 | **$\$42,046.24$** | $\$420,462.39$ |

Detailed test cohort results are archived in [`reports/phase2/test_set_evaluation.csv`](file:///home/syd/Downloads/archive/reports/phase2/test_set_evaluation.csv) and [`reports/phase2/test_set_multi_tier_evaluation.csv`](file:///home/syd/Downloads/archive/reports/phase2/test_set_multi_tier_evaluation.csv).

---

## 8. Real-Prevalence Impact & Uncertainty Analysis

Because genuine production prevalence fluctuates across merchant portfolios, sensitivity analysis was conducted across $\pi \in [0.05\%, 0.25\%]$:

```
Operational Precision vs. Fraud Prevalence at Threshold 0.0453 (TPR = 77.63%, FPR = 24.17%)
-----------------------------------------------------------------------------------------
Prevalence (pi = 0.05%):  Pop Precision = 0.160%  (623 false alarms per 1 fraud)
Prevalence (pi = 0.122%): Pop Precision = 0.391%  (255 false alarms per 1 fraud)  <-- Baseline
Prevalence (pi = 0.20%):  Pop Precision = 0.638%  (156 false alarms per 1 fraud)
Prevalence (pi = 0.50%):  Pop Precision = 1.583%  (62 false alarms per 1 fraud)
```

**Key Finding:** Even if true fraud prevalence increases fivefold to $0.50\%$, single binary thresholding still yields over 60 false alarms per caught fraud. Multi-tier routing remains essential across all plausible prevalence bounds.

---

## 9. Identified Limitations & Operational Risks

1. **Synthetic Data Artifacts:** The IBM transactions dataset exhibits static merchant categories and zero cardholder profile drift over its 20-year span. Real-world fraud patterns evolve rapidly as fraudsters adapt to velocity rules.
2. **Review Team Bottlenecks:** Policy `(0.0453, 0.2000)` routes $8.16\%$ of transactions to senior review ($81.6$ reviews per 1,000 transactions). For an institution processing 1,000,000 daily transactions, this represents **81,600 daily manual reviews**, exceeding standard tier-1 operations capacity. The more conservative policy `(0.1000, 0.4000)` reduces this burden to $38.7$ reviews per 1,000 transactions.
3. **Customer Drop-Off Friction:** While automated SMS OTP / 3DS challenges cost only $\$0.05$–$\$0.10$ in API routing fees, cardholders experiencing checkout friction may abandon e-commerce carts, indirectly impacting interchange revenue.
4. **Streaming Concurrency & Idempotency:** As established in Phase 1.3, the streaming Redis engine currently lacks atomic Lua state locking and transactional message deduplication keys.

---

## 10. Final Recommendation & Operational Conclusion

### 10.1 Definitive Operational Guidance
* **DO NOT deploy Candidate V2 as a single binary approve/decline threshold.** An alert rate of $24.2\%$ under a single threshold is economically unfeasible and operationally catastrophic.
* **RECOMMENDED TARGET POLICY:** Adopt a **3-Tier Decision Architecture**:
  * **Option A (High Catch Rate Focus):** $\tau_{\text{low}} = 0.0453, \tau_{\text{high}} = 0.3000$ (Catch rate $81.96\%$, $76.5\%$ auto-allowed, $5.0\%$ manual review).
  * **Option B (Operational Capacity Focus):** $\tau_{\text{low}} = 0.1000, \tau_{\text{high}} = 0.4000$ (Catch rate $65.31\%$, $85.5\%$ auto-allowed, $3.5\%$ manual review).

### 10.2 Next Steps Before Production Consideration
1. Implement a **Shadow Mode Consumer** streaming live traffic without blocking to observe real operational queue depths.
2. Conduct user experience tests to measure 3DS challenge completion and abandonment rates.
3. Harden the Redis StateStore with atomic Lua scripts and message deduplication keys.

---
*(Phase 2 complete. Deliverables produced and verified. No automatic blocking enabled.)*
