# Verified 34-Feature Contract Registry (R1 LightGBM v2)

**Model Version:** `v2_remediated`  
**Trained Timestamp:** `2026-10-28T18:38:11.682894`  
**Optimal Decision Threshold:** `0.0453`  

## 1. Feature Specifications (Exact Ordering & Types)

| Index | Feature Name | Description | Source / Extraction |
|---|---|---|---|
| 0 | `amount` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 1 | `log_amount` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 2 | `spend_deviation_ratio` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 3 | `credit_limit_utilization` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 4 | `amount_zscore` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 5 | `current_age` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 6 | `fico_score` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 7 | `yearly_income` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 8 | `total_debt` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 9 | `num_credit_cards` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 10 | `credit_limit` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 11 | `card_age_at_tx_years` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 12 | `years_since_pin_changed` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 13 | `distance_to_home_km` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 14 | `time_since_last_tx_sec` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 15 | `travel_speed_kmh` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 16 | `tx_count_last_1h` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 17 | `tx_count_last_24h` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 18 | `spend_sum_last_1h` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 19 | `is_night_tx` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 20 | `sin_hour` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 21 | `cos_hour` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 22 | `has_chip` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 23 | `card_on_dark_web` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 24 | `use_chip_code` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 25 | `card_brand_code` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 26 | `card_type_code` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 27 | `entry_method_code` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 28 | `is_online_tx` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 29 | `is_credential_error` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 30 | `is_insufficient_balance` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 31 | `is_refund` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 32 | `mcc_high_risk_flag` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |
| 33 | `mcc_risk_score` | Real-time engineered feature | `src/r1_fraud_online/feature_engineering.py` |

## 2. Validation & Prevalence Performance
- **AUC-ROC:** `0.8746438880737777`
- **PR-AUC:** `0.10816051211250512`
- **Production Prevalence (0.122%):** Precision `None`, Recall `None`

## 3. Top Gain Features
- `mcc_risk_score`: Split Gain = None
- `use_chip_code`: Split Gain = None
- `spend_sum_last_1h`: Split Gain = None
- `entry_method_code`: Split Gain = None
- `time_since_last_tx_sec`: Split Gain = None
- `spend_deviation_ratio`: Split Gain = None
- `cos_hour`: Split Gain = None
- `yearly_income`: Split Gain = None
- `credit_limit`: Split Gain = None
- `sin_hour`: Split Gain = None
