# Historical experiments — past *reported* measured exploratory outcomes, not deployed detectors

These figures are copied from the supplied master handover; **the executed notebooks/logs are not in this workspace** and this repo did not rerun the experiments.

| Experiment | Reported findings | Interpretation |
|---|---|---|
| CNP >= 2 in 10m | 42,296 flagged; TP 2,469; FP 39,827; precision 5.84%; recall 8.30% | weak and noisy |
| CNP >= 3 in 10m | 194 flagged; TP 19 | low recall / restrictive |
| Micro >=2 | 521 flagged; TP 5 | not enough evidence for automatic decisions |
| LR out-of-time 2019–2020 | precision ~4.16%; recall ~10.21%; PR-AUC ~0.00684 | exploratory baseline |
| GBT same test | precision ~4.30%; recall ~3.21%; PR-AUC ~0.0036617 | weak baseline |
| R1 LightGBM v2 | reported ROC-AUC 0.8746, fraud recall 81.18%, ~6.4ms latency, threshold ~0.0453 | **UNVERIFIED** without code, data split/feature registry, prediction logs and hardware/test context |

Historical base rate ~0.122%, so raw accuracy can be misleading. Never treat historical labels as actual industry bank data. No test tuning on an already examined test split. Different train/test windows or feature leakage (especially `mcc_risk_score`) make cross-model comparisons invalid until R1 evidence and parity audit arrive.
