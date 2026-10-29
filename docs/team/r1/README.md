# R1: Sayed — ingestion / online model
**Inputs:** historical CSV, source master PostgreSQL, simulator settings. **Outputs:** versioned Kafka envelope, S3 raw/bronze files, CDC topics, MCC/Cities reference, stable transaction IDs and isolated simulation truth; real LightGBM predictions + alert streams if model and state available.
**Quick local test:** `python -m pytest tests/r1 tests/contracts -q`. **Reference only:** `src/r1_ingestion/generator.py` and `kafka_reference.py` are new fixture adapters, NOT original Sayed source.
**Required handoff:** sanitized actual code and configs, topic keys, model feature ordering/versions, 3 anomaly positive/negative event sequences, Redis fallback behavior, S3 file manifest and live test logs. Confirm cloud cost controls.
