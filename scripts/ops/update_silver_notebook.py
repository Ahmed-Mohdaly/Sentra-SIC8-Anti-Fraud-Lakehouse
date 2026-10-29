#!/usr/bin/env python3
"""
Script to safely update VALIDSIC8_08B_Streaming_Silver_Sayed_Contract_v2_1_Refund_Fix(1).ipynb
with:
1. Continuous Streaming Mode (processingTime='10 seconds') alongside AVAILABLE_NOW
2. In-stream Ingestion and Processing Lag calculations
3. CDC Dimension Enrichment (historical_users & historical_cards broadcast join)
4. Streaming Model Predictions table & Real-time Ground Truth Reconciliation
5. Publishing to workspace.sic8_streaming.streaming_transactions_enriched_v2
"""

import json
import sys

nb_path = "databricks/stream/VALIDSIC8_08B_Streaming_Silver_Sayed_Contract_v2_1_Refund_Fix(1).ipynb"

with open(nb_path, "r", encoding="utf-8") as f:
    nb = json.load(f)

# 1. Update Cell 0 Markdown
nb["cells"][0]["source"] = [
    "# SIC8 | 08B v3.0 — Continuous Streaming Silver & CDC Enrichment\n",
    "\n",
    "**R2 / Continuous Streaming Silver Tier.** This notebook implements the strict Sayed contract parser, isolates ground truth, computes in-stream lag, enriches transactions with CDC user and card dimensions, and reconciles real-time model predictions.\n",
    "\n",
    "### v3.0 Capabilities:\n",
    "1. **Continuous Streaming Mode:** Runs continuously (`processingTime='10 seconds'`) for 24/7 low-latency processing, or `availableNow=True` for bounded backfills.\n",
    "2. **Lag Telemetry:** Computes `ingestion_lag_seconds` (`current_timestamp() - event_time_utc`) and `processing_lag_seconds` (`current_timestamp() - source_modified_utc`).\n",
    "3. **CDC Dimension Enrichment:** Enriches transactions via broadcast join with `workspace.sic8_silver.historical_users` and `historical_cards` into `workspace.sic8_streaming.streaming_transactions_enriched_v2`.\n",
    "4. **Model Reconciliation:** Reconciles real-time model predictions (`streaming_model_predictions_v1`) against ground truth (`streaming_synthetic_truth_v2`) to monitor live precision, recall, and drift.\n"
]

# 2. Update Cell 1 Configuration
cell1_lines = """# Configuration — edit RUN_STAGE and STREAMING_MODE as needed.
RUN_STAGE = "SILVER"          # PREFLIGHT | SILVER
STREAMING_MODE = "CONTINUOUS" # CONTINUOUS | AVAILABLE_NOW
TRIGGER_INTERVAL = "10 seconds"

CATALOG = "workspace"
SCHEMA = "sic8_streaming"
RAW_TABLE = f"{CATALOG}.{SCHEMA}.streaming_raw_envelopes_v3"
TX_TABLE = f"{CATALOG}.{SCHEMA}.streaming_transactions_v2"
TRUTH_TABLE = f"{CATALOG}.{SCHEMA}.streaming_synthetic_truth_v2"
QUAR_TABLE = f"{CATALOG}.{SCHEMA}.streaming_quarantine_v2"
REG_TABLE = f"{CATALOG}.{SCHEMA}.streaming_event_registry_v2"

# Enhanced Tier: CDC Enrichment & Predictions Reconciliation
ENRICHED_TX_TABLE = f"{CATALOG}.{SCHEMA}.streaming_transactions_enriched_v2"
PREDICTIONS_TABLE = f"{CATALOG}.{SCHEMA}.streaming_model_predictions_v1"
DRIFT_METRICS_TABLE = f"{CATALOG}.{SCHEMA}.streaming_model_drift_metrics_v1"
DIM_USERS_TABLE = f"{CATALOG}.sic8_silver.historical_users"
DIM_CARDS_TABLE = f"{CATALOG}.sic8_silver.historical_cards"

STATE_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/streaming_state"
SILVER_CHECKPOINT = f"{STATE_VOLUME}/silver_transactions_v2/checkpoint"

assert RUN_STAGE in {"PREFLIGHT", "SILVER"}
assert STREAMING_MODE in {"CONTINUOUS", "AVAILABLE_NOW"}
spark.conf.set("spark.sql.session.timeZone", "UTC")
print("RUN_STAGE:", RUN_STAGE)
print("STREAMING_MODE:", STREAMING_MODE, f"(interval={TRIGGER_INTERVAL})")
print("RAW SOURCE:", RAW_TABLE)
print("SILVER TABLES:", TX_TABLE, TRUTH_TABLE, QUAR_TABLE, REG_TABLE)
print("ENRICHED SILVER TABLE:", ENRICHED_TX_TABLE)
print("SILVER CHECKPOINT:", SILVER_CHECKPOINT)
"""
nb["cells"][1]["source"] = [l + "\n" for l in cell1_lines.strip().split("\n")]

# 3. Update Cell 11 Publishing Schema
cell11_lines = """if RUN_STAGE == "SILVER":
    spark.sql(f\"\"\"CREATE TABLE IF NOT EXISTS {TX_TABLE} (
      event_id STRING, event_type STRING, schema_version STRING,
      transaction_id STRING, source_transaction_id STRING, transaction_id_origin STRING,
      user_id BIGINT, user_id_origin STRING, card_index BIGINT, scoped_card_key STRING,
      amount DECIMAL(18,2), currency STRING, currency_origin STRING,
      merchant_name STRING, merchant_city STRING, merchant_state STRING,
      merchant_latitude DOUBLE, merchant_longitude DOUBLE, zip STRING, mcc INT,
      use_chip STRING, transaction_entry_method STRING, errors STRING,
      source_system STRING, event_time_utc TIMESTAMP, source_event_time_local TIMESTAMP_NTZ,
      event_payload_hash STRING, source_file STRING, source_modified_utc TIMESTAMP,
      kafka_partition INT, kafka_offset BIGINT, kafka_key STRING, ingested_at_utc TIMESTAMP,
      ingestion_lag_seconds DOUBLE, processing_lag_seconds DOUBLE
    ) USING DELTA\"\"\")
    
    spark.sql(f\"\"\"CREATE TABLE IF NOT EXISTS {ENRICHED_TX_TABLE} (
      event_id STRING, event_type STRING, schema_version STRING,
      transaction_id STRING, source_transaction_id STRING, transaction_id_origin STRING,
      user_id BIGINT, user_id_origin STRING, card_index BIGINT, scoped_card_key STRING,
      amount DECIMAL(18,2), currency STRING, currency_origin STRING,
      merchant_name STRING, merchant_city STRING, merchant_state STRING,
      merchant_latitude DOUBLE, merchant_longitude DOUBLE, zip STRING, mcc INT,
      use_chip STRING, transaction_entry_method STRING, errors STRING,
      source_system STRING, event_time_utc TIMESTAMP, source_event_time_local TIMESTAMP_NTZ,
      event_payload_hash STRING, source_file STRING, source_modified_utc TIMESTAMP,
      kafka_partition INT, kafka_offset BIGINT, kafka_key STRING, ingested_at_utc TIMESTAMP,
      ingestion_lag_seconds DOUBLE, processing_lag_seconds DOUBLE,
      customer_id STRING, user_city STRING, user_region STRING,
      user_fico_score INT, user_age INT, user_num_cards INT, user_total_debt DECIMAL(18,2),
      card_brand STRING, card_type STRING, credit_limit DECIMAL(18,2),
      is_enriched BOOLEAN, enrichment_status STRING
    ) USING DELTA\"\"\")

    spark.sql(f\"\"\"CREATE TABLE IF NOT EXISTS {TRUTH_TABLE} (
      event_id STRING, synthetic_is_fraud BOOLEAN, event_payload_hash STRING,
      scenario STRING, run_id STRING
    ) USING DELTA\"\"\")
    
    spark.sql(f\"\"\"CREATE TABLE IF NOT EXISTS {QUAR_TABLE} (
      quarantine_id STRING, event_id STRING, quarantine_reason STRING,
      raw_json_line STRING, source_file STRING, kafka_offset BIGINT,
      ingested_at_utc TIMESTAMP, remediation_status STRING
    ) USING DELTA\"\"\")
    
    spark.sql(f\"\"\"CREATE TABLE IF NOT EXISTS {REG_TABLE} (
      event_id STRING, event_payload_hash STRING, source_file STRING,
      first_ingested_at_utc TIMESTAMP
    ) USING DELTA\"\"\")

    spark.sql(f\"\"\"CREATE TABLE IF NOT EXISTS {PREDICTIONS_TABLE} (
      event_id STRING, prediction_score DOUBLE, predicted_label BOOLEAN,
      latency_ms DOUBLE, model_version STRING, predicted_at_utc TIMESTAMP
    ) USING DELTA\"\"\")

    spark.sql(f\"\"\"CREATE TABLE IF NOT EXISTS {DRIFT_METRICS_TABLE} (
      reconciled_at_utc TIMESTAMP, batch_id BIGINT, total_evaluated BIGINT,
      true_positives BIGINT, false_positives BIGINT, true_negatives BIGINT, false_negatives BIGINT,
      precision DOUBLE, recall DOUBLE, f1_score DOUBLE, mean_latency_ms DOUBLE
    ) USING DELTA\"\"\")

    expected = {TX_TABLE: {"event_id", "user_id", "card_index", "amount", "event_payload_hash"},
                ENRICHED_TX_TABLE: {"event_id", "user_id", "user_fico_score", "card_brand", "is_enriched"},
                TRUTH_TABLE: {"event_id", "synthetic_is_fraud", "event_payload_hash"},
                QUAR_TABLE: {"quarantine_id", "quarantine_reason"},
                REG_TABLE: {"event_id", "event_payload_hash"}}
    for name, columns in expected.items():
        found = set(spark.table(name).columns)
        if columns - found:
            raise RuntimeError(f"SILVER_TABLE_SCHEMA_MISMATCH {name}: {sorted(columns-found)}")
    try:
        dbutils.fs.ls(SILVER_CHECKPOINT)
        has_checkpoint = True
    except Exception:
        has_checkpoint = False
    print("SILVER_V3_OUTPUTS_READY", "checkpoint_present:", has_checkpoint)
else:
    print("PREFLIGHT: No Silver output tables created")
"""
nb["cells"][11]["source"] = [l + "\n" for l in cell11_lines.strip().split("\n")]

# 4. Update Cell 13 (silver_v2_microbatch logic)
cell13_lines = """def silver_v2_microbatch(batch_df, batch_id):
    session = batch_df.sparkSession
    TX = TX_TABLE
    TRUTH = TRUTH_TABLE
    QUAR = QUAR_TABLE
    REG = REG_TABLE
    ENRICHED_TX = ENRICHED_TX_TABLE
    PREDS = PREDICTIONS_TABLE
    DRIFT = DRIFT_METRICS_TABLE
    DIM_U = DIM_USERS_TABLE
    DIM_C = DIM_CARDS_TABLE

    def parse_sayed_raw(frame):
        # Local definition ensures pure closure across executors
        def get(k):
            return F.coalesce(
                F.get_json_object(F.col("_business"), f\"$.{k}\"),
                F.get_json_object(F.col("_business"), f\"$.['{k}']\"),
                F.get_json_object(F.col("_business"), f\"$.\\\"{k}\\\"\")
            )

        p = (frame
            .withColumn(\"_event_json_str\", F.get_json_object(F.col(\"raw_json_line\"), \"$.value\"))
            .withColumn(\"_is_kafka_wrapper\", F.col(\"_event_json_str\").isNotNull())
            .withColumn(\"_envelope\", F.when(F.col(\"_is_kafka_wrapper\"), F.col(\"_event_json_str\"))
                                        .otherwise(F.col(\"raw_json_line\")))
            .withColumn(\"event_id_parsed\", F.coalesce(F.get_json_object(F.col(\"_envelope\"), \"$.event_id\"), F.col(\"event_id\")))
            .withColumn(\"event_type_parsed\", F.coalesce(F.get_json_object(F.col(\"_envelope\"), \"$.event_type\"), F.col(\"event_type\")))
            .withColumn(\"schema_version_parsed\", F.coalesce(F.get_json_object(F.col(\"_envelope\"), \"$.schema_version\"), F.col(\"schema_version\")))
            .withColumn(\"event_time_raw\", F.coalesce(
                F.get_json_object(F.col(\"_envelope\"), \"$.event_timestamp\"),
                F.get_json_object(F.col(\"_envelope\"), \"$.event_time\"),
                F.get_json_object(F.col(\"_envelope\"), \"$.timestamp\")))
            .withColumn(\"source_system\", F.get_json_object(F.col(\"_envelope\"), \"$.source_system\"))
            .withColumn(\"run_id\", F.get_json_object(F.col(\"_envelope\"), \"$.run_id\"))
            .withColumn(\"scenario\", F.get_json_object(F.col(\"_envelope\"), \"$.scenario\"))
            .withColumn(\"_payload_candidate\", F.get_json_object(F.col(\"_envelope\"), \"$.payload\"))
            .withColumn(\"_data_candidate\", F.get_json_object(F.col(\"_envelope\"), \"$.data\"))
            .withColumn(\"_business\", F.coalesce(F.col(\"_payload_candidate\"), F.col(\"_data_candidate\"))))

        p = (p.withColumn(\"user_raw\", F.coalesce(get(\"User\"), get(\"user_id\"), get(\"user\")))
            .withColumn(\"card_raw\", F.coalesce(get(\"Card\"), get(\"card_id\"), get(\"card_index\"), get(\"card_idx\")))
            .withColumn(\"amount_raw\", F.coalesce(get(\"Amount\"), get(\"amount\"), get(\"transaction_amount\")))
            .withColumn(\"fraud_raw\", F.coalesce(get(\"Is Fraud?\"), get(\"is_fraud\"), get(\"fraud_label\")))
            .withColumn(\"mcc_raw\", F.coalesce(get(\"MCC\"), get(\"mcc\"), get(\"merchant_category_code\")))
            .withColumn(\"merchant_name\", F.coalesce(get(\"Merchant Name\"), get(\"merchant_name\")))
            .withColumn(\"merchant_city\", F.coalesce(get(\"Merchant City\"), get(\"merchant_city\")))
            .withColumn(\"merchant_state\", F.coalesce(get(\"Merchant State\"), get(\"merchant_state\")))
            .withColumn(\"merchant_lat_raw\", F.coalesce(get(\"Merchant Latitude\"), get(\"merchant_latitude\")))
            .withColumn(\"merchant_lon_raw\", F.coalesce(get(\"Merchant Longitude\"), get(\"merchant_longitude\")))
            .withColumn(\"zip_raw\", F.coalesce(get(\"Zip\"), get(\"zip\"), get(\"zip_code\")))
            .withColumn(\"use_chip\", F.coalesce(get(\"Use Chip\"), get(\"use_chip\")))
            .withColumn(\"transaction_entry_method\", F.coalesce(get(\"Transaction Entry Method\"), get(\"transaction_entry_method\")))
            .withColumn(\"errors\", F.coalesce(get(\"Errors?\"), get(\"errors\")))
            .withColumn(\"currency_raw\", F.coalesce(get(\"currency\"), get(\"Currency\")))
            .withColumn(\"source_transaction_id\", F.coalesce(get(\"transaction_id\"), get(\"tx_id\")))
            .withColumn(\"local_year\", get(\"Year\"))
            .withColumn(\"local_month\", get(\"Month\"))
            .withColumn(\"local_day\", get(\"Day\"))
            .withColumn(\"local_clock\", get(\"Time\")))

        p = (p.withColumn(\"payload_user_numeric\", F.expr(\"try_cast(trim(user_raw) AS BIGINT)\"))
            .withColumn(\"kafka_user_numeric\", F.expr(\"try_cast(trim(kafka_key) AS BIGINT)\"))
            .withColumn(\"user_id\", F.when(F.col(\"user_raw\").isNotNull(), F.col(\"payload_user_numeric\"))
                    .otherwise(F.col(\"kafka_user_numeric\")))
            .withColumn(\"user_id_origin\", F.when(F.col(\"user_raw\").isNotNull(), F.lit(\"PAYLOAD\"))
                    .when(F.col(\"kafka_user_numeric\").isNotNull(), F.lit(\"KAFKA_KEY_VERIFIED_FALLBACK\"))
                    .otherwise(F.lit(\"UNKNOWN\")))
            .withColumn(\"card_index\", F.expr(\"try_cast(trim(card_raw) AS BIGINT)\"))
            .withColumn(\"mcc\", F.expr(\"try_cast(trim(mcc_raw) AS INT)\"))
            .withColumn(\"merchant_latitude\", F.expr(\"try_cast(trim(merchant_lat_raw) AS DOUBLE)\"))
            .withColumn(\"merchant_longitude\", F.expr(\"try_cast(trim(merchant_lon_raw) AS DOUBLE)\"))
            .withColumn(\"zip\", F.regexp_replace(F.trim(\"zip_raw\"), r\"\\.0$\", \"\"))
            .withColumn(\"event_time_utc\", F.expr(\"try_to_timestamp(event_time_raw)\")))

        p = (p.withColumn(\"currency\", F.when(F.col(\"currency_raw\").isNotNull(), F.upper(F.trim(\"currency_raw\")))
                      .when(F.col(\"amount_raw\").rlike(r\"^\\s*(?:-?\\$|\\$-)\"), F.lit(\"USD\"))
                      .otherwise(F.lit(None).cast(\"string\")))
            .withColumn(\"currency_origin\", F.when(F.col(\"currency_raw\").isNotNull(), \"EXPLICIT\")
                      .when(F.col(\"amount_raw\").rlike(r\"^\\s*(?:-?\\$|\\$-)\"), \"IMPLIED_DOLLAR_SIGN\")
                      .otherwise(\"UNKNOWN\"))
            .withColumn(\"amount_stripped\", F.regexp_replace(F.trim(\"amount_raw\"), r\"[\\$,]\", \"\"))
            .withColumn(\"amount\", F.expr(\"try_cast(amount_stripped AS DECIMAL(18,2))\"))
            .withColumn(\"fraud_norm\", F.lower(F.trim(\"fraud_raw\")))
            .withColumn(\"synthetic_is_fraud\", F.when(F.col(\"fraud_norm\").isin(\"yes\", \"true\", \"1\"), F.lit(True))
                      .when(F.col(\"fraud_norm\").isin(\"no\", \"false\", \"0\"), F.lit(False))
                      .otherwise(F.lit(None).cast(\"boolean\")))
            .withColumn(\"scoped_card_key\", F.concat_ws(\":\", \"user_id\", \"card_index\"))
            .withColumn(\"kafka_partition\", F.expr(\"try_cast(regexp_extract(source_file, 'partition=([0-9]+)', 1) AS INT)\"))
            .withColumn(\"transaction_id_origin\", F.when(F.length(F.trim(\"source_transaction_id\"))>0, \"SOURCE\")
                      .otherwise(\"DERIVED_FROM_EVENT_ID\"))
            .withColumn(\"transaction_id\", F.when(F.col(\"transaction_id_origin\")==\"SOURCE\", F.col(\"source_transaction_id\"))
                      .otherwise(F.concat(F.lit(\"EVENT_\"), F.col(\"event_id_parsed\")))))

        local_text = F.concat(F.lpad(F.trim(\"local_year\"), 4, \"0\"), F.lit(\"-\"),
                              F.lpad(F.trim(\"local_month\"), 2, \"0\"), F.lit(\"-\"),
                              F.lpad(F.trim(\"local_day\"), 2, \"0\"), F.lit(\" \"),
                              F.when(F.trim(\"local_clock\").rlike(r\"^[0-9]{2}:[0-9]{2}$\"),
                                     F.concat(F.trim(\"local_clock\"), F.lit(\":00\")))
                               .otherwise(F.trim(\"local_clock\")))
        p = p.withColumn(\"_local_text\", local_text).withColumn(
            \"source_event_time_local\", F.expr(\"try_cast(_local_text AS TIMESTAMP_NTZ)\"))

        payload_canonical = F.to_json(F.map_from_entries(F.sort_array(F.map_entries(F.col(\"_business\")))))
        p = (p.withColumn(\"_canonical_payload\", payload_canonical)
            .withColumn(\"event_payload_hash\", F.sha2(F.to_json(F.struct(
                \"event_id_parsed\", \"event_type_parsed\", \"schema_version_parsed\", \"event_time_raw\",
                \"source_system\", \"run_id\", \"scenario\", \"_canonical_payload\"),
                {\"ignoreNullFields\": \"false\"}), 256))
            .withColumn(\"quarantine_id\", F.sha2(F.to_json(F.struct(
                \"source_file\", \"kafka_offset\", \"raw_json_line\"), {\"ignoreNullFields\": \"false\"}), 256)))

        reason = (F.when(F.col(\"_envelope\").isNull() | F.col(\"_business\").isNull(), \"UNPARSEABLE_ENVELOPE_OR_PAYLOAD\")
            .when(F.col(\"event_id_parsed\").isNull() | (F.length(\"event_id_parsed\")==0), \"MISSING_EVENT_ID\")
            .when(F.col(\"event_id\").isNotNull() & (F.col(\"event_id\") != F.col(\"event_id_parsed\")), \"RAW_EVENT_ID_MISMATCH\")
            .when(~F.col(\"schema_version_parsed\").isin(\"1\", \"1.0.0\") |
                  F.col(\"schema_version_parsed\").isNull(), \"UNSUPPORTED_SCHEMA_VERSION\")
            .when(F.col(\"event_type_parsed\").isNull() | (F.col(\"event_type_parsed\") != \"transaction.created\"), \"UNEXPECTED_EVENT_TYPE\")
            .when(F.col(\"user_raw\").isNotNull() & F.col(\"payload_user_numeric\").isNull(), \"INVALID_USER_ID\")
            .when(F.col(\"user_id\").isNull() | (F.col(\"user_id\") < 0), \"MISSING_CARD_IDENTITY\")
            .when(F.col(\"kafka_user_numeric\").isNotNull() & F.col(\"user_raw\").isNotNull() &
                  (F.col(\"payload_user_numeric\") != F.col(\"kafka_user_numeric\")), \"KAFKA_USER_MISMATCH\")
            .when(F.col(\"card_index\").isNull() | (F.col(\"card_index\") < 0), \"MISSING_CARD_IDENTITY\")
            .when(F.col(\"amount_raw\").isNull() |
                  (~F.trim(\"amount_raw\").rlike(r\"^(?:-?\\$?|\\$-)(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\\.[0-9]{1,2})?$\")) |
                  F.col(\"amount\").isNull(), \"BAD_AMOUNT\")
            .when(F.col(\"currency\").isNull() | (~F.col(\"currency\").rlike(r\"^[A-Z]{3}$\")), \"BAD_CURRENCY\")
            .when(F.col(\"event_time_utc\").isNull(), \"BAD_EVENT_TIME\")
            .when(F.col(\"source_event_time_local\").isNull() & F.col(\"local_year\").isNotNull(), \"BAD_LOCAL_DATETIME\")
            .when(F.col(\"mcc\").isNull() | (F.col(\"mcc\") < 0), \"BAD_MCC\")
            .when(F.col(\"synthetic_is_fraud\").isNull(), \"BAD_SYNTHETIC_TRUTH\")
            .when((F.col(\"merchant_lat_raw\").isNotNull() & F.col(\"merchant_latitude\").isNull()) |
                  (F.col(\"merchant_lon_raw\").isNotNull() & F.col(\"merchant_longitude\").isNull()) |
                  (F.col(\"merchant_latitude\").isNull() != F.col(\"merchant_longitude\").isNull()) |
                  (F.abs(\"merchant_latitude\") > 90) | (F.abs(\"merchant_longitude\") > 180) |
                  F.isnan(\"merchant_latitude\") | F.isnan(\"merchant_longitude\"), \"INVALID_COORDINATES\")
            .otherwise(F.lit(None).cast(\"string\")))
        return p.withColumn(\"quarantine_reason\", reason)

    parsed = parse_sayed_raw(batch_df).persist()
    try:
        totals = parsed.agg(
            F.count(\"*\").alias(\"n\"),
            F.sum(F.col(\"quarantine_reason\").isNull().cast(\"long\")).alias(\"valid_n\")
        ).first()
        n = int(totals.n)
        if n == 0:
            print(f\"SILVER_EMPTY_BATCH batch={batch_id}\")
            return
        good = int(totals.valid_n or 0)
        if good == 0 or good / n < 0.50:
            raise RuntimeError(f\"SILVER_CONTRACT_DRIFT batch={batch_id} valid={good} total={n}; stop before write\")

        # Reject conflicting event IDs within batch
        keyed = parsed.filter(F.col(\"event_id_parsed\").isNotNull() & (F.length(\"event_id_parsed\") > 0))
        conflict = keyed.groupBy(\"event_id_parsed\").agg(
            F.countDistinct(\"event_payload_hash\").alias(\"hashes\")).filter(\"hashes > 1\").limit(1).count()
        if conflict:
            raise RuntimeError(f\"EVENT_ID_CONFLICT_IN_BATCH batch={batch_id}\")
        
        reg_w = Window.partitionBy(\"event_id_parsed\").orderBy(
            F.col(\"source_file\").asc_nulls_last(), F.col(\"kafka_offset\").asc_nulls_last())
        reg_src = (keyed.withColumn(\"rn_reg\", F.row_number().over(reg_w)).filter(\"rn_reg=1\")
            .select(F.col(\"event_id_parsed\").alias(\"event_id\"), \"event_payload_hash\",
                    \"source_file\", F.col(\"ingested_at_utc\").alias(\"first_ingested_at_utc\")))
        
        conflict_stored = (reg_src.alias(\"i\").join(session.table(REG).alias(\"r\"), \"event_id\", \"inner\")
             .filter(F.col(\"i.event_payload_hash\") != F.col(\"r.event_payload_hash\")).limit(1).count())
        if conflict_stored:
            raise RuntimeError(f\"EVENT_ID_CONFLICT_AGAINST_REGISTRY batch={batch_id}\")

        w = Window.partitionBy(\"event_id_parsed\").orderBy(
             F.col(\"source_file\").asc_nulls_last(), F.col(\"kafka_offset\").asc_nulls_last())
        good_src = (parsed.filter(F.col(\"quarantine_reason\").isNull())
                    .withColumn(\"rn\", F.row_number().over(w)).filter(\"rn=1\").drop(\"rn\"))
        
        # Calculate in-stream Lag Telemetry
        good_src = (good_src
            .withColumn(\"ingestion_lag_seconds\",
                F.round(F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col(\"event_time_utc\")), 2))
            .withColumn(\"processing_lag_seconds\",
                F.round(F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col(\"source_modified_utc\")), 2)))

        tx = good_src.select(
            F.col(\"event_id_parsed\").alias(\"event_id\"),
            F.col(\"event_type_parsed\").alias(\"event_type\"),
            F.col(\"schema_version_parsed\").alias(\"schema_version\"),
            \"transaction_id\", \"source_transaction_id\", \"transaction_id_origin\",
            \"user_id\", \"user_id_origin\", \"card_index\", \"scoped_card_key\", \"amount\", \"currency\", \"currency_origin\",
            \"merchant_name\", \"merchant_city\", \"merchant_state\", \"merchant_latitude\", \"merchant_longitude\",
            \"zip\", \"mcc\", \"use_chip\", \"transaction_entry_method\", \"errors\", \"source_system\",
            \"event_time_utc\", \"source_event_time_local\", \"event_payload_hash\", \"source_file\",
            \"source_modified_utc\", \"kafka_partition\", \"kafka_offset\", \"kafka_key\", \"ingested_at_utc\",
            \"ingestion_lag_seconds\", \"processing_lag_seconds\"
        )
        
        truth = good_src.select(
            F.col(\"event_id_parsed\").alias(\"event_id\"),
            \"synthetic_is_fraud\", \"event_payload_hash\", \"scenario\", \"run_id\"
        )
        
        bad = (parsed.filter(F.col(\"quarantine_reason\").isNotNull())
               .withColumn(\"remediation_status\", F.lit(\"NEW\"))
               .select(\"quarantine_id\", F.col(\"event_id_parsed\").alias(\"event_id\"),
                       \"quarantine_reason\", \"raw_json_line\", \"source_file\", \"kafka_offset\",
                       \"ingested_at_utc\", \"remediation_status\")
               .dropDuplicates([\"quarantine_id\"]))

        # CDC Dimension Enrichment (Broadcast Join)
        try:
            dim_u = session.table(DIM_U).select(
                F.col(\"source_user_index\").alias(\"dim_user_id\"),
                F.col(\"customer_id\").alias(\"dim_customer_id\"),
                F.col(\"customer_city\").alias(\"user_city\"),
                F.col(\"customer_region\").alias(\"user_region\"),
                F.col(\"fico_score\").alias(\"user_fico_score\"),
                F.col(\"current_age\").alias(\"user_age\"),
                F.col(\"num_credit_cards\").alias(\"user_num_cards\"),
                F.col(\"total_debt\").alias(\"user_total_debt\")
            )
            dim_c = session.table(DIM_C).select(
                F.col(\"customer_id\").alias(\"dim_c_cust_id\"),
                F.col(\"source_card_index\").cast(\"bigint\").alias(\"dim_c_card_idx\"),
                F.col(\"card_brand\"),
                F.col(\"card_type\"),
                F.col(\"credit_limit\")
            )
            tx_enriched = (tx.join(F.broadcast(dim_u), tx[\"user_id\"] == dim_u[\"dim_user_id\"], \"left\")
                             .join(F.broadcast(dim_c),
                                   (F.col(\"dim_customer_id\") == F.col(\"dim_c_cust_id\")) &
                                   (tx[\"card_index\"] == F.col(\"dim_c_card_idx\")), \"left\")
                             .withColumn(\"customer_id\", F.col(\"dim_customer_id\"))
                             .withColumn(\"is_enriched\", F.col(\"user_fico_score\").isNotNull())
                             .withColumn(\"enrichment_status\",
                                         F.when(F.col(\"user_fico_score\").isNotNull(), F.lit(\"ENRICHED\"))
                                          .otherwise(F.lit(\"PENDING_USER_CDC\")))
                             .drop(\"dim_user_id\", \"dim_customer_id\", \"dim_c_cust_id\", \"dim_c_card_idx\"))
        except Exception as enrich_exc:
            print(\"CDC_ENRICHMENT_DIAGNOSTIC:\", str(enrich_exc)[:200])
            tx_enriched = (tx.withColumn(\"customer_id\", F.lit(None).cast(\"string\"))
                             .withColumn(\"user_city\", F.lit(None).cast(\"string\"))
                             .withColumn(\"user_region\", F.lit(None).cast(\"string\"))
                             .withColumn(\"user_fico_score\", F.lit(None).cast(\"int\"))
                             .withColumn(\"user_age\", F.lit(None).cast(\"int\"))
                             .withColumn(\"user_num_cards\", F.lit(None).cast(\"int\"))
                             .withColumn(\"user_total_debt\", F.lit(None).cast(\"decimal(18,2)\"))
                             .withColumn(\"card_brand\", F.lit(None).cast(\"string\"))
                             .withColumn(\"card_type\", F.lit(None).cast(\"string\"))
                             .withColumn(\"credit_limit\", F.lit(None).cast(\"decimal(18,2)\"))
                             .withColumn(\"is_enriched\", F.lit(False))
                             .withColumn(\"enrichment_status\", F.lit(\"LOOKUP_OFFLINE\")))

        def insert_only(target, source, view, key):
            source.createOrReplaceTempView(view)
            cols = source.columns
            columns = \", \".join(f\"`{col}`\" for col in cols)
            values = \", \".join(f\"s.`{col}`\" for col in cols)
            session.sql(f\"MERGE INTO {target} AS t USING {view} AS s ON t.`{key}`=s.`{key}` \"
                        f\"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})\").collect()

        insert_only(TX, tx, \"_sic8_v2_tx_batch\", \"event_id\")
        insert_only(ENRICHED_TX, tx_enriched, \"_sic8_v2_tx_enriched_batch\", \"event_id\")
        insert_only(TRUTH, truth, \"_sic8_v2_truth_batch\", \"event_id\")
        insert_only(QUAR, bad, \"_sic8_v2_quar_batch\", \"quarantine_id\")
        insert_only(REG, reg_src, \"_sic8_v2_reg_batch\", \"event_id\")

        # Optional Model Predictions Reconciliation (if predictions are present)
        try:
            if session.catalog.tableExists(PREDS) and session.catalog.tableExists(DRIFT):
                preds_df = session.table(PREDS)
                rec = preds_df.join(truth, \"event_id\", \"inner\").select(
                    \"event_id\", F.col(\"predicted_label\"), F.col(\"synthetic_is_fraud\").alias(\"actual_label\"), \"latency_ms\"
                )
                rec_cnt = rec.count()
                if rec_cnt > 0:
                    tp = rec.filter(\"predicted_label = true and actual_label = true\").count()
                    fp = rec.filter(\"predicted_label = true and actual_label = false\").count()
                    tn = rec.filter(\"predicted_label = false and actual_label = false\").count()
                    fn = rec.filter(\"predicted_label = false and actual_label = true\").count()
                    prec = (tp / (tp + fp)) if (tp + fp) > 0 else 0.0
                    reca = (tp / (tp + fn)) if (tp + fn) > 0 else 0.0
                    f1 = (2 * prec * reca / (prec + reca)) if (prec + reca) > 0 else 0.0
                    mean_lat = rec.agg(F.avg(\"latency_ms\")).first()[0] or 0.0
                    drift_row = session.createDataFrame([{
                        \"reconciled_at_utc\": F.current_timestamp(),
                        \"batch_id\": int(batch_id),
                        \"total_evaluated\": rec_cnt,
                        \"true_positives\": tp,
                        \"false_positives\": fp,
                        \"true_negatives\": tn,
                        \"false_negatives\": fn,
                        \"precision\": float(prec),
                        \"recall\": float(reca),
                        \"f1_score\": float(f1),
                        \"mean_latency_ms\": float(mean_lat)
                    }])
                    drift_row.write.format(\"delta\").mode(\"append\").saveAsTable(DRIFT)
        except Exception as pred_exc:
            pass

        avg_lag = good_src.agg(F.avg(\"ingestion_lag_seconds\")).first()[0] if good > 0 else 0.0
        print(f\"SILVER_BATCH_SUCCESS batch={batch_id} raw={n} accepted={good} invalid={n-good} avg_lag_sec={avg_lag}\")
    finally:
        parsed.unpersist()
"""
nb["cells"][13]["source"] = [l + "\n" for l in cell13_lines.strip().split("\n")]

# 5. Update Cell 15 (Trigger execution)
cell15_lines = """if RUN_STAGE == "SILVER":
    raw_stream = spark.readStream.format("delta").table(RAW_TABLE)
    trigger_arg = {"processingTime": TRIGGER_INTERVAL} if STREAMING_MODE == "CONTINUOUS" else {"availableNow": True}
    query = (raw_stream.writeStream.foreachBatch(silver_v2_microbatch)
             .option("checkpointLocation", SILVER_CHECKPOINT)
             .trigger(**trigger_arg).start())
    print(f"SILVER_STREAM_STARTED (mode={STREAMING_MODE}, trigger={trigger_arg})")
    if STREAMING_MODE == "AVAILABLE_NOW":
        query.awaitTermination()
        if query.exception() is not None:
            raise RuntimeError("SILVER_STREAM_ERROR: " + str(query.exception()))
        print("SILVER_STREAM_COMPLETED", query.lastProgress)
    else:
        print("Silver continuous stream is active. Awaiting new micro-batches...")
        query.awaitTermination()
else:
    print("PREFLIGHT: no streaming query started")
"""
nb["cells"][15]["source"] = [l + "\n" for l in cell15_lines.strip().split("\n")]

with open(nb_path, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1)

print("08B successfully updated with Continuous Mode, Lag Telemetry & CDC Enrichment!")
