# Streaming Data Specification & Transformation Guide
> **Target Audience:** Data Engineers, Analytics Engineers, and Databricks / Spark Pipeline Developers.  
> **Topic:** End-to-End Streaming Schema, Parsing Logic, Silver Transformations, and Data Quality Contracts.

---

## 1. Architecture Context & Ingestion Path

This document defines the exact contract of the real-time streaming transaction data produced by the streaming generator and ingested into the Lakehouse.

```
+-------------------------------------------------------------+
|        Synthetic Financial Transaction Engine (FastAPI)     |
+-------------------------------------------------------------+
                              |
                     Publishes JSON Events
                              v
+-------------------------------------------------------------+
|        Amazon MSK (Apache Kafka Topic: `transactions`)      |
|             Key: User ID (Ensures User Partitioning)        |
+-------------------------------------------------------------+
         |                                           |
         v                                           v
+------------------------+           +---------------------------------+
| Real-Time ML Consumer  |           | Aiven Kafka S3 Sink Connector   |
| (Sub-5ms LightGBM)     |           | Bronze Landing (`.json.gz`)     |
+------------------------+           +---------------------------------+
                                                     |
                                                     v
                                     +---------------------------------+
                                     | s3://sic-pipeline-storage/      |
                                     | bronze/transactions/partition=* |
                                     +---------------------------------+
                                                     |
                                      Databricks Auto Loader
                                                     v
                                     +---------------------------------+
                                     | Delta Raw Envelopes             |
                                     | `streaming_raw_envelopes_v3`    |
                                     +---------------------------------+
                                                     |
                                     Streaming Transformation (Silver)
                                                     v
                         +---------------------------+---------------------------+
                         |                                                       |
                         v                                                       v
        +---------------------------------+                     +---------------------------------+
        | Silver Cleansed Transactions    |                     | Silver Ground Truth Table       |
        | `streaming_transactions_v2`     |                     | `streaming_synthetic_truth_v2`  |
        +---------------------------------+                     +---------------------------------+
                         |
                         v (Quarantine Rows)
        +---------------------------------+
        | Streaming Quarantine / DLQ      |
        | `streaming_quarantine_v2`       |
        +---------------------------------+
```

---

## 2. Streaming Event Message Structure

Each event emitted to Kafka and landed into S3 follows the **Event Envelope Pattern**.

### 2.1 Complete JSON Example (Kafka Message Value)

```json
{
  "event_id": "edc89ac2-125b-47ef-9164-57290ca0e051",
  "event_type": "transaction.created",
  "schema_version": 1,
  "event_timestamp": "2026-10-29T14:35:10.120000+00:00",
  "source_system": "POS_TERMINAL",
  "run_id": "run-001",
  "scenario": "normal_spend",
  "payload": {
    "User": 2042,
    "Card": 0,
    "Year": 2026,
    "Month": 10,
    "Day": 29,
    "Time": "14:35",
    "Amount": "$45.20",
    "Use Chip": "Chip Transaction",
    "Transaction Entry Method": "contactless_chip",
    "Merchant Name": 12847192,
    "Merchant City": "Cleveland",
    "Merchant State": "OH",
    "Merchant Latitude": 41.4993,
    "Merchant Longitude": -81.6944,
    "Zip": "44101",
    "MCC": 5411,
    "Errors?": null,
    "Is Fraud?": "No"
  }
}
```

> **Note on S3 Gzip Files:**  
> When Kafka S3 Sink writes records to S3, each line in the `.json.gz` file contains Kafka metadata wrapping the event:
> ```json
> {
>   "key": "2042",
>   "offset": 105234,
>   "timestamp": 1793280000000,
>   "value": { /* The Event Envelope shown above */ }
> }
> ```
> *(In some environments, `value` may be serialized either directly as an embedded JSON object or as an escaped JSON string).*

---

## 3. Detailed Data Dictionary

### 3.1 Envelope Attributes (Outer Layer)

| Field Name | Data Type | Nullable | Example Value | Description & Business Rules |
| :--- | :--- | :---: | :--- | :--- |
| `event_id` | `STRING (UUIDv4)` | **No** | `"edc89ac2-125b-47ef-9164-57290ca0e051"` | Primary unique identifier for this event. |
| `event_type` | `STRING` | **No** | `"transaction.created"` | Type of streaming event. |
| `schema_version` | `INTEGER` / `STRING` | **No** | `1` or `"1.0.0"` | Schema version for evolutionary contract checking. |
| `event_timestamp` | `STRING (ISO 8601)` | **No** | `"2026-10-29T14:35:10.120000+00:00"` | UTC creation timestamp of the event. |
| `source_system` | `STRING` | **No** | `"POS_TERMINAL"` | Source system: `POS_TERMINAL`, `ATM_NETWORK`, `ONLINE_PORTAL`, `MOBILE_SDK`. |
| `run_id` | `STRING` | **No** | `"run-001"` | Simulation batch or ingestion pipeline run ID. |
| `scenario` | `STRING` | Yes | `"normal_spend"` | Generator scenario (e.g. `normal_spend`, `travel_speed`, `high_value`, `declined_error`). |
| `payload` | `STRUCT / OBJECT` | **No** | `{...}` | Business transaction payload (see table 3.2). |

---

### 3.2 Payload Attributes (Core Financial Transaction)

| Payload Key | Raw Type | Target Silver Type | Nullable | Example | Transformation Notes & Validation Rules |
| :--- | :--- | :--- | :---: | :--- | :--- |
| `User` | `INTEGER` | `BIGINT` | **No** | `2042` | Natural user identifier. Foreign key to dimension table `users.user_id`. |
| `Card` | `INTEGER` | `BIGINT` | **No** | `0` | Card sequence index belonging to this user (0, 1, 2...). |
| `Year` | `INTEGER` | `INTEGER` | **No** | `2026` | Transaction business year (local merchant calendar). |
| `Month` | `INTEGER` | `INTEGER` | **No** | `10` | Transaction business month (`1` to `12`). |
| `Day` | `INTEGER` | `INTEGER` | **No** | `29` | Transaction business day (`1` to `31`). |
| `Time` | `STRING` | `STRING` | **No** | `"14:35"` | Local transaction time in `HH:MM` 24-hour format. |
| `Amount` | `STRING` | `DECIMAL(18,2)` | **No** | `"$45.20"` | **Must be cleaned!** Strip leading `$` and `,`. Negative amounts (e.g. `-$15.00`) represent refunds and are valid. |
| `Use Chip` | `STRING` | `STRING` | **No** | `"Chip Transaction"` | High-level channel: `"Swipe Transaction"`, `"Chip Transaction"`, or `"Online Transaction"`. |
| `Transaction Entry Method` | `STRING` | `STRING` | Yes | `"contactless_chip"` | Granular physical/digital entry mechanism (e.g. `contact_chip`, `contactless_chip`, `ecommerce`, `magnetic_stripe_swipe`). |
| `Merchant Name` | `INTEGER` / `STRING` | `STRING` | **No** | `12847192` | Merchant account ID or business name. |
| `Merchant City` | `STRING` | `STRING` | **No** | `"Cleveland"` | City of transaction. For online purchases, this will be `"ONLINE"`. |
| `Merchant State` | `STRING` | `STRING` | Yes | `"OH"` | 2-letter US state code. May be `null` when `Merchant City == "ONLINE"`. |
| `Merchant Latitude` | `FLOAT` | `DOUBLE` | Yes | `41.4993` | Latitude of city centroid (not store rooftop). Null for online transactions. Range: `[-90.0, 90.0]`. |
| `Merchant Longitude` | `FLOAT` | `DOUBLE` | Yes | `-81.6944` | Longitude of city centroid. Null for online transactions. Range: `[-180.0, 180.0]`. |
| `Zip` | `STRING` / `FLOAT` | `STRING` | Yes | `"44101"` | 5-digit postal code. Strip any trailing `.0` if cast from numeric. |
| `MCC` | `INTEGER` | `INTEGER` | **No** | `5411` | 4-digit Merchant Category Code (e.g. 5411 = Grocery Stores). |
| `Errors?` | `STRING` | `STRING` | Yes | `null` | Terminal error message if declined (e.g. `"Bad PIN"`, `"Insufficient Balance"`). |
| `Is Fraud?` | `STRING` | `BOOLEAN` | **No** | `"No"` | Ground truth label (`"Yes"` or `"No"`). **Must be isolated to prevent model target leakage.** |

---

## 4. Key Transformation Rules for the Data Engineer

### 4.1 Amount Parsing & Monetary Integrity
The incoming `Amount` is a string formatted as currency (e.g. `"$45.20"` or `"-$12.50"`).
- **Rule:** Never cast directly to `FLOAT` due to binary floating-point rounding issues in accounting.
- **Implementation:**
  ```python
  # PySpark SQL / Expression
  clean_amount = F.expr("try_cast(regexp_replace(trim(payload.Amount), '[$,]', '') as DECIMAL(18,2))")
  ```

### 4.2 Timestamps & Local Time Harmonization
The incoming payload splits the merchant's local timestamp across 4 separate fields (`Year`, `Month`, `Day`, `Time`), while the envelope provides `event_timestamp` in UTC ISO 8601.
- **`event_time_utc`**: Direct cast from `event_timestamp` via `to_timestamp()`.
- **`source_event_time_local`**: Reconstitute local calendar timestamp:
  ```python
  local_ts_expr = """
    try_cast(
      concat(
        lpad(trim(payload.Year), 4, '0'), '-',
        lpad(trim(payload.Month), 2, '0'), '-',
        lpad(trim(payload.Day), 2, '0'), ' ',
        trim(payload.Time), ':00'
      ) as TIMESTAMP_NTZ
    )
  """
  ```

### 4.3 Composite Scoped Keys
A card index (`Card`) is relative to a user (User 1 has Card 0, User 2 has Card 0).
- **Rule:** To avoid joining errors with reference tables, create a scoped card key:
  ```python
  # Examples: "2042:0" or "IBM_CARD_2042_0"
  scoped_card_key = F.concat_ws(":", F.col("payload.User"), F.col("payload.Card"))
  ```

### 4.4 Coordinate Integrity Constraint
- `Merchant Latitude` and `Merchant Longitude` must be **co-present**: both populated or both `null`.
- If one coordinate is populated and the other is `null`, or if coordinates exceed valid ranges (`[-90, 90]`, `[-180, 180]`), flag the record.
- **Never impute missing coordinates with `(0.0, 0.0)`** as that represents Null Island in the Gulf of Guinea.

### 4.5 Target Leakage Prevention (Is Fraud Separation)
- In the Silver layer, the raw business transaction is separated from the simulated ground truth label (`Is Fraud?`).
- `Is Fraud?` must be directed to `streaming_synthetic_truth_v2` linked by `event_id`.
- The analytical transaction table `streaming_transactions_v2` should NOT expose ground truth fraud flags to downstream feature stores.

---

## 5. Target Silver Delta Lake Schemas

### 5.1 Main Analytical Transactions Table (`streaming_transactions_v2`)

```sql
CREATE TABLE IF NOT EXISTS workspace.sic8_streaming.streaming_transactions_v2 (
    event_id STRING NOT NULL,
    schema_version STRING,
    user_id BIGINT NOT NULL,
    card_index BIGINT NOT NULL,
    scoped_card_key STRING NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    currency STRING DEFAULT 'USD',
    merchant_name STRING,
    merchant_city STRING,
    merchant_state STRING,
    merchant_latitude DOUBLE,
    merchant_longitude DOUBLE,
    zip STRING,
    mcc INT,
    use_chip STRING,
    transaction_entry_method STRING,
    errors STRING,
    source_system STRING,
    scenario STRING,
    event_time_utc TIMESTAMP,
    source_event_time_local TIMESTAMP_NTZ,
    event_payload_hash STRING,
    source_file STRING,
    kafka_key STRING,
    kafka_offset BIGINT,
    ingested_at_utc TIMESTAMP
) USING DELTA
PARTITIONED BY (DATE(event_time_utc));
```

### 5.2 Ground Truth Table (`streaming_synthetic_truth_v2`)

```sql
CREATE TABLE IF NOT EXISTS workspace.sic8_streaming.streaming_synthetic_truth_v2 (
    event_id STRING NOT NULL,
    synthetic_is_fraud BOOLEAN NOT NULL,
    scenario STRING,
    event_payload_hash STRING,
    source_file STRING,
    created_at_utc TIMESTAMP
) USING DELTA;
```

### 5.3 Quarantine / DLQ Table (`streaming_quarantine_v2`)

```sql
CREATE TABLE IF NOT EXISTS workspace.sic8_streaming.streaming_quarantine_v2 (
    quarantine_id STRING NOT NULL,
    quarantine_reason STRING NOT NULL,
    raw_json_line STRING,
    source_file STRING,
    failed_at_utc TIMESTAMP
) USING DELTA;
```

---

## 6. Quarantine Gate & Routing Criteria

Any record failing one of the following criteria MUST NOT be inserted into `streaming_transactions_v2`. It should be routed to `streaming_quarantine_v2`:

| Error Code | Detection Condition |
| :--- | :--- |
| `UNPARSEABLE_ENVELOPE` | The raw text line cannot be parsed as valid JSON. |
| `MISSING_EVENT_ID` | `event_id` is null or empty. |
| `MISSING_CARD_IDENTITY`| `payload.User` is null OR `payload.Card` is null. |
| `BAD_AMOUNT` | `Amount` is null or contains non-numeric characters after removing `$` and `,`. |
| `BAD_EVENT_TIME` | `event_timestamp` cannot be parsed into an ISO 8601 UTC timestamp. |
| `INVALID_COORDINATES` | Latitude or longitude is out of range, or only one coordinate is present. |
| `DUPLICATE_PAYLOAD_HASH`| An identical `event_id` already exists with a conflicting `event_payload_hash`. |

---

## 7. Reference PySpark Transformation Template

Below is a reference implementation of the microbatch processor for PySpark / Databricks Structured Streaming:

```python
from pyspark.sql import functions as F, types as T

def process_silver_microbatch(microbatch_df, batch_id):
    """Parses raw Kafka/S3 JSON envelopes, validates schema, and routes to Silver & Quarantine."""
    
    # 1. Parse outer JSON line or inner Kafka value
    parsed = (microbatch_df
        .withColumn("inner_json", F.coalesce(
            F.get_json_object("raw_json_line", "$.value"),
            F.col("raw_json_line")
        ))
        .withColumn("kafka_offset", F.expr("try_cast(get_json_object(raw_json_line, '$.offset') as BIGINT)"))
        .withColumn("kafka_key", F.get_json_object("raw_json_line", "$.key"))
    )

    # 2. Extract Envelope Fields
    envelope_df = (parsed
        .withColumn("event_id", F.get_json_object("inner_json", "$.event_id"))
        .withColumn("event_type", F.get_json_object("inner_json", "$.event_type"))
        .withColumn("schema_version", F.get_json_object("inner_json", "$.schema_version"))
        .withColumn("source_system", F.get_json_object("inner_json", "$.source_system"))
        .withColumn("scenario", F.get_json_object("inner_json", "$.scenario"))
        .withColumn("event_time_utc", F.to_timestamp(F.get_json_object("inner_json", "$.event_timestamp")))
        .withColumn("event_payload_hash", F.sha2(F.col("inner_json"), 256))
        .withColumn("quarantine_id", F.sha2(F.concat_ws("|", F.col("source_file"), F.col("raw_json_line")), 256))
    )

    # 3. Extract Payload Fields
    p = envelope_df.withColumn("p", F.col("inner_json"))  # Or use from_json with exact StructType
    tx_df = (p
        .withColumn("user_id", F.expr("try_cast(get_json_object(p, '$.payload.User') as BIGINT)"))
        .withColumn("card_index", F.expr("try_cast(get_json_object(p, '$.payload.Card') as BIGINT)"))
        .withColumn("amount", F.expr("try_cast(regexp_replace(trim(get_json_object(p, '$.payload.Amount')), '[$,]', '') as DECIMAL(18,2))"))
        .withColumn("use_chip", F.get_json_object(p, "$.payload['Use Chip']"))
        .withColumn("entry_method", F.get_json_object(p, "$.payload['Transaction Entry Method']"))
        .withColumn("merchant_name", F.get_json_object(p, "$.payload['Merchant Name']"))
        .withColumn("merchant_city", F.get_json_object(p, "$.payload['Merchant City']"))
        .withColumn("merchant_state", F.get_json_object(p, "$.payload['Merchant State']"))
        .withColumn("merchant_lat", F.expr("try_cast(get_json_object(p, '$.payload[\"Merchant Latitude\"]') as DOUBLE)"))
        .withColumn("merchant_lon", F.expr("try_cast(get_json_object(p, '$.payload[\"Merchant Longitude\"]') as DOUBLE)"))
        .withColumn("zip", F.regexp_replace(F.trim(F.get_json_object(p, "$.payload.Zip")), r"\.0$", ""))
        .withColumn("mcc", F.expr("try_cast(get_json_object(p, '$.payload.MCC') as INT)"))
        .withColumn("errors", F.get_json_object(p, "$.payload['Errors?']"))
        .withColumn("raw_is_fraud", F.trim(F.get_json_object(p, "$.payload['Is Fraud?']")))
        .withColumn("scoped_card_key", F.concat_ws(":", F.col("user_id"), F.col("card_index")))
        .withColumn("source_event_time_local", F.expr("""
            try_cast(concat(
                lpad(trim(get_json_object(p, '$.payload.Year')), 4, '0'), '-',
                lpad(trim(get_json_object(p, '$.payload.Month')), 2, '0'), '-',
                lpad(trim(get_json_object(p, '$.payload.Day')), 2, '0'), ' ',
                trim(get_json_object(p, '$.payload.Time')), ':00'
            ) as TIMESTAMP_NTZ)
        """))
    )

    # 4. Quarantine Evaluation
    reason = (F.when(F.col("event_id").isNull() | (F.length(F.trim("event_id")) == 0), "MISSING_EVENT_ID")
      .when(F.col("user_id").isNull() | F.col("card_index").isNull(), "MISSING_CARD_IDENTITY")
      .when(F.col("amount").isNull(), "BAD_AMOUNT")
      .when(F.col("event_time_utc").isNull(), "BAD_EVENT_TIME")
      .when((F.col("merchant_lat").isNotNull() & F.col("merchant_lon").isNull()) |
            (F.col("merchant_lat").isNull() & F.col("merchant_lon").isNotNull()), "INVALID_COORDINATES")
      .otherwise(F.lit(None).cast("string")))

    annotated = tx_df.withColumn("quarantine_reason", reason)

    # 5. Split Streams
    valid_tx = annotated.filter(F.col("quarantine_reason").isNull())
    quarantine_records = annotated.filter(F.col("quarantine_reason").isNotNull())

    # Write idempotently via Delta Merge or Append (depending on idempotency config)
    # ...
```
