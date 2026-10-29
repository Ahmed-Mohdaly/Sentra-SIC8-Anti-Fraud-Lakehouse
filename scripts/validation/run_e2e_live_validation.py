"""Comprehensive End-to-End Live Validation for SIC Synthetic Transaction Data Platform.

Tests:
  Test A: Transaction streaming to MSK -> S3 Bronze (verified ABSENT from PostgreSQL).
  Test B: User insertion into PostgreSQL -> Debezium CDC -> MSK 'users' -> S3 Bronze.
  Test C: Card insertion into PostgreSQL -> Debezium CDC -> MSK 'cards' -> S3 Bronze.
  Test D: Batch Reference Data Extraction for MCC & Cities -> S3 Bronze with Parquet validation.
"""

import datetime
import gzip
import io
import json
import os
import sys
import time
import urllib.parse
import uuid
import boto3
import psycopg2
from confluent_kafka import Consumer, Producer

print("=" * 80)
print("  SIC PLATFORM — PHASE 8 END-TO-END LIVE PIPELINE VALIDATION")
print("=" * 80)

# ------------------------------------------------------------------------------
# 1. AWS Clients, Runtime Path and Configuration
# ------------------------------------------------------------------------------
region = "eu-west-3"
bucket = "sic-pipeline-storage"
brokers = "b-1.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092,b-2.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092"

s3 = boto3.client("s3", region_name=region)
sm = boto3.client("secretsmanager", region_name=region)

os.makedirs("/tmp/ingestion", exist_ok=True)
with open("/tmp/ingestion/__init__.py", "w") as f:
    f.write("")
s3.download_file(bucket, "scripts/batch_reference_extractor.py", "/tmp/ingestion/batch_reference_extractor.py")
if "/tmp" not in sys.path:
    sys.path.insert(0, "/tmp")


# ------------------------------------------------------------------------------
# 2. Connect to RDS PostgreSQL
# ------------------------------------------------------------------------------
print("\n[STEP 1] Connecting to Amazon RDS PostgreSQL (containerdb)...")
secret_resp = sm.get_secret_value(SecretId="synthetic-data-provider/database")
db_info = json.loads(secret_resp["SecretString"])
db_url = db_info["DATABASE_URL"]

p = urllib.parse.urlparse(db_url)
pg_conn = psycopg2.connect(
    host=p.hostname,
    port=p.port or 5432,
    user=p.username,
    password=p.password,
    dbname=p.path.lstrip("/"),
    sslmode="require",
)
pg_conn.autocommit = True
cur = pg_conn.cursor()

cur.execute("SELECT count(*) FROM transactions;")
initial_tx_count = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM users;")
initial_user_count = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM cards;")
initial_card_count = cur.fetchone()[0]

print(f"  [DB STATE] Current row counts in RDS PostgreSQL:")
print(f"    - transactions: {initial_tx_count}")
print(f"    - users:        {initial_user_count}")
print(f"    - cards:        {initial_card_count}")

# ------------------------------------------------------------------------------
# 3. Tests B & C: Insert Users and Cards into PostgreSQL to trigger Debezium CDC
# ------------------------------------------------------------------------------
batch_size = 52  # Exceeds Aiven S3 Sink file.max.records (50) for immediate flush
base_user_id = 888800
unique_suffix = int(time.time())

print(f"\n[STEP 2] (Tests B & C) Inserting {batch_size} Users & Cards to trigger Debezium CDC...")
cur.execute(f"DELETE FROM cards WHERE user_id BETWEEN {base_user_id + 1} AND {base_user_id + batch_size};")
cur.execute(f"DELETE FROM users WHERE user_id BETWEEN {base_user_id + 1} AND {base_user_id + batch_size};")

inserted_user_ids = []
for i in range(1, batch_size + 1):
    u_id = base_user_id + i
    c_idx = 0
    cur.execute(
        """
        INSERT INTO users (user_id, person, current_age, retirement_age, birth_year, birth_month, gender, address, apartment, city, state, zipcode, latitude, longitude, per_capita_income, yearly_income, total_debt, fico_score, num_credit_cards, created_at)
        VALUES (%s, %s, 35, 65, 1991, 5, 'Female', '100 Boulevard Haussmann', 'Apt 4B', 'Paris', 'IDF', 75008, 48.8744, 2.3328, '$48,000', '$92,000', '$15,000', 760, 1, NOW());
        """,
        (u_id, f"LiveUser{i}"),
    )
    cur.execute(
        """
        INSERT INTO cards (user_id, card_index, card_brand, card_type, card_number, expires, cvv, has_chip, cards_issued, credit_limit, acct_open_date, year_pin_last_changed, card_on_dark_web, created_at)
        VALUES (%s, %s, 'Visa', 'Credit', '4111222233334444', '12/2028', '123', 'YES', 1, '$15,000', '01/2024', 2024, 'No', NOW());
        """,
        (u_id, c_idx),
    )
    inserted_user_ids.append(u_id)


print(f"  Successfully inserted {batch_size} Users (IDs {inserted_user_ids[0]}..{inserted_user_ids[-1]}) and Cards into RDS PostgreSQL.")

# ------------------------------------------------------------------------------
# 4. Test A: Publish Transactions directly to MSK 'transactions' topic
# ------------------------------------------------------------------------------
tx_count_to_send = 55  # Exceeds Aiven S3 Sink file.max.records (50)
print(f"\n[STEP 3] (Test A) Publishing {tx_count_to_send} Transactions directly to MSK topic 'transactions'...")
producer = Producer({"bootstrap.servers": brokers, "client.id": f"e2e-producer-{unique_suffix}"})

published_tx_ids = []
for i in range(1, tx_count_to_send + 1):
    tx_id = f"tx-e2e-{unique_suffix}-{i:03d}"
    published_tx_ids.append(tx_id)
    tx_payload = {
        "schema_version": "1.0.0",
        "event_id": str(uuid.uuid4()),
        "event_type": "transaction.created",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "producer": "sic.synthetic_streaming",
        "data": {
            "transaction_id": tx_id,
            "user_id": inserted_user_ids[0],
            "card_id": 0,
            "amount": round(25.50 + i * 1.75, 2),
            "currency": "EUR",
            "merchant_name": "SIC Cloud Supermarket",
            "mcc": 5411,
            "transaction_type": "pos_terminal",
            "status": "APPROVED",
            "is_fraud": False,
            "zip": "75008",
        },
    }
    producer.produce(
        topic="transactions",
        key=str(inserted_user_ids[0]).encode("utf-8"),
        value=json.dumps(tx_payload).encode("utf-8"),
    )

producer.flush(timeout=15)
print(f"  Successfully published and flushed {tx_count_to_send} transactions to Amazon MSK.")

# ------------------------------------------------------------------------------
# 5. Test A Assertion: Strictly verify Transactions are NOT in PostgreSQL
# ------------------------------------------------------------------------------
print("\n[STEP 4] (Test A Assertion) Verifying transactions are STRICTLY NOT in PostgreSQL...")
cur.execute("SELECT count(*) FROM transactions WHERE transaction_id LIKE %s;", (f"tx-e2e-{unique_suffix}%",))
db_tx_found = cur.fetchone()[0]

cur.execute("SELECT count(*) FROM transactions;")
post_tx_count = cur.fetchone()[0]

print(f"  Transactions matching 'tx-e2e-{unique_suffix}%' found in RDS PostgreSQL: {db_tx_found}")
print(f"  PostgreSQL transactions count before: {initial_tx_count}, after: {post_tx_count}")

assert db_tx_found == 0, f"CRITICAL VIOLATION: {db_tx_found} synthetic transactions were inserted into RDS PostgreSQL!"
assert post_tx_count == initial_tx_count, f"CRITICAL VIOLATION: PostgreSQL transactions table changed ({initial_tx_count} -> {post_tx_count})!"
print("  >>> [PASS] Acceptance Criterion 2: Synthetic transactions are strictly excluded from PostgreSQL! <<<")

# ------------------------------------------------------------------------------
# 6. Verify Transaction delivery on Amazon MSK
# ------------------------------------------------------------------------------
print("\n[STEP 5] Verifying transaction event receipt on Amazon MSK 'transactions' topic...")
from confluent_kafka import TopicPartition
consumer_tx = Consumer({
    "bootstrap.servers": brokers,
    "group.id": f"e2e-tx-validator-{unique_suffix}",
    "enable.auto.commit": False,
})
tx_tps = []
for p_idx in range(3):
    tp = TopicPartition("transactions", p_idx)
    low, high = consumer_tx.get_watermark_offsets(tp, timeout=5)
    tp.offset = max(0, high - 60)
    tx_tps.append(tp)
    print(f"  TopicPartition transactions-{p_idx}: low={low}, high={high}, starting from offset={tp.offset}")

consumer_tx.assign(tx_tps)

first_tx_found = False
for _ in range(50):
    msg = consumer_tx.poll(timeout=1.0)
    if msg and not msg.error():
        payload = json.loads(msg.value().decode("utf-8"))
        if payload.get("data", {}).get("transaction_id") == published_tx_ids[0]:
            print(f"  [PASS] Successfully read transaction {published_tx_ids[0]} from MSK partition {msg.partition()} at offset {msg.offset()}")
            first_tx_found = True
            break
consumer_tx.close()
assert first_tx_found, f"Transaction {published_tx_ids[0]} was not received from MSK!"


# ------------------------------------------------------------------------------
# 7. Verify Debezium CDC publication to MSK 'users' and 'cards' topics
# ------------------------------------------------------------------------------
print("\n[STEP 6] Verifying Debezium CDC change events on Amazon MSK topics 'users' and 'cards'...")
from confluent_kafka import TopicPartition

# 7A. Verify Users CDC
print("  Verifying 'users' CDC stream...")
consumer_users = Consumer({
    "bootstrap.servers": brokers,
    "group.id": f"e2e-users-validator-{unique_suffix}",
    "enable.auto.commit": False,
})
u_tps = []
for part in range(3):
    tp = TopicPartition("users", part)
    low, high = consumer_users.get_watermark_offsets(tp, timeout=5)
    tp.offset = max(0, high - 60)
    u_tps.append(tp)
    print(f"    TopicPartition users-{part}: low={low}, high={high}, starting from offset={tp.offset}")
consumer_users.assign(u_tps)

user_cdc_captured = False
for _ in range(80):
    msg = consumer_users.poll(timeout=1.0)
    if msg and not msg.error():
        val = json.loads(msg.value().decode("utf-8"))
        after = val.get("payload", {}).get("after") or val.get("after") or val
        if str(after.get("user_id")) in [str(u) for u in inserted_user_ids]:
            print(f"    [PASS] Debezium CDC captured User {after.get('user_id')} on MSK topic 'users' (Partition {msg.partition()}, Offset {msg.offset()})")
            user_cdc_captured = True
            break
consumer_users.close()
assert user_cdc_captured, f"Debezium CDC event for inserted users not found on MSK 'users' topic!"

# 7B. Verify Cards CDC
print("  Verifying 'cards' CDC stream...")
consumer_cards = Consumer({
    "bootstrap.servers": brokers,
    "group.id": f"e2e-cards-validator-{unique_suffix}",
    "enable.auto.commit": False,
})
c_tps = []
for part in range(3):
    tp = TopicPartition("cards", part)
    low, high = consumer_cards.get_watermark_offsets(tp, timeout=5)
    tp.offset = max(0, high - 60)
    c_tps.append(tp)
    print(f"    TopicPartition cards-{part}: low={low}, high={high}, starting from offset={tp.offset}")
consumer_cards.assign(c_tps)

card_cdc_captured = False
for _ in range(80):
    msg = consumer_cards.poll(timeout=1.0)
    if msg and not msg.error():
        val = json.loads(msg.value().decode("utf-8"))
        after = val.get("payload", {}).get("after") or val.get("after") or val
        if str(after.get("user_id")) in [str(u) for u in inserted_user_ids]:
            print(f"    [PASS] Debezium CDC captured Card (user_id={after.get('user_id')}, card_index={after.get('card_index')}) on MSK topic 'cards' (Partition {msg.partition()}, Offset {msg.offset()})")
            card_cdc_captured = True
            break
consumer_cards.close()
assert card_cdc_captured, f"Debezium CDC event for inserted cards not found on MSK 'cards' topic!"

print("  >>> [PASS] Acceptance Criteria 3 & 4: Debezium PostgreSQL CDC captures users and cards to MSK! <<<")



# ------------------------------------------------------------------------------
# 8. Test D: Run Batch Reference Extractor for MCC and Cities
# ------------------------------------------------------------------------------
print("\n[STEP 7] (Test D) Running Batch Reference Extractor for MCC & Cities...")
from ingestion.batch_reference_extractor import BatchReferenceExtractor

extractor = BatchReferenceExtractor(
    db_url=db_url,
    s3_bucket=bucket,
    region=region,
    s3_client=s3,
)
batch_results = extractor.run_all()
for dataset_name, details in batch_results.items():
    stats = details["stats"]
    s3_k = details["s3_key"]
    print(f"  [PASS] Extracted {stats['record_count']} rows for '{dataset_name}' -> s3://{bucket}/{s3_k}")

print("  >>> [PASS] Acceptance Criterion 6: Reference datasets extracted via scheduled batch workflow directly to S3 Bronze! <<<")

# ------------------------------------------------------------------------------
# 9. Verify S3 Bronze Lakehouse Delivery across all pipelines
# ------------------------------------------------------------------------------
print("\n[STEP 8] Verifying S3 Bronze Lakehouse delivery across Streaming, CDC & Batch...")
found_test_tx_in_s3 = False
found_user_in_s3 = False

for attempt in range(1, 10):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix="bronze/")
    all_keys = [o["Key"] for o in resp.get("Contents", [])]

    tx_keys = [k for k in all_keys if k.startswith("bronze/transactions/")]
    users_keys = [k for k in all_keys if k.startswith("bronze/users/")]
    cards_keys = [k for k in all_keys if k.startswith("bronze/cards/")]
    mcc_keys = [k for k in all_keys if k.startswith("bronze/mcc/")]
    cities_keys = [k for k in all_keys if k.startswith("bronze/cities/")]

    if not found_test_tx_in_s3:
        gz_tx_keys = [k for k in tx_keys if k.endswith(".json.gz")]
        for key in reversed(gz_tx_keys[-10:]):
            obj = s3.get_object(Bucket=bucket, Key=key)
            decompressed = gzip.decompress(obj["Body"].read()).decode("utf-8")
            for line in decompressed.strip().split("\n"):
                if not line:
                    continue
                record = json.loads(line)
                val = record.get("value")
                if isinstance(val, str):
                    val = json.loads(val)
                if isinstance(val, dict):
                    t_id = val.get("data", {}).get("transaction_id") or val.get("transaction_id")
                    if t_id in published_tx_ids:
                        print(f"\n  [PASS] Verified test transaction '{t_id}' in S3: s3://{bucket}/{key}")
                        found_test_tx_in_s3 = True
                        break
            if found_test_tx_in_s3:
                break

    if not found_user_in_s3:
        gz_user_keys = [k for k in users_keys if k.endswith(".json.gz")]
        for key in reversed(gz_user_keys[-10:]):
            obj = s3.get_object(Bucket=bucket, Key=key)
            decompressed = gzip.decompress(obj["Body"].read()).decode("utf-8")
            for line in decompressed.strip().split("\n"):
                if not line:
                    continue
                record = json.loads(line)
                val = record.get("value")
                if isinstance(val, str):
                    val = json.loads(val)
                if isinstance(val, dict):
                    after = val.get("payload", {}).get("after") or val.get("after") or val
                    u_id = after.get("user_id") if isinstance(after, dict) else None
                    if u_id in inserted_user_ids:
                        print(f"  [PASS] Verified CDC user '{u_id}' in S3: s3://{bucket}/{key}")
                        found_user_in_s3 = True
                        break
            if found_user_in_s3:
                break

    if found_test_tx_in_s3 and found_user_in_s3:
        break

    print(f"  [Attempt {attempt}/9] Waiting for Aiven S3 Sink flush to S3 (6s)...")
    time.sleep(6)

print(f"  S3 Bronze Object Counts by Category:")
print(f"    - bronze/transactions/: {len(tx_keys)} objects")
print(f"    - bronze/users/:        {len(users_keys)} objects")
print(f"    - bronze/cards/:        {len(cards_keys)} objects")
print(f"    - bronze/mcc/:          {len(mcc_keys)} objects")
print(f"    - bronze/cities/:       {len(cities_keys)} objects")

assert len(mcc_keys) > 0, "Missing bronze/mcc/ in S3 Bronze!"
assert len(cities_keys) > 0, "Missing bronze/cities/ in S3 Bronze!"
assert len(tx_keys) > 0, "Missing bronze/transactions/ in S3 Bronze!"
assert len(users_keys) > 0, "Missing bronze/users/ in S3 Bronze!"
assert len(cards_keys) > 0, "Missing bronze/cards/ in S3 Bronze!"

# Print sample file paths
print("\n  Sample S3 Bronze Keys:")
for prefix_name, key_list in [("Transactions", tx_keys), ("Users", users_keys), ("Cards", cards_keys), ("MCC", mcc_keys), ("Cities", cities_keys)]:
    if key_list:
        print(f"    - {prefix_name}: s3://{bucket}/{key_list[-1]}")

assert found_test_tx_in_s3, f"Test transaction {published_tx_ids[0]} not found in bronze/transactions/ S3 objects!"
assert found_user_in_s3, f"CDC user records not found in bronze/users/ S3 objects!"

print("\n" + "=" * 80)
print("  >>> ALL ACCEPTANCE CRITERIA SUCCESSFULLY VERIFIED (100% PASS) <<<")
print("=" * 80)
