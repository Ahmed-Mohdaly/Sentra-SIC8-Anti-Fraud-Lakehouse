#!/usr/bin/env python3
"""
Production Lakehouse Transformation: Bronze -> Silver -> Gold
Executes directly against Amazon S3 (sic-pipeline-storage) using PyArrow and Boto3.
Generates conforming Silver Delta/Parquet tables and Gold Analytical/ML Feature Store tables.
"""

from __future__ import annotations

import io
import json
import gzip
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# S3 Configuration
S3_BUCKET = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3")

s3 = boto3.client("s3", region_name=AWS_REGION)


def haversine_distance_km(lat1: Optional[float], lon1: Optional[float], lat2: Optional[float], lon2: Optional[float]) -> Optional[float]:
    """Calculate Great Circle distance in kilometers between two geo coordinates."""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    try:
        r = 6371.0  # Earth's mean radius in km
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * (math.sin(delta_lambda / 2.0) ** 2)
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return round(r * c, 2)
    except Exception:
        return None


def clean_currency_to_float(val: Any) -> float:
    """Strip currency symbols, commas, and whitespace and convert to float."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


print("=" * 80)
print(f"SIC LAKEHOUSE PIPELINE: BRONZE -> SILVER -> GOLD")
print(f"Target S3 Bucket: s3://{S3_BUCKET}/")
print(f"Region: {AWS_REGION}")
print("=" * 80)

# ==============================================================================
# STEP 1: Ingest & Conforming Reference Tables (MCC & Cities)
# ==============================================================================
print("\n[STEP 1/5] Ingesting Reference Dimensions (MCC & Cities)...")

# 1.1 Ingest Cities
city_lookup: Dict[str, Tuple[float, float]] = {}  # city_state_lower -> (lat, lon)
cities_res = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="bronze/cities/")
cities_keys = [o["Key"] for o in cities_res.get("Contents", []) if o["Key"].endswith(".parquet")]

if cities_keys:
    latest_city_key = sorted(cities_keys)[-1]
    print(f"  Reading latest cities snapshot: {latest_city_key}")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=latest_city_key)
    cities_table = pq.read_table(io.BytesIO(obj["Body"].read()))
    
    # Store in Silver
    silver_cities_buf = io.BytesIO()
    pq.write_table(cities_table, silver_cities_buf, compression="snappy")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="silver/cities/cities_conformed.parquet",
        Body=silver_cities_buf.getvalue(),
    )
    print(f"  [SILVER] Written {len(cities_table)} cities -> s3://{S3_BUCKET}/silver/cities/cities_conformed.parquet")

    # Populate cache
    c_names = cities_table["city_name"].to_pylist()
    c_states = cities_table["state_code"].to_pylist()
    c_lats = cities_table["latitude"].to_pylist()
    c_lons = cities_table["longitude"].to_pylist()
    for name, st, lat, lon in zip(c_names, c_states, c_lats, c_lons):
        if name and st and lat is not None and lon is not None:
            city_lookup[f"{name.strip().lower()}_{st.strip().lower()}"] = (float(lat), float(lon))
            city_lookup[name.strip().lower()] = (float(lat), float(lon))

# 1.2 Ingest MCC
mcc_lookup: Dict[int, Dict[str, str]] = {}
mcc_res = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="bronze/mcc/")
mcc_keys = [o["Key"] for o in mcc_res.get("Contents", []) if o["Key"].endswith(".parquet")]

if mcc_keys:
    latest_mcc_key = sorted(mcc_keys)[-1]
    print(f"  Reading latest MCC snapshot: {latest_mcc_key}")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=latest_mcc_key)
    mcc_table = pq.read_table(io.BytesIO(obj["Body"].read()))

    # Store in Silver
    silver_mcc_buf = io.BytesIO()
    pq.write_table(mcc_table, silver_mcc_buf, compression="snappy")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="silver/merchants/mcc_conformed.parquet",
        Body=silver_mcc_buf.getvalue(),
    )
    print(f"  [SILVER] Written {len(mcc_table)} MCC definitions -> s3://{S3_BUCKET}/silver/merchants/mcc_conformed.parquet")

    m_codes = mcc_table["mcc"].to_pylist()
    m_descs = mcc_table["description"].to_pylist()
    m_cats = mcc_table["category"].to_pylist()
    for code, desc, cat in zip(m_codes, m_descs, m_cats):
        if code is not None:
            mcc_lookup[int(code)] = {
                "description": desc or "Unknown",
                "category": cat or "General Retail",
            }


# ==============================================================================
# STEP 2: Conforming Users Dimension (Baseline CSV + CDC Stream Upsert)
# ==============================================================================
print("\n[STEP 2/5] Building Conformed Silver Users (Baseline + CDC Merge)...")

users_map: Dict[int, Dict[str, Any]] = {}

# 2.1 Read Baseline Users CSV
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="raw/baseline/sd254_users.csv")
    csv_text = obj["Body"].read().decode("utf-8")
    import csv
    reader = csv.DictReader(io.StringIO(csv_text))
    for i, row in enumerate(reader):
        u_id = int(row.get("User", i))
        lat = float(row["Latitude"]) if row.get("Latitude") else None
        lon = float(row["Longitude"]) if row.get("Longitude") else None
        users_map[u_id] = {
            "user_id": u_id,
            "person_name": row.get("Person", ""),
            "current_age": int(row["Current Age"]) if row.get("Current Age") else None,
            "gender": row.get("Gender", "Unknown"),
            "city": row.get("City", ""),
            "state": row.get("State", ""),
            "zipcode": row.get("Zipcode", ""),
            "latitude": lat,
            "longitude": lon,
            "yearly_income": clean_currency_to_float(row.get("Yearly Income - Person")),
            "total_debt": clean_currency_to_float(row.get("Total Debt")),
            "fico_score": int(row["FICO Score"]) if row.get("FICO Score") else None,
            "num_credit_cards": int(row["Num Credit Cards"]) if row.get("Num Credit Cards") else 1,
            "source": "baseline",
            "updated_at": "2026-10-25T00:00:00Z",
        }
    print(f"  Loaded {len(users_map)} baseline users from raw/baseline/sd254_users.csv")
except Exception as e:
    print(f"  Warning loading baseline users: {e}")

# 2.2 Replay CDC Users
cdc_users_res = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="bronze/users/")
cdc_user_keys = [o["Key"] for o in cdc_users_res.get("Contents", []) if o["Key"].endswith(".json.gz")]
cdc_user_updates = 0

for key in cdc_user_keys:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        decomp = gzip.decompress(obj["Body"].read()).decode("utf-8")
        for line in decomp.strip().split("\n"):
            if not line:
                continue
            rec = json.loads(line)
            val = rec.get("value", {})
            op = val.get("op")
            after = val.get("after")
            if op in ("c", "u", "r") and after:
                u_id = int(after.get("user_id", 0))
                lat = float(after["latitude"]) if after.get("latitude") is not None else None
                lon = float(after["longitude"]) if after.get("longitude") is not None else None
                users_map[u_id] = {
                    "user_id": u_id,
                    "person_name": after.get("person", f"User-{u_id}"),
                    "current_age": int(after.get("current_age", 30)),
                    "gender": after.get("gender", "Unknown"),
                    "city": after.get("city", ""),
                    "state": after.get("state", ""),
                    "zipcode": str(after.get("zipcode", "")),
                    "latitude": lat,
                    "longitude": lon,
                    "yearly_income": clean_currency_to_float(after.get("yearly_income")),
                    "total_debt": clean_currency_to_float(after.get("total_debt")),
                    "fico_score": int(after.get("fico_score", 700)),
                    "num_credit_cards": int(after.get("num_credit_cards", 1)),
                    "source": "cdc_stream",
                    "updated_at": after.get("created_at", "2026-10-26T00:00:00Z"),
                }
                cdc_user_updates += 1
            elif op == "d":
                before = val.get("before") or {}
                u_id = before.get("user_id")
                if u_id and int(u_id) in users_map:
                    users_map.pop(int(u_id), None)
    except Exception as e:
        continue

print(f"  Merged CDC user events ({cdc_user_updates} operations). Total active users: {len(users_map)}")

# Build Silver Users Arrow Table
silver_users_data = {
    "user_id": [u["user_id"] for u in users_map.values()],
    "person_name": [u["person_name"] for u in users_map.values()],
    "current_age": [u["current_age"] for u in users_map.values()],
    "gender": [u["gender"] for u in users_map.values()],
    "city": [u["city"] for u in users_map.values()],
    "state": [u["state"] for u in users_map.values()],
    "zipcode": [u["zipcode"] for u in users_map.values()],
    "latitude": [u["latitude"] for u in users_map.values()],
    "longitude": [u["longitude"] for u in users_map.values()],
    "yearly_income": [u["yearly_income"] for u in users_map.values()],
    "total_debt": [u["total_debt"] for u in users_map.values()],
    "fico_score": [u["fico_score"] for u in users_map.values()],
    "num_credit_cards": [u["num_credit_cards"] for u in users_map.values()],
    "source": [u["source"] for u in users_map.values()],
    "updated_at": [u["updated_at"] for u in users_map.values()],
}
silver_users_table = pa.Table.from_pydict(silver_users_data)
silver_users_buf = io.BytesIO()
pq.write_table(silver_users_table, silver_users_buf, compression="snappy")
s3.put_object(
    Bucket=S3_BUCKET,
    Key="silver/users/silver_users.parquet",
    Body=silver_users_buf.getvalue(),
)
print(f"  [SILVER] Written {len(silver_users_table)} records -> s3://{S3_BUCKET}/silver/users/silver_users.parquet")


# ==============================================================================
# STEP 3: Conforming Cards Dimension (Baseline CSV + CDC Stream Upsert)
# ==============================================================================
print("\n[STEP 3/5] Building Conformed Silver Cards (Baseline + CDC Merge)...")

cards_map: Dict[Tuple[int, int], Dict[str, Any]] = {}

# 3.1 Read Baseline Cards CSV
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="raw/baseline/sd254_cards.csv")
    csv_text = obj["Body"].read().decode("utf-8")
    import csv
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        u_id = int(row["User"])
        c_idx = int(row["CARD INDEX"])
        cards_map[(u_id, c_idx)] = {
            "user_id": u_id,
            "card_index": c_idx,
            "card_brand": row.get("Card Brand", "Visa"),
            "card_type": row.get("Card Type", "Credit"),
            "card_number_masked": f"****-****-****-{row.get('Card Number', '1234')[-4:]}",
            "expires": row.get("Expires", ""),
            "has_chip": row.get("Has Chip", "YES").upper() in ("YES", "TRUE", "1"),
            "cards_issued": int(row.get("Cards Issued", 1)) if row.get("Cards Issued") else 1,
            "credit_limit": clean_currency_to_float(row.get("Credit Limit")),
            "acct_open_date": row.get("Acct Open Date", ""),
            "card_on_dark_web": row.get("Card on Dark Web", "No").upper() == "YES",
            "source": "baseline",
        }
    print(f"  Loaded {len(cards_map)} baseline cards from raw/baseline/sd254_cards.csv")
except Exception as e:
    print(f"  Warning loading baseline cards: {e}")

# 3.2 Replay CDC Cards
cdc_cards_res = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="bronze/cards/")
cdc_card_keys = [o["Key"] for o in cdc_cards_res.get("Contents", []) if o["Key"].endswith(".json.gz")]
cdc_card_updates = 0

for key in cdc_card_keys:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        decomp = gzip.decompress(obj["Body"].read()).decode("utf-8")
        for line in decomp.strip().split("\n"):
            if not line:
                continue
            rec = json.loads(line)
            val = rec.get("value", {})
            op = val.get("op")
            after = val.get("after")
            if op in ("c", "u", "r") and after:
                u_id = int(after.get("user_id", 0))
                c_idx = int(after.get("card_index", 0))
                card_num = str(after.get("card_number", "4111"))
                cards_map[(u_id, c_idx)] = {
                    "user_id": u_id,
                    "card_index": c_idx,
                    "card_brand": after.get("card_brand", "Visa"),
                    "card_type": after.get("card_type", "Credit"),
                    "card_number_masked": f"****-****-****-{card_num[-4:]}",
                    "expires": after.get("expires", ""),
                    "has_chip": str(after.get("has_chip", "YES")).upper() in ("YES", "TRUE", "1"),
                    "cards_issued": int(after.get("cards_issued", 1)),
                    "credit_limit": clean_currency_to_float(after.get("credit_limit")),
                    "acct_open_date": after.get("acct_open_date", ""),
                    "card_on_dark_web": str(after.get("card_on_dark_web", "No")).upper() == "YES",
                    "source": "cdc_stream",
                }
                cdc_card_updates += 1
            elif op == "d":
                before = val.get("before") or {}
                u_id = before.get("user_id")
                c_idx = before.get("card_index")
                if u_id is not None and c_idx is not None:
                    cards_map.pop((int(u_id), int(c_idx)), None)
    except Exception:
        continue

print(f"  Merged CDC card events ({cdc_card_updates} operations). Total active cards: {len(cards_map)}")

silver_cards_data = {
    "user_id": [c["user_id"] for c in cards_map.values()],
    "card_index": [c["card_index"] for c in cards_map.values()],
    "card_brand": [c["card_brand"] for c in cards_map.values()],
    "card_type": [c["card_type"] for c in cards_map.values()],
    "card_number_masked": [c["card_number_masked"] for c in cards_map.values()],
    "expires": [c["expires"] for c in cards_map.values()],
    "has_chip": [c["has_chip"] for c in cards_map.values()],
    "cards_issued": [c["cards_issued"] for c in cards_map.values()],
    "credit_limit": [c["credit_limit"] for c in cards_map.values()],
    "acct_open_date": [c["acct_open_date"] for c in cards_map.values()],
    "card_on_dark_web": [c["card_on_dark_web"] for c in cards_map.values()],
    "source": [c["source"] for c in cards_map.values()],
}
silver_cards_table = pa.Table.from_pydict(silver_cards_data)
silver_cards_buf = io.BytesIO()
pq.write_table(silver_cards_table, silver_cards_buf, compression="snappy")
s3.put_object(
    Bucket=S3_BUCKET,
    Key="silver/cards/silver_cards.parquet",
    Body=silver_cards_buf.getvalue(),
)
print(f"  [SILVER] Written {len(silver_cards_table)} records -> s3://{S3_BUCKET}/silver/cards/silver_cards.parquet")


# ==============================================================================
# STEP 4: Conforming Transactions Fact (Bronze Batches + Bronze Stream)
# ==============================================================================
print("\n[STEP 4/5] Building Conformed Silver Transactions (Deduplicated & Enriched)...")

transactions_list: List[Dict[str, Any]] = []
seen_tx_ids = set()

# 4.1 Read Bronze Parquet Batches
tx_parquet_res = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="bronze/transactions/year=")
tx_parquet_keys = [o["Key"] for o in tx_parquet_res.get("Contents", []) if o["Key"].endswith(".parquet")]

print(f"  Processing {len(tx_parquet_keys)} Bronze Parquet batch files...")
for key in tx_parquet_keys:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        batch_tab = pq.read_table(io.BytesIO(obj["Body"].read()))
        pydict = batch_tab.to_pydict()
        num_rows = len(batch_tab)
        for i in range(num_rows):
            tx_id = pydict["transaction_id"][i]
            if tx_id in seen_tx_ids:
                continue
            seen_tx_ids.add(tx_id)

            u_id = pydict["user_id"][i]
            c_idx = pydict["card_index"][i]
            mcc_code = pydict["mcc"][i]
            m_lat = pydict["merchant_latitude"][i]
            m_lon = pydict["merchant_longitude"][i]
            m_city = pydict["merchant_city"][i] or ""
            m_state = pydict["merchant_state"][i] or ""

            # Geo distance from cardholder
            user_prof = users_map.get(u_id, {})
            u_lat = user_prof.get("latitude")
            u_lon = user_prof.get("longitude")
            dist_km = haversine_distance_km(u_lat, u_lon, m_lat, m_lon)

            # MCC Lookup
            mcc_meta = mcc_lookup.get(mcc_code, {"category": "Other", "description": "Other"})

            created_str = pydict["created_at"][i]
            try:
                dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            except Exception:
                dt = datetime.now(timezone.utc)

            amt = float(pydict["amount"][i])
            is_fraud_val = 1 if str(pydict["is_fraud"][i]).strip().lower() in ("yes", "true", "1") else 0

            transactions_list.append({
                "transaction_id": tx_id,
                "event_id": pydict["event_id"][i],
                "user_id": u_id,
                "card_index": c_idx,
                "amount": amt,
                "is_refund": amt < 0,
                "is_declined": pydict["errors"][i] is not None and str(pydict["errors"][i]).strip() != "",
                "decline_reason": pydict["errors"][i] or "None",
                "use_chip": pydict["use_chip"][i] or "Unknown",
                "transaction_entry_method": pydict["transaction_entry_method"][i] or "unknown",
                "mcc": mcc_code,
                "merchant_category": mcc_meta["category"],
                "merchant_name": pydict["merchant_name"][i] or "Unknown",
                "merchant_city": m_city,
                "merchant_state": m_state,
                "merchant_latitude": m_lat,
                "merchant_longitude": m_lon,
                "distance_to_home_km": dist_km,
                "is_foreign_state": (m_state != user_prof.get("state")) if (m_state and user_prof.get("state")) else False,
                "is_fraud": is_fraud_val,
                "timestamp_utc": dt.isoformat(),
                "year": dt.year,
                "month": dt.month,
                "day": dt.day,
                "hour": dt.hour,
                "day_of_week": dt.weekday(),
                "source_pipeline": "batch_extractor",
            })
    except Exception as e:
        print(f"  Error reading batch {key}: {e}")

# 4.2 Read Streaming JSON.GZ
tx_stream_res = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="bronze/transactions/partition=")
tx_stream_keys = [o["Key"] for o in tx_stream_res.get("Contents", []) if o["Key"].endswith(".json.gz")]

print(f"  Processing {len(tx_stream_keys)} Bronze streaming JSON.GZ files...")
for key in tx_stream_keys:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        decomp = gzip.decompress(obj["Body"].read()).decode("utf-8")
        for line in decomp.strip().split("\n"):
            if not line:
                continue
            r = json.loads(line)
            val = r.get("value", {})
            if isinstance(val, str):
                try:
                    val = json.loads(val)
                except Exception:
                    pass
            data = val.get("payload") or val.get("data") or val if isinstance(val, dict) else {}
            if not data or not isinstance(data, dict):
                continue
            tx_id = data.get("transaction_id", val.get("event_id", f"tx-stream-{len(transactions_list)}"))
            if tx_id in seen_tx_ids:
                continue
            seen_tx_ids.add(tx_id)

            u_id = int(data.get("User") or data.get("user_id") or 0)
            c_idx = int(data.get("Card") or data.get("card_id") or data.get("card_index") or 0)
            raw_amt = data.get("Amount") or data.get("amount") or 0.0
            amt = clean_currency_to_float(raw_amt)
            mcc_code = int(data.get("MCC") or data.get("mcc") or 5411)
            mcc_meta = mcc_lookup.get(mcc_code, {"category": "Retail", "description": "Retail"})
            user_prof = users_map.get(u_id, {})
            u_lat = user_prof.get("latitude")
            u_lon = user_prof.get("longitude")

            raw_fraud = data.get("Is Fraud?") or data.get("is_fraud", "false")
            is_fraud_val = 1 if str(raw_fraud).lower() in ("true", "yes", "1") else 0
            ts_epoch = r.get("timestamp") or time.time() * 1000
            dt = datetime.fromtimestamp(ts_epoch / 1000.0, timezone.utc)

            status_val = str(data.get("status", "")).upper()
            has_error = bool(data.get("Errors?")) or (status_val == "DECLINED")

            transactions_list.append({
                "transaction_id": str(tx_id),
                "event_id": str(val.get("event_id", tx_id)),
                "user_id": u_id,
                "card_index": c_idx,
                "amount": amt,
                "is_refund": amt < 0,
                "is_declined": has_error,
                "decline_reason": str(data.get("Errors?") or ("Declined" if has_error else "None")),
                "use_chip": str(data.get("Use Chip", "Chip Transaction")),
                "transaction_entry_method": str(data.get("Transaction Entry Method", "contactless_chip")),
                "mcc": mcc_code,
                "merchant_category": mcc_meta["category"],
                "merchant_name": str(data.get("Merchant Name") or data.get("merchant_name") or "Supermarket"),
                "merchant_city": str(data.get("Merchant City") or user_prof.get("city", "Paris")),
                "merchant_state": str(data.get("Merchant State") or user_prof.get("state", "IDF")),
                "merchant_latitude": u_lat,
                "merchant_longitude": u_lon,
                "distance_to_home_km": 0.0,
                "is_foreign_state": False,
                "is_fraud": is_fraud_val,
                "timestamp_utc": dt.isoformat(),
                "year": dt.year,
                "month": dt.month,
                "day": dt.day,
                "hour": dt.hour,
                "day_of_week": dt.weekday(),
                "source_pipeline": "streaming_msk",
            })
    except Exception as e:
        continue

print(f"  Total conformed Silver transactions assembled: {len(transactions_list)}")

# Build Silver Transactions Arrow Table
silver_tx_columns = list(transactions_list[0].keys()) if transactions_list else []
silver_tx_pydict = {col: [t[col] for t in transactions_list] for col in silver_tx_columns}
silver_tx_table = pa.Table.from_pydict(silver_tx_pydict)

silver_tx_buf = io.BytesIO()
pq.write_table(silver_tx_table, silver_tx_buf, compression="snappy")
s3.put_object(
    Bucket=S3_BUCKET,
    Key="silver/transactions/silver_transactions.parquet",
    Body=silver_tx_buf.getvalue(),
)
print(f"  [SILVER] Written {len(silver_tx_table)} records -> s3://{S3_BUCKET}/silver/transactions/silver_transactions.parquet")


# ==============================================================================
# STEP 5: Gold Layer (Star Schema + ML Feature Store + Analytics)
# ==============================================================================
print("\n[STEP 5/5] Building Gold Layer Analytical Models & ML Feature Store...")

# 5.1 Gold Star Schema Fact: fact_transactions
gold_fact_data = {
    "transaction_id": silver_tx_pydict["transaction_id"],
    "user_id": silver_tx_pydict["user_id"],
    "card_index": silver_tx_pydict["card_index"],
    "mcc": silver_tx_pydict["mcc"],
    "amount": silver_tx_pydict["amount"],
    "is_fraud": silver_tx_pydict["is_fraud"],
    "is_refund": silver_tx_pydict["is_refund"],
    "is_declined": silver_tx_pydict["is_declined"],
    "distance_to_home_km": silver_tx_pydict["distance_to_home_km"],
    "is_foreign_state": silver_tx_pydict["is_foreign_state"],
    "entry_method": silver_tx_pydict["transaction_entry_method"],
    "timestamp_utc": silver_tx_pydict["timestamp_utc"],
    "year": silver_tx_pydict["year"],
    "month": silver_tx_pydict["month"],
    "hour": silver_tx_pydict["hour"],
}
gold_fact_tab = pa.Table.from_pydict(gold_fact_data)
gold_fact_buf = io.BytesIO()
pq.write_table(gold_fact_tab, gold_fact_buf, compression="snappy")
s3.put_object(
    Bucket=S3_BUCKET,
    Key="gold/fact_transactions/fact_transactions.parquet",
    Body=gold_fact_buf.getvalue(),
)
print(f"  [GOLD] Written Fact Table ({len(gold_fact_tab)} rows) -> s3://{S3_BUCKET}/gold/fact_transactions/fact_transactions.parquet")

# 5.2 Gold ML Feature Store: fraud_features
# Calculate per-user spend profile stats (mean, std)
user_spends = defaultdict(list)
for t in transactions_list:
    user_spends[t["user_id"]].append(t["amount"])

user_avg_spend = {u: (sum(amounts) / len(amounts)) for u, amounts in user_spends.items()}

feature_rows = []
for t in transactions_list:
    u_id = t["user_id"]
    c_idx = t["card_index"]
    amt = t["amount"]
    u_prof = users_map.get(u_id, {})
    c_prof = cards_map.get((u_id, c_idx), {})

    credit_limit = c_prof.get("credit_limit") or 5000.0
    utilization = round((amt / credit_limit), 4) if credit_limit > 0 else 0.0
    avg_amt = user_avg_spend.get(u_id, 50.0)
    amt_ratio_to_avg = round((amt / avg_amt), 2) if avg_amt > 0 else 1.0

    is_night = 1 if (t["hour"] >= 23 or t["hour"] <= 5) else 0

    feature_rows.append({
        "transaction_id": t["transaction_id"],
        "user_id": u_id,
        "amount": amt,
        "credit_limit": credit_limit,
        "card_utilization_ratio": utilization,
        "user_avg_spend": round(avg_amt, 2),
        "spend_deviation_ratio": amt_ratio_to_avg,
        "distance_to_home_km": t["distance_to_home_km"] or 0.0,
        "is_foreign_state": 1 if t["is_foreign_state"] else 0,
        "is_night_transaction": is_night,
        "fico_score": u_prof.get("fico_score") or 700,
        "user_age": u_prof.get("current_age") or 35,
        "card_on_dark_web": 1 if c_prof.get("card_on_dark_web") else 0,
        "is_fraud_label": t["is_fraud"],  # Target label for ML
    })

feature_pydict = {col: [f[col] for f in feature_rows] for col in feature_rows[0].keys()} if feature_rows else {}
gold_features_tab = pa.Table.from_pydict(feature_pydict)
gold_features_buf = io.BytesIO()
pq.write_table(gold_features_tab, gold_features_buf, compression="snappy")
s3.put_object(
    Bucket=S3_BUCKET,
    Key="gold/fraud_feature_store/fraud_feature_store.parquet",
    Body=gold_features_buf.getvalue(),
)
print(f"  [GOLD] Written ML Feature Store ({len(gold_features_tab)} rows) -> s3://{S3_BUCKET}/gold/fraud_feature_store/fraud_feature_store.parquet")

# 5.3 Gold Merchant Risk Analytics
mcc_groups = defaultdict(lambda: {"total_tx": 0, "total_vol": 0.0, "fraud_count": 0, "declines": 0})
for t in transactions_list:
    m = t["mcc"]
    mcc_groups[m]["total_tx"] += 1
    mcc_groups[m]["total_vol"] += t["amount"]
    mcc_groups[m]["fraud_count"] += t["is_fraud"]
    mcc_groups[m]["declines"] += 1 if t["is_declined"] else 0

merchant_risk_rows = []
for mcc_code, stats in mcc_groups.items():
    meta = mcc_lookup.get(mcc_code, {"category": "Other", "description": "Other"})
    tot = stats["total_tx"]
    fraud_rate = round((stats["fraud_count"] / tot) * 100.0, 2) if tot > 0 else 0.0
    merchant_risk_rows.append({
        "mcc": mcc_code,
        "merchant_category": meta["category"],
        "category_description": meta["description"],
        "total_transactions": tot,
        "total_volume_usd": round(stats["total_vol"], 2),
        "avg_ticket_size": round(stats["total_vol"] / tot, 2) if tot > 0 else 0.0,
        "fraud_count": stats["fraud_count"],
        "fraud_rate_pct": fraud_rate,
        "decline_count": stats["declines"],
    })

merchant_risk_pydict = {col: [m[col] for m in merchant_risk_rows] for col in merchant_risk_rows[0].keys()}
gold_merchant_tab = pa.Table.from_pydict(merchant_risk_pydict)
gold_merchant_buf = io.BytesIO()
pq.write_table(gold_merchant_tab, gold_merchant_buf, compression="snappy")
s3.put_object(
    Bucket=S3_BUCKET,
    Key="gold/merchant_analytics/merchant_risk_profile.parquet",
    Body=gold_merchant_buf.getvalue(),
)
print(f"  [GOLD] Written Merchant Risk Analytics ({len(gold_merchant_tab)} categories) -> s3://{S3_BUCKET}/gold/merchant_analytics/merchant_risk_profile.parquet")

# 5.4 Gold Daily KPI Summary
daily_groups = defaultdict(lambda: {"tx_count": 0, "volume": 0.0, "fraud_count": 0, "users": set()})
for t in transactions_list:
    d_key = f"{t['year']:04d}-{t['month']:02d}-{t['day']:02d}"
    daily_groups[d_key]["tx_count"] += 1
    daily_groups[d_key]["volume"] += t["amount"]
    daily_groups[d_key]["fraud_count"] += t["is_fraud"]
    daily_groups[d_key]["users"].add(t["user_id"])

kpi_rows = []
for d_key, stats in sorted(daily_groups.items()):
    c = stats["tx_count"]
    kpi_rows.append({
        "date_key": d_key,
        "total_transactions": c,
        "total_volume_usd": round(stats["volume"], 2),
        "unique_active_users": len(stats["users"]),
        "fraud_incidents": stats["fraud_count"],
        "fraud_rate_pct": round((stats["fraud_count"] / c) * 100.0, 3) if c > 0 else 0.0,
        "avg_ticket_size": round(stats["volume"] / c, 2) if c > 0 else 0.0,
    })

kpi_pydict = {col: [k[col] for k in kpi_rows] for col in kpi_rows[0].keys()}
gold_kpi_tab = pa.Table.from_pydict(kpi_pydict)
gold_kpi_buf = io.BytesIO()
pq.write_table(gold_kpi_tab, gold_kpi_buf, compression="snappy")
s3.put_object(
    Bucket=S3_BUCKET,
    Key="gold/daily_kpis/daily_kpi_summary.parquet",
    Body=gold_kpi_buf.getvalue(),
)
print(f"  [GOLD] Written Daily KPI Summary ({len(gold_kpi_tab)} days) -> s3://{S3_BUCKET}/gold/daily_kpis/daily_kpi_summary.parquet")

print("\n" + "=" * 80)
print("SUCCESS: Silver & Gold Medallion Lakehouse tiers fully created on S3!")
print("=" * 80)
