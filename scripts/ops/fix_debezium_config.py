import json
import os
import sys
import time
import urllib.parse
import urllib.request
import boto3

connect_ip = "172.31.10.236"
connect_url = f"http://{connect_ip}:8083"
print(f"[INFO] Connecting to Kafka Connect at {connect_url}...")

# Retrieve DB secret
sm = boto3.client("secretsmanager", region_name="eu-west-3")
secret_resp = sm.get_secret_value(SecretId="synthetic-data-provider/database")
db_info = json.loads(secret_resp["SecretString"])

parsed = urllib.parse.urlparse(db_info["DATABASE_URL"])
db_host = parsed.hostname
db_port = str(parsed.port or 5432)
db_user = parsed.username
db_password = parsed.password
db_name = parsed.path.lstrip("/")

# Debezium config with explicit $1
debezium_config = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "plugin.name": "pgoutput",
    "database.hostname": db_host,
    "database.port": db_port,
    "database.user": db_user,
    "database.password": db_password,
    "database.dbname": db_name,
    "database.server.name": "sic",
    "database.sslmode": "require",
    "topic.prefix": "sic",
    "table.include.list": "public.users,public.cards",
    "publication.name": "debezium_publication",
    "slot.name": "debezium_cdc_slot",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "double",
    "time.precision.mode": "connect",
    "transforms": "reroute",
    "transforms.reroute.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.reroute.regex": r"(?:.*\.public\.)?(.*)",
    "transforms.reroute.replacement": "$1",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
}

print(f"[INFO] Updating debezium-postgres-source config with replacement='$1'...")
req = urllib.request.Request(
    f"{connect_url}/connectors/debezium-postgres-source/config",
    data=json.dumps(debezium_config).encode(),
    headers={"Content-Type": "application/json"},
    method="PUT",
)
try:
    with urllib.request.urlopen(req) as resp:
        print("[INFO] PUT Response:", resp.getcode())
except urllib.error.HTTPError as e:
    print("[ERROR] PUT failed:", e.code, e.read().decode())
    sys.exit(1)

# Restart connector with includeTasks=true
print("[INFO] Restarting debezium-postgres-source with includeTasks=true...")
restart_req = urllib.request.Request(
    f"{connect_url}/connectors/debezium-postgres-source/restart?includeTasks=true",
    data=b"{}",
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(restart_req) as resp:
        print("[INFO] Restart response:", resp.getcode())
except Exception as e:
    print("[WARN] Restart call:", e)

# Wait 15 seconds for connector task initialization
print("[INFO] Waiting 15 seconds for connector task startup...")
time.sleep(15)

status_req = urllib.request.urlopen(f"{connect_url}/connectors?expand=status")
status = json.loads(status_req.read().decode())
print("\n[INFO] Verified Connectors Status:")
all_healthy = True
for name, val in status.items():
    c_state = val.get("status", {}).get("connector", {}).get("state")
    print(f"Connector: {name} => {c_state}")
    for t in val.get("status", {}).get("tasks", []):
        t_state = t.get("state")
        print(f"  Task {t.get('id')}: {t_state}")
        if t.get("trace"):
            print(f"  Trace: {t.get('trace')[:300]}")
        if t_state != "RUNNING":
            all_healthy = False

if all_healthy:
    print("\n>>> ALL CONNECTORS AND TASKS ARE 100% HEALTHY & RUNNING! <<<")
else:
    print("\n>>> WARNING: One or more connectors/tasks not RUNNING! <<<")
