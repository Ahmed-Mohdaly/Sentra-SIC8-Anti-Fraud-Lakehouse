import json
import time
import urllib.request

connect_ip = "172.31.13.208"
connect_url = f"http://{connect_ip}:8083"
print(f"[INFO] Connecting to Kafka Connect at {connect_url}...")

# Updated Aiven S3 Sink config with regional STS endpoint and Dead Letter Queue (DLQ)
s3_sink_config = {
    "connector.class": "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
    "tasks.max": "1",
    "topics": "transactions,users,cards",
    "aws.s3.bucket.name": "sic-pipeline-storage",
    "aws.s3.region": "eu-west-3",
    "aws.sts.role.arn": "arn:aws:iam::138257835434:role/syntheticTaskRole",
    "aws.sts.role.session.name": "sic-s3-sink-session",
    "aws.sts.config.endpoint": "https://sts.eu-west-3.amazonaws.com",
    "format.output.type": "jsonl",
    "format.output.fields": "key,value,offset,timestamp",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "file.name.template": "bronze/{{topic}}/partition={{partition}}/{{start_offset:padding=true}}.json.gz",
    "file.compression.type": "gzip",
    "file.max.records": "50",
    "flush.timeout.ms": "5000",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "sic_sink_dlq",
    "errors.deadletterqueue.topic.replication.factor": "2",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
}

print("[INFO] Updating aiven-s3-sink with aws.sts.config.endpoint...")
req = urllib.request.Request(
    f"{connect_url}/connectors/aiven-s3-sink/config",
    data=json.dumps(s3_sink_config).encode(),
    headers={"Content-Type": "application/json"},
    method="PUT",
)
with urllib.request.urlopen(req) as resp:
    print("[INFO] PUT Response:", resp.getcode())

# Restart aiven-s3-sink connector
print("[INFO] Restarting aiven-s3-sink with includeTasks=true...")
restart_req = urllib.request.Request(
    f"{connect_url}/connectors/aiven-s3-sink/restart?includeTasks=true",
    data=b"{}",
    headers={"Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(restart_req) as resp:
        print("[INFO] Restart response:", resp.getcode())
except Exception as e:
    print("[WARN] Restart call:", e)

print("[INFO] Waiting 15 seconds for connector task reinitialization...")
time.sleep(15)

status_req = urllib.request.urlopen(f"{connect_url}/connectors?expand=status")
status = json.loads(status_req.read().decode())
print("\n[INFO] Verified Connectors Status:")
for name, val in status.items():
    c_state = val.get("status", {}).get("connector", {}).get("state")
    print(f"Connector: {name} => {c_state}")
    for t in val.get("status", {}).get("tasks", []):
        t_state = t.get("state")
        print(f"  Task {t.get('id')}: {t_state}")
        if t.get("trace"):
            print(f"  Trace: {t.get('trace')[:300]}")
