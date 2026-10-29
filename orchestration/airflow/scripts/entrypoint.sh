#!/bin/bash
set -e

echo "=== [SIC AIRFLOW FARGATE STARTUP] ==="
echo "AWS Region: ${AWS_DEFAULT_REGION:-eu-west-3}"
echo "S3 Bucket: ${S3_BUCKET:-sic-pipeline-storage}"

# 1. Ensure directories exist
mkdir -p /opt/airflow/dags /opt/airflow/logs

# 2. Sync latest DAGs from S3
echo "[INFO] Syncing DAGs from s3://${S3_BUCKET}/airflow/dags/ ..."
python3 -c "
import os, boto3
s3 = boto3.client('s3', region_name=os.environ.get('AWS_DEFAULT_REGION', 'eu-west-3'))
bucket = os.environ.get('S3_BUCKET', 'sic-pipeline-storage')
resp = s3.list_objects_v2(Bucket=bucket, Prefix='airflow/dags/')
for obj in resp.get('Contents', []):
    key = obj['Key']
    if key.endswith('.py'):
        fname = os.path.basename(key)
        local_path = os.path.join('/opt/airflow/dags', fname)
        print(f'Downloading {key} -> {local_path}')
        s3.download_file(bucket, key, local_path)
"

# 3. Initialize Airflow Database
echo "[INFO] Running Airflow DB migration..."
airflow db migrate

# 4. Create Admin User if not exists
echo "[INFO] Ensuring admin user exists..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@sic-pipeline.internal \
    --password admin || true

# 5. Ensure databricks_default connection exists if credentials provided
if [ -n "$DATABRICKS_HOST" ] && [ -n "$DATABRICKS_TOKEN" ]; then
    echo "[INFO] Configuring databricks_default connection for Databricks Lakehouse ($DATABRICKS_HOST)..."
    airflow connections add 'databricks_default' \
        --conn-type 'databricks' \
        --conn-host "$DATABRICKS_HOST" \
        --conn-password "$DATABRICKS_TOKEN" || true
fi

# 6. Start Scheduler in background
echo "[INFO] Starting Airflow Scheduler in background..."
airflow scheduler &

# 7. Start Webserver in foreground (1 worker for lightweight footprint on 0.25 vCPU)
echo "[INFO] Starting Airflow Webserver on 0.0.0.0:8080 with 1 worker..."
exec airflow webserver --hostname 0.0.0.0 --port 8080 --workers 1
