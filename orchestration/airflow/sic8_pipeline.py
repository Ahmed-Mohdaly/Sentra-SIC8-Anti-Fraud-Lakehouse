"""Small local reference DAG that performs real local fixture and quality gate.

This is not a live AWS/Databricks orchestration deployment. It does not run a
perpetual consumer; live streaming health is a separate platform concern.
"""
from __future__ import annotations
from datetime import datetime
from pathlib import Path

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    DAG = None


def generate_fixture():
    from src.common.pipeline import run_local
    run_local('build/airflow-demo')


def validate_fixture():
    import json
    p=Path('build/airflow-demo/report.json')
    r=json.loads(p.read_text())
    if r['transactions']<=0 or not r['records_without_truth_in_detection']:
        raise ValueError('Local quality gate failed')

if DAG is not None:
    with DAG('sic8_local_reference',start_date=datetime(2026,1,1),schedule=None,catchup=False,
             tags=['SIC8','DEMO_FIXTURE'],max_active_runs=1) as dag:
        create=PythonOperator(task_id='generate_validate_publish_local_fixture',python_callable=generate_fixture,retries=1)
        gate=PythonOperator(task_id='reconcile_gold_fixture',python_callable=validate_fixture,retries=1)
        create >> gate
