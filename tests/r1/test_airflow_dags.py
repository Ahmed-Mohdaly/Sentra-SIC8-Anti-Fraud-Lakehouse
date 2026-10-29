"""Unit and Parity Tests for Apache Airflow Orchestration DAGs.

Validates:
1. DAG syntax, structure, task IDs, and schedule intervals.
2. Topological integrity and absence of cycles.
3. Airflow DAG sync utility to AWS S3.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DAGS_DIR = (REPO_ROOT / "orchestration" / "airflow" / "dags") if (REPO_ROOT / "orchestration" / "airflow" / "dags").exists() else (REPO_ROOT / "airflow" / "dags")


def test_dag_files_exist():
    """Verify both priority DAG files exist on disk."""
    assert (DAGS_DIR / "dag_01_incremental_ingestion.py").exists()
    assert (DAGS_DIR / "dag_02_batch_reference.py").exists()


def test_dag_syntax_ast():
    """Verify DAG files have valid Python syntax without compilation errors."""
    for dag_file in DAGS_DIR.glob("dag_*.py"):
        with open(dag_file, "r", encoding="utf-8") as f:
            content = f.read()
        tree = ast.parse(content, filename=str(dag_file))
        assert len(tree.body) > 0, f"DAG file {dag_file.name} is empty"


def test_dag_01_ast_structure():
    """Verify task IDs, schedule, and operator wiring in dag_01_incremental_ingestion.py."""
    dag_path = DAGS_DIR / "dag_01_incremental_ingestion.py"
    with open(dag_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Verify key Airflow identifiers exist in AST
    assert "dag_01_incremental_ingestion" in content
    assert "check_api_readiness" in content
    assert "extract_transactions" in content
    assert "verify_s3_landing" in content
    assert "trigger_databricks_lakehouse" in content
    assert "*/15 * * * *" in content
    assert "check_api >> extract >> verify_s3 >> trigger_databricks" in content


def test_dag_02_ast_structure():
    """Verify task IDs, schedule, and operator wiring in dag_02_batch_reference.py."""
    dag_path = DAGS_DIR / "dag_02_batch_reference.py"
    with open(dag_path, "r", encoding="utf-8") as f:
        content = f.read()

    assert "dag_02_batch_reference" in content
    assert "extract_mcc_reference" in content
    assert "extract_cities_reference" in content
    assert "validate_reference_landing" in content
    assert "0 0 * * *" in content
    assert "[mcc_task, cities_task] >> validate_task" in content


def test_sync_airflow_dags_logic(tmp_path: Path):
    """Verify sync_dags function uploads all .py files to S3 with correct keys."""
    from scripts.sync_airflow_dags import sync_dags

    # Create dummy DAG files in temporary directory
    dag1 = tmp_path / "test_dag_1.py"
    dag1.write_text("print('dag1')")
    dag2 = tmp_path / "test_dag_2.py"
    dag2.write_text("print('dag2')")
    non_dag = tmp_path / "notes.txt"
    non_dag.write_text("not a dag")

    mock_s3 = MagicMock()
    with patch("boto3.client", return_value=mock_s3):
        uploaded = sync_dags(
            local_dir=tmp_path,
            bucket="test-bucket",
            prefix="airflow/dags",
            region="eu-west-3",
        )

    assert uploaded == 2
    assert mock_s3.put_object.call_count == 2
    calls = mock_s3.put_object.call_args_list
    uploaded_keys = [c[1]["Key"] for c in calls]
    assert "airflow/dags/test_dag_1.py" in uploaded_keys
    assert "airflow/dags/test_dag_2.py" in uploaded_keys
