import pytest
from src.r2_spark.spark_jobs import merge_silver_foreach_batch

def test_write_guard():
    with pytest.raises(PermissionError,match='disabled'):
        merge_silver_foreach_batch(None,1,'workspace.sic8_silver.transactions')
