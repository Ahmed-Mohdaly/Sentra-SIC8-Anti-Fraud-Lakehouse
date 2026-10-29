import pytest

@pytest.mark.skip(reason='Live Kafka/MSK -> S3 -> Databricks Delta -> Power BI credentials/artifacts and opt-in not provided')
def test_live_project01_e2e():
    pass

@pytest.mark.skip(reason='Original LightGBM artifact and 34-feature parity registry not provided by R1')
def test_real_model_feature_parity():
    pass

@pytest.mark.skip(reason='Airflow, Docker, Databricks runtime absent; no cloud writes opted in')
def test_airflow_delta_restart():
    pass
