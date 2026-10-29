import pytest

@pytest.mark.skip(reason='Live Kafka/MSK -> S3 -> Databricks Delta -> Power BI credentials/artifacts and opt-in not provided')
def test_live_project01_e2e():
    pass

def test_real_model_feature_parity():
    """Verify R1 verified LightGBM v2 model loads and scores with 34 features."""
    from src.r1_fraud_online.model_adapter import score
    event = {
        'event_id': 'tx-parity-001',
        'account_id': 'u-1920',
        'card_id': 'c-0',
        'channel': 'POS',
        'amount': '150.00',
        'mcc': 5411,
        'currency': 'USD',
        'status': 'APPROVED'
    }
    res = score(event)
    assert res.model_status == 'ACTIVE_V2_LIGHTGBM'
    assert res.state_quality == 'HYDRATED'
    assert 0.0 <= res.probability <= 1.0

@pytest.mark.skip(reason='Airflow, Docker, Databricks runtime absent; no cloud writes opted in')
def test_airflow_delta_restart():
    pass
