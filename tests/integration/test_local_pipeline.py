import sqlite3
from src.common.pipeline import run_local

def test_idempotent_fixture_pipeline(tmp_path):
    first=run_local(tmp_path)
    second=run_local(tmp_path)
    assert first['transactions']==second['transactions'] and second['silver_inserted_this_run']==0
    assert first['alerts']>=3 and first['truth_positive_events']>=3
    assert first['terminal_failures']==1 and first['terminal_attempts']>=1
    assert first['fixture_mode']=='DEMO_FIXTURE' and first['model_status']=='MODEL_NOT_CONFIGURED'

def test_fact_alert_separation(tmp_path):
    run_local(tmp_path)
    with sqlite3.connect(tmp_path/'gold_fixture.sqlite') as c:
        assert c.execute('select count(*) from fact_transactions').fetchone()[0] == 13
        assert c.execute('select count(*) from fact_alerts').fetchone()[0] >=3
        # Truth stored in another table, not transaction facts.
        cols=[x[1] for x in c.execute('pragma table_info(fact_transactions)').fetchall()]
        assert 'scenario_family' not in cols and 'is_fraud' not in cols
