"""A local, fixture-only vertical slice: raw -> validated events -> rules -> SQLite Gold.

This is not a substitute for R1's actual Kafka, Databricks Delta, or Power BI.
"""
from __future__ import annotations
import json
from pathlib import Path
from src.common.contracts import normalize, ContractError
from src.r1_ingestion.generator import make_fixture
from src.r2_spark.rules import detect
from src.r1_fraud_online.model_adapter import score
from src.common.sqlite_gold import init_db, publish_events, publish_truth, publish_alerts, metrics

def run_local(output_dir: str | Path, *, seed: int = 8, run_id: str='fixture-r1') -> dict:
    folder = Path(output_dir); folder.mkdir(parents=True, exist_ok=True)
    events, truth = make_fixture(seed=seed, run_id=run_id)
    raw_path=folder/'raw_events.jsonl'; truth_path=folder/'isolated_truth.jsonl'
    # Reproduced output is a standalone fixture run; never write into actual S3 Bronze.
    raw_path.write_text(''.join(json.dumps({'value':{'schema_version':'v1-draft','event_id':e['event_id'], 'event_type':'transaction.created','data':e}})+'\n' for e in events), encoding='utf8')
    truth_path.write_text(''.join(json.dumps(t)+'\n' for t in truth), encoding='utf8')
    valid=[]; rejected=[]
    for event in events:
        try:valid.append(normalize(event))
        except ContractError as exc:rejected.append({'event_id':event.get('event_id'),'reason':str(exc)})
    cleaned,alerts,velocities=detect(valid)
    gold=folder/'gold_fixture.sqlite'; con=init_db(gold)
    try:
        inserted=publish_events(con,cleaned,velocities)
        publish_truth(con,truth)
        publish_alerts(con,alerts)
        report=metrics(con,run_id)
    finally:con.close()
    report.update({'run_id':run_id,'fixture_mode':'DEMO_FIXTURE','validated_events':len(valid),
                   'rejected_events':len(rejected),'silver_inserted_this_run':inserted,
                   'model_status':'MODEL_NOT_CONFIGURED','prediction_samples':[score(e).__dict__ for e in cleaned[:1]],
                   'records_without_truth_in_detection':True,'gold_sqlite':str(gold)})
    (folder/'report.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf8')
    return report
