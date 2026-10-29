"""Optional actual local Kafka publish (requires running broker and kafka-python).

Cloud publishing is explicitly prohibited. This does not replace R1 team's code.
"""
import json
import os
from src.r1_ingestion.generator import make_fixture


def publish_local_kafka(*, bootstrap='localhost:9092',topic='sic8-local-transactions', allow=False):
    if not allow or os.getenv('SIC8_CLOUD_WRITES','false').lower()!='false':
        raise PermissionError('Local Kafka requires allow=True and SIC8_CLOUD_WRITES=false')
    if bootstrap not in ('localhost:9092','127.0.0.1:9092'):
        raise PermissionError('Only local Kafka is allowed in reference publisher')
    from kafka import KafkaProducer  # optional dependency, never imported by offline tests
    producer=KafkaProducer(bootstrap_servers=[bootstrap],value_serializer=lambda x:json.dumps(x).encode('utf8'))
    events,truth=make_fixture()
    for e in events:
        payload={'schema_version':'v1-draft','event_id':e['event_id'],'event_type':'transaction.created','data':e}
        producer.send(topic,key=e['account_id'].encode('utf8'),value=payload).get(timeout=10)
    producer.flush(timeout=10)
    producer.close()
    return {'published_events':len(events),'ground_truth_sidecar_count':len(truth),'topic':topic}
