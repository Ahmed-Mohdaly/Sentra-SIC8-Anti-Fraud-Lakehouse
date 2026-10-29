from src.r1_ingestion.generator import make_fixture
from src.r2_spark.rules import detect, distance_km
from src.common.contracts import normalize, ContractError
from src.r2_spark.bronze_adapter import parse_bronze
import pytest, json

def test_alerts_cover_all_three_families():
    events,_=make_fixture()
    _, alerts, _=detect(events)
    assert {x['rule_id'] for x in alerts}=={'CNP_BURST','MICRO_TESTING','GEO_VELOCITY'}

def test_duplicate_event_not_double_counted():
    events,_=make_fixture();_,a,_=detect(events)
    _,b,_=detect(events+[events[0],events[5]])
    assert b==a

def test_refunds_not_counted():
    e,_=make_fixture();n,a,v=detect(e)
    refund=next(x for x in n if x['amount'].startswith('-'))
    assert v[refund['event_id']]['card_10m']==0
    assert not any(x['event_id']==refund['event_id'] for x in a)

def test_geo_uses_exact_only():
    e,_=make_fixture()
    for x in e:
        if x['account_id']=='geo':x['location_precision']='APPROXIMATE_CENTROID'
    _,alerts,_=detect(e)
    assert 'GEO_VELOCITY' not in {a['rule_id'] for a in alerts}

def test_ten_minute_window_exact_boundary_is_excluded():
    e,_=make_fixture();short=[x.copy() for x in e if x['account_id']=='cnp']
    short[2]['event_time']='2026-10-25T12:20:00+00:00'
    _,a,v=detect(short)
    assert not any(x['rule_id']=='CNP_BURST' for x in a)

def test_missing_account_not_fabricated_from_real_r1_sample():
    sample=json.loads(open('tests/fixtures/r1_s3_sample_50.jsonl').readline())
    with pytest.raises(ContractError,match='account_id'):
        parse_bronze(sample)

def test_transport_and_isolated_source_truth():
    e,_=make_fixture()
    raw={'key':'account','offset':7,'value':json.dumps({'event_id':e[0]['event_id'],'timestamp':e[0]['event_time'],'data':dict(e[0],is_fraud=True)})}
    x,meta,truth=parse_bronze(raw)
    assert truth=={'is_fraud':True} and meta['offset']==7
    assert 'is_fraud' not in x

def test_wrong_coordinate_and_timezone():
    e,_=make_fixture();x=dict(e[0],latitude=95)
    with pytest.raises(ContractError):normalize(x)
    with pytest.raises(ContractError):normalize(dict(e[0],event_time='2026-10-25T10:00:00'))

def test_detect_does_not_consume_truth():
    e,_=make_fixture()
    with pytest.raises(ContractError,match='truth'):
        detect([dict(e[0],actual_is_fraud=True)])


def test_account_velocity_counts_across_cards():
    events,_=make_fixture()
    a=dict(events[0],event_id='new-other-card',transaction_id='new-other-card',card_id='different-card')
    a['event_time']='2026-10-25T12:02:00Z'
    _, alerts, velocities = detect([events[0],a])
    assert velocities['new-other-card']['card_10m']==1
    assert velocities['new-other-card']['account_10m']==2

def test_real_s3_sample_parses_with_explicit_account_mapping():
    sample=json.loads(open('tests/fixtures/r1_s3_sample_50.jsonl').readline())
    data,transport,truth=parse_bronze(sample,account_mapping={'888801':'SIM_ACCT_888801'})
    assert data['account_id']=='SIM_ACCT_888801'
    assert data['currency']=='EUR' and data['amount']=='27.25'
    assert data['card_id']=='SIM:888801:0' and 'is_fraud' not in data
    assert truth=={'is_fraud': False} and transport['offset']==275

def test_uploaded_r1_jsonl_file_all_50_rows():
    samples=[json.loads(line) for line in open('tests/fixtures/r1_s3_sample_50.jsonl')]
    parsed=[parse_bronze(x,account_mapping={'888801':'SIM_ACCT_888801'}) for x in samples]
    assert len(samples)==50
    assert len({d['event_id'] for d,_,_ in parsed}) == 50
    assert [m['offset'] for _,m,_ in parsed]==list(range(275,325))
    assert all('is_fraud' not in d for d,_,_ in parsed)
    assert {d['currency'] for d,_,_ in parsed} == {'EUR'}
