from src.r1_ingestion.generator import make_fixture
from src.common.contracts import TRUTH_FORBIDDEN

def test_deterministic_three_channels_and_truth_separated():
    a,t=make_fixture();b,t2=make_fixture()
    assert a==b and t==t2
    assert {e['channel'] for e in a}=={'POS','ATM','ECOMMERCE'}
    assert {x['scenario_family'] for x in t}=={'CNP_BURST','GEO_VELOCITY','MICRO_TESTING'}
    assert all(not set(e).intersection(TRUTH_FORBIDDEN) for e in a)

def test_different_seed_changes_fixture():
    assert make_fixture(seed=8)[0]!=make_fixture(seed=10)[0]
