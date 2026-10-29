import json
from pathlib import Path
import pytest
from jsonschema import Draft202012Validator
from src.r1_ingestion.generator import make_fixture

BASE=Path('contracts/jsonschema')
@pytest.mark.parametrize('schema',list(BASE.glob('*.json')))
def test_schema_compiles(schema):
    Draft202012Validator.check_schema(json.loads(schema.read_text()))

def test_good_and_bad_transaction_contract():
    validator=Draft202012Validator(json.loads((BASE/'transaction-v1-draft.json').read_text()))
    events,_=make_fixture()
    assert not list(validator.iter_errors(events[0]))
    bad=dict(events[0],channel='CARD_READER',is_fraud=True)
    assert list(validator.iter_errors(bad))
