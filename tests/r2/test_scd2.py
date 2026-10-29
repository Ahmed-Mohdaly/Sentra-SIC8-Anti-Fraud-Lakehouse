from src.r2_spark.scd2 import history_for_entity
import pytest

def test_scd2_intervals_and_delete():
    ev=[
      dict(entity_id='1',source_position=200,op='u',event_time='2026-10-26T12:00:00Z',after={'segment':'B'}),
      dict(entity_id='1',source_position=100,op='c',event_time='2026-10-26T11:00:00Z',after={'segment':'A'}),
      dict(entity_id='1',source_position=300,op='d',event_time='2026-10-26T13:00:00Z',after=None)]
    rows=history_for_entity(ev)
    assert len(rows)==2
    assert rows[0]['valid_to']==rows[1]['valid_from']
    assert not rows[1]['is_current'] and rows[1]['valid_to'].startswith('2026-10-26T13:00')

def test_scd2_source_position_conflicts_time():
    ev=[dict(entity_id='1',source_position=10,op='c',event_time='2026-10-26T12:00:00Z',after={}),
        dict(entity_id='1',source_position=20,op='u',event_time='2026-10-26T11:00:00Z',after={})]
    with pytest.raises(ValueError,match='disagree'): history_for_entity(ev)
