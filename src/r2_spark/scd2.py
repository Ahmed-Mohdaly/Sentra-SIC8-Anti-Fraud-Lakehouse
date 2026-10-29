"""Pure reference SCD2 interval builder (supports before/after CDC source ordering).

Real Delta SCD2 MERGE and retroactive corrections require Databricks tests.
"""
from datetime import datetime, timezone
from src.common.contracts import parse_utc


def history_for_entity(events):
    """Return ordered versions with half-open [valid_from, valid_to) intervals.

    Events are dicts: entity_id, source_position, op, event_time, after.
    Duplicate/stale positions collapse. Deletes end the prior version.
    """
    ordered=sorted(events,key=lambda e:(e['source_position'],e['event_time']))
    if not ordered: return []
    entities={x['entity_id'] for x in ordered}
    if len(entities)!=1:raise ValueError('One entity per history_for_entity invocation')
    out=[];positions=set()
    for ev in ordered:
        pos=ev['source_position']
        if pos in positions: continue
        positions.add(pos)
        op=ev['op']
        if op not in ('c','u','r','d'):raise ValueError('invalid CDC op')
        time=parse_utc(ev['event_time']).isoformat()
        if out and out[-1]['is_current']:
            if time < out[-1]['valid_from']:raise ValueError('source positions disagree with effective time')
            out[-1]['valid_to']=time
            out[-1]['is_current']=False
        if op!='d':
            if not isinstance(ev.get('after'),dict):raise ValueError('CDC update requires after data')
            out.append({'entity_id':ev['entity_id'],'source_position':pos,'valid_from':time,
                        'valid_to':None,'is_current':True,'attributes':ev['after']})
    for a,b in zip(out,out[1:]):
        if a['valid_to'] is None or a['valid_to']>b['valid_from']:
            raise ValueError('overlapping SCD2 intervals')
    return out
