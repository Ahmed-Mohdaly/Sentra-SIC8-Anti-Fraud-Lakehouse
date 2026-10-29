"""Validated local CDC current-state/SCD2 helper for demonstration.

Does not replace production Debezium LSN/time ordering or historical retroactive repair.
"""
from dataclasses import dataclass

@dataclass(frozen=True)
class CdcEvent:
    entity_id: str
    op: str
    ordering_key: int
    after: dict | None


def apply_cdc(events):
    """Order by monotonically increasing source position, suppress old update, honor delete."""
    current={}; seen={}
    for e in sorted(events,key=lambda x:(x.ordering_key,x.entity_id)):
        if e.op not in ('c','u','d','r'):
            raise ValueError('Invalid Debezium op')
        if e.ordering_key <= seen.get(e.entity_id,-1):
            continue
        seen[e.entity_id]=e.ordering_key
        if e.op=='d':current.pop(e.entity_id,None)
        elif not isinstance(e.after,dict):raise ValueError('after required for c/u/r')
        else:current[e.entity_id]=e.after
    return current
