"""Deterministic reference fraud rules; Spark production equivalent requires separate validation."""
from collections import defaultdict, deque
from datetime import timedelta
from decimal import Decimal
from math import radians, sin, cos, asin, sqrt
from uuid import uuid5, NAMESPACE_URL
from src.common.contracts import normalize, parse_utc

RULE_VERSION='local-reference-1'

def distance_km(a, b):
    p1,p2=radians(a[0]),radians(b[0]); dp=p2-p1; dl=radians(b[1]-a[1])
    return 6371 * 2 * asin(min(1,sqrt(sin(dp/2)**2+cos(p1)*cos(p2)*sin(dl/2)**2)))

def detect(events, *, cnp_min=3, micro_min=3, window_minutes=10, geo_threshold_km=8046.72):
    """Sorted event-time demonstration; positive upper bound inclusive, lower bound exclusive.

    Actual out-of-order Structured Streaming requires watermark/state and explicit late policy.
    """
    normalized = [normalize(e) for e in events]
    normalized.sort(key=lambda e: (parse_utc(e['event_time']), e['event_id']))
    seen=set(); histories=defaultdict(lambda: defaultdict(deque)); account_windows=defaultdict(deque); alerts=[]; velocities={}
    for event in normalized:
        eid=event['event_id']
        if eid in seen:
            continue
        seen.add(eid)
        time=parse_utc(event['event_time']); account=event['account_id']; card=event['card_id']
        h=histories[card]
        positive=Decimal(event['amount']) > 0 and event['status'] == 'APPROVED'
        for queue in h.values():
            while queue and time - queue[0][0] >= timedelta(minutes=window_minutes):
                queue.popleft()
        if positive:
            h['velocity'].append((time,eid))
            if event['channel']=='ECOMMERCE':h['cnp'].append((time,eid))
            if event['channel']=='ECOMMERCE' and Decimal(event['amount']) <= Decimal('2.00'):
                h['micro'].append((time,eid))
        aw=account_windows[account]
        while aw and time-aw[0][0] >= timedelta(minutes=window_minutes):
            aw.popleft()
        if positive:
            aw.append((time,eid))
        velocities[eid]={'card_10m':len(h['velocity']), 'account_10m':len(aw), 'account_id':account}

        def alert(rule, evidence):
            alerts.append({'alert_id':str(uuid5(NAMESPACE_URL,eid+':'+rule+':'+RULE_VERSION)),
                           'event_id':eid,'rule_id':rule,'rule_version':RULE_VERSION,'evidence':evidence,
                           'detected_at':time.isoformat(), 'source':'LOCAL_REFERENCE_RULES'})
        if positive and event['channel']=='ECOMMERCE' and len(h['cnp']) >= cnp_min:
            alert('CNP_BURST',{'events_in_10m':len(h['cnp'])})
        if positive and event['channel']=='ECOMMERCE' and Decimal(event['amount']) <= 2 and len(h['micro']) >= micro_min:
            alert('MICRO_TESTING',{'small_attempts_in_10m':len(h['micro'])})
        if positive and event['location_precision']=='EXACT_SYNTHETIC':
            current=(event['latitude'],event['longitude'])
            for prev_time,prev_location,prev_eid in reversed(h['geo']):
                mins=(time-prev_time).total_seconds()/60
                if mins <= 0:continue
                distance=distance_km(prev_location,current)
                if distance >= geo_threshold_km and mins <= 15:
                    alert('GEO_VELOCITY', {'distance_km':round(distance,1),'elapsed_minutes':mins,'prior_event_id':prev_eid})
                    break
            h['geo'].append((time,current,eid))
    return normalized,alerts,velocities
