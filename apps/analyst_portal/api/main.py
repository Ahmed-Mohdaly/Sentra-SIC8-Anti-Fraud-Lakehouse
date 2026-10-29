"""Read-only fixture API. ENABLE ONLY with local test data and explicit token.

Not production-ready; production requires proper identity, rate limiting, audit logging
and Databricks governed SQL access via the platform owner (R4).
"""
import os
import sqlite3
from pathlib import Path
from fastapi import FastAPI, Header, HTTPException, Query

app=FastAPI(title='SIC8 Analyst Fixture API',version='0.1.0')


def _authorize(authorization):
    token=os.getenv('SIC8_API_DEMO_TOKEN')
    if not token:
        raise HTTPException(status_code=503, detail='Local fixture API is disabled')
    from secrets import compare_digest
    if not authorization or not compare_digest(authorization, 'Bearer '+token):
        raise HTTPException(status_code=401, detail='Unauthorized')


def _connect():
    path=Path(os.getenv('SIC8_DEMO_SQLITE','build/demo/gold_fixture.sqlite')).resolve()
    allowed=Path(os.getenv('SIC8_DEMO_DATA_ROOT','build/demo')).resolve()
    if allowed not in path.parents:
        raise HTTPException(status_code=403, detail='Fixture database path is not allowlisted')
    if not path.is_file():
        raise HTTPException(status_code=503, detail='Generate fixture first')
    con=sqlite3.connect(f'file:{path}?mode=ro',uri=True)
    con.row_factory=sqlite3.Row
    return con

@app.get('/health')
def health():
    return {'status':'UP','mode':'DEMO_FIXTURE','live_data':False}

@app.get('/metrics')
def get_metrics(authorization: str | None = Header(default=None)):
    _authorize(authorization)
    with _connect() as con:
        tx=con.execute('SELECT count(*) AS n FROM fact_transactions').fetchone()['n']
        alerts=con.execute('SELECT count(*) AS n FROM fact_alerts').fetchone()['n']
    return {'mode':'DEMO_FIXTURE','transactions':tx,'alerts':alerts}

@app.get('/trace/{event_id}')
def trace(event_id: str, authorization: str | None = Header(default=None)):
    _authorize(authorization)
    if len(event_id)>128 or not event_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_:.' for c in event_id):
        raise HTTPException(status_code=400,detail='Invalid event_id')
    with _connect() as con:
        event=con.execute('''SELECT event_id,transaction_id,channel,currency,status,event_time
                             FROM fact_transactions WHERE event_id=?''',(event_id,)).fetchone()
        if event is None:raise HTTPException(status_code=404,detail='Event not found')
        rules=[dict(x) for x in con.execute('SELECT alert_id,rule_id FROM fact_alerts WHERE event_id=? LIMIT 50',(event_id,))]
    return {'mode':'DEMO_FIXTURE','event':dict(event),'alerts':rules,
            'stages':{'KAFKA':'NOT_CONNECTED','S3_RAW':'NOT_CONNECTED', 'SILVER':'LOCAL_FIXTURE', 'GOLD':'LOCAL_FIXTURE', 'MODEL':'MODEL_NOT_CONFIGURED'}}

@app.post('/genie/chat')
def genie_unavailable(authorization: str | None = Header(default=None)):
    _authorize(authorization)
    raise HTTPException(status_code=503,detail='Genie entitlement, credentials and governed views not configured; no fabricated answers')
