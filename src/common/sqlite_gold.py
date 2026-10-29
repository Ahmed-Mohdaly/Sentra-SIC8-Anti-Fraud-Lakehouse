"""Local Gold analog for deterministic fixture testing, not Databricks Gold deployment."""
import json
import sqlite3
from pathlib import Path

DDL='''
CREATE TABLE IF NOT EXISTS fact_transactions(
    event_id TEXT PRIMARY KEY, transaction_id TEXT NOT NULL, run_id TEXT NOT NULL,
    account_id TEXT NOT NULL, card_id TEXT NOT NULL, channel TEXT NOT NULL,
    currency TEXT NOT NULL, amount TEXT NOT NULL, status TEXT NOT NULL,
    event_time TEXT NOT NULL, terminal_id TEXT, terminal_failure_reason TEXT,
    card_velocity_10m INTEGER NOT NULL, account_velocity_10m INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_alerts(
    alert_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, rule_id TEXT NOT NULL,
    evidence_json TEXT NOT NULL, FOREIGN KEY(event_id) REFERENCES fact_transactions(event_id)
);
CREATE TABLE IF NOT EXISTS scenario_truth(
    event_id TEXT NOT NULL, run_id TEXT NOT NULL, scenario_family TEXT NOT NULL,
    PRIMARY KEY(event_id,scenario_family)
);
'''

def init_db(path):
    con=sqlite3.connect(str(path));con.execute('PRAGMA foreign_keys=ON');con.executescript(DDL)
    return con

def publish_events(con,events,velocity):
    before=con.total_changes
    with con:
        for e in events:
            con.execute('''INSERT OR IGNORE INTO fact_transactions
              (event_id, transaction_id, run_id, account_id, card_id, channel, currency,
               amount, status, event_time, terminal_id, terminal_failure_reason,card_velocity_10m,account_velocity_10m)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (e['event_id'],e['transaction_id'],e.get('run_id','unknown'),e['account_id'],e['card_id'],e['channel'],e['currency'],e['amount'],e['status'],e['event_time'],e.get('terminal_id'),e.get('terminal_failure_reason'),velocity[e['event_id']]['card_10m'],velocity[e['event_id']]['account_10m']))
    return con.total_changes-before

def publish_truth(con, truth):
    with con:
        for t in truth:
            con.execute('INSERT OR IGNORE INTO scenario_truth VALUES (?,?,?)',(t['event_id'],t['run_id'],t['scenario_family']))

def publish_alerts(con,alerts):
    with con:
        for a in alerts:
            con.execute('INSERT OR IGNORE INTO fact_alerts VALUES (?,?,?,?)',(a['alert_id'],a['event_id'],a['rule_id'],json.dumps(a['evidence'])))

def metrics(con,run_id):
    def scalar(q,*params):return con.execute(q,params).fetchone()[0]
    params=(run_id,)
    total=scalar('SELECT count(*) FROM fact_transactions WHERE run_id=?',*params)
    alerts=scalar('''SELECT count(*) FROM fact_alerts a JOIN fact_transactions t USING(event_id) WHERE t.run_id=?''',*params)
    unique=scalar('''SELECT count(DISTINCT a.event_id) FROM fact_alerts a JOIN fact_transactions t USING(event_id) WHERE t.run_id=?''',*params)
    positive=scalar('SELECT count(DISTINCT event_id) FROM scenario_truth WHERE run_id=?',*params)
    tp=scalar('''SELECT count(DISTINCT t.event_id) FROM scenario_truth t
                JOIN fact_alerts a ON t.event_id=a.event_id WHERE t.run_id=?''',*params)
    terminal=scalar('''SELECT count(*) FROM fact_transactions WHERE run_id=? AND terminal_id IS NOT NULL''',*params)
    terminal_fail=scalar('''SELECT count(*) FROM fact_transactions WHERE run_id=?
                     AND terminal_id IS NOT NULL AND terminal_failure_reason IS NOT NULL''',*params)
    families=scalar('SELECT count(DISTINCT scenario_family) FROM scenario_truth WHERE run_id=?',*params)
    triggered=scalar('''SELECT count(DISTINCT s.scenario_family) FROM scenario_truth s
                      JOIN fact_alerts a ON s.event_id=a.event_id
                      WHERE s.run_id=? AND a.rule_id=s.scenario_family''',*params)
    return {'scenario_family_count':families,'detected_scenario_families':triggered,
            'scenario_family_detection_rate':(triggered/families if families else None),
            'transactions':total,'alerts':alerts,'unique_flagged_transactions':unique,'truth_positive_events':positive,
            'detected_truth_positive_events':tp,
            'event_detection_rate':(tp/positive if positive else None),
            'terminal_attempts':terminal,'terminal_failures':terminal_fail,
            'terminal_failure_rate':(terminal_fail/terminal if terminal else None)}
