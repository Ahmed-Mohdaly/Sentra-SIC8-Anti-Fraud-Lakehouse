# ADR-0002: Ground truth isolation — ACCEPT AS SAFETY GATE

R1 simulation labels and IBM `Is Fraud?` are source truth; never appear in rule/ML input. Store truth in separate evaluation-only table / sidecar keyed by run + event; alerts/predictions in distinct facts, never confuse an alert with truth. Tests under `tests/contracts` and `tests/r2` enforce local isolation. Historic time remains timezone unknown, precise GPS absent. Changes need review by R1 R2 R3.
