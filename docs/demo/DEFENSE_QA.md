# Viva Q&A quick reference

**Batch vs true stream?** Historical CSV batch reads finite snapshots; Kafka publishes new messages continuously. S3 file-arrival Structured Streaming is micro-batch file ingestion with sink latency; not direct Kafka subsecond scoring.

**At least once vs exactly once?** Kafka/S3 sink may duplicate; stable event IDs + idempotent Delta MERGE recover logical uniqueness but do not erase raw attempt evidence. Exactly-once end-to-end needs proven contracts and test results.

**Ground truth vs prediction?** IBM `Is Fraud?` and simulation scenario sidecar are source labels; fraud rules/LightGBM must not consume them. Alerts/predictions are outputs, Precision/Recall evaluation join happens later.

**Velocity?** Same actual account, approved positive attempts in event-time `(t-10m,t]`; dedup and late events matter. Historical `User` alone not a verified account ID.

**GPS?** City centroid is approximate reference, not transaction GPS. Only exact synthetic location and UTC permit impossible travel.

**SCD2?** CDC source positions and valid_from/valid_to create versioned dimensions; look up attributes valid at transaction event time, no overlapping versions. Local helper does not establish live Delta SCD2.

**Model metrics?** Historical LR/GBT exploratory baselines and R1 reported LightGBM metrics use different periods and evidence. Don't compare AUC/recall as equal or claim model production ready without real artifacts and feature leakage audits.

**Security?** No PAN/CVV, scoped permissions, token hygiene and governed SELECT views. Offline token guard is not enterprise authentication.

**Genie?** Databricks Genie needs correct entitlement, UC access, SQL Warehouse and governed metric views. Current local endpoint honestly returns Not Configured.

**Costs?** Verify AWS MSK/ECS/RDS active resources and Databricks compute/billing before reactivating; live streaming runs can incur ongoing cost.
