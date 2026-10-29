# Proposed Removal Candidates: Synthetic Transaction Data Provider
**Inventory of Obsolete, Dead, and Redundant Artifacts**  
**Date:** October 25, 2026  
**Auditor Roles:** Senior Data Engineer, Python Backend Architect, DevOps Engineer, Code Quality Auditor  
**Workspace:** `/home/syd/Downloads/archive`  
**Status:** Audit Inventory Complete — Read-Only (Pending Approval Gate)

---

## 1. Executive Summary

During the repository audit, multiple components, files, configurations, and dependencies were identified that either directly violate the target architecture, introduce critical runtime failures, or represent obsolete remnants of previous iterations (such as Kafka streaming, distributed worker daemons, Azure cloud templates, and unversioned data artifacts).

Per the Rules of Engagement:
> **Do not delete anything during the audit.**  
> Propose removals with evidence, import relationships, risk assessments, and verification steps. Wait for explicit approval before changing application code or deleting modules.

This document compiles the evidence-based inventory of all proposed removals.

---

## 2. Inventory of Proposed Removals

| Item / Path | Category | Size / Lines | Evidence of Non-use / Problem | References Discovered | Risk Level | Proposed Action / Replacement | Verification Steps |
|---|---|---|---|---|---|---|---|
| `src/synthetic_streaming/kafka_publisher.py` | Legacy Module | 325 lines | Top-level unconditional `import confluent_kafka` crashes the production Docker container. Downstream consumers extract via REST API and SQL, not Kafka. | `generator_service.py`, `scripts/run_generator.py`, `tests/unit/test_publisher.py` | Low | Remove or isolate behind optional dynamic import if Kafka is not a supported target. | Verify provider image boots and passes all unit tests without `confluent-kafka` installed. |
| `src/synthetic_streaming/worker.py` | Legacy Architecture | 144 lines | Implements an obsolete internal HTTP daemon on port 8001. In `api.py`, batch generation via worker proxy is not implemented (returns HTTP 501). | `scripts/run_generator.py`, `api.py` (worker proxy branches), `tests/unit/test_worker.py` | Low | Remove. Consolidate provider into the single API container per Target Architecture Section 1. | Verify all operational routes (/health, /ready, /status, /generate) operate cleanly without worker proxy. |
| `synthetic_transaction_streaming_docs/` | Duplicate Directory | ~100 KB (6 files) | Complete duplicate of `docs/` containing stale architecture documents referencing removed Kafka/Spark/MinIO/Iceberg stack. | None from source code; internal docs only. | Zero | Delete folder completely. Consolidate all documentation under `docs/`. | Verify no scripts or tests reference `synthetic_transaction_streaming_docs`. |
| `docker/docker-compose.yml` | Duplicate Configuration | 22 lines | Duplicate of root `compose.yaml`. Only defines `db` with a different volume name (`postgres-dev-data`), causing confusion with root `compose.yaml`. | None in scripts. | Zero | Delete `docker/docker-compose.yml`. Standardize on root `compose.yaml`. | Run `docker compose config` on root `compose.yaml` to verify validity. |
| `infra/main.bicep` & parameters | Obsolete Infrastructure | ~350 lines | Azure Bicep template deploying Azure Container Apps, Event Hubs, and Log Analytics. The target architecture is AWS (ECS Fargate + RDS). | None in application code. | Zero | Propose removal or archive, replacing with AWS deployment specifications in `docs/ARCHITECTURE.md`. | Confirm zero build or test references. |
| `generator.db` (root) | Accidental SQLite Artifact | 1.8 MB | Active SQLite database containing 136 transactions and 2,003 users accidentally committed/left in root. | Mentioned in `config.py` default. | Low | Delete file and add `*.db` and `*.sqlite` to `.gitignore`. | Verify tests use temporary isolated databases via `tmp_path`. |
| `credit_card_transactions-ibm_v2.csv` | Accidental Data Artifact | **2.35 GB** | Massive historical transactions CSV file left in repository root. Untracked by `.gitignore`. `BaselineLoader` already samples from `User0_credit_card_transactions.csv` (1.9 MB). | `data_profile.py`, `baseline_loader.py` fallback | Low | Remove from repo root or add to `.gitignore`. Retain lightweight `User0_credit_card_transactions.csv` for merchant sampling. | Run `tests/unit/test_baseline_bootstrap.py` and `test_generators.py` to confirm sampling functions without 2.35 GB file. |
| `load_results_live_kafka.json` | Leftover Benchmark Artifact | 1 KB | Static benchmark output from legacy Kafka load run. | None. | Zero | Delete file and add `load_results_*.json` to `.gitignore`. | Repository hygiene check. |
| `DATA_ENHANCEMENT_REPORT.docx` | Generated Binary Artifact | 47 KB | Binary Word document generated from markdown report. Binary artifacts should not be committed. | None. | Zero | Delete binary file. The source report exists as `docs/DATA_ENHANCEMENT_REPORT.md`. | Confirm markdown documentation remains intact. |
| `scripts/generate_docx_report.py` | Obsolete Script | 282 lines | One-off script relying on `python-docx` (not listed in `pyproject.toml` dependencies). Fails if run in clean environment. | None. | Zero | Delete script or move to scratch. | Confirm `pyproject.toml` has no missing dependency. |
| `tests/integration/test_kafka_integration.py` | Obsolete Test | 65 lines | Requires a live Kafka broker on `localhost:9092`. Always skipped in local and CI environments. | None. | Zero | Remove test once Kafka streaming publisher is decoupled/retired. | Run `pytest` to confirm suite is 100% green without skips. |
| `tests/unit/test_api_distributed.py` & `test_worker.py` | Obsolete Tests | 119 lines | Tests testing the deprecated worker proxy over HTTP port 8001. | None. | Zero | Retire tests alongside `worker.py` deprecation. | Run full unit test suite. |

---

## 3. Detailed Evidence for Key Removals

### 3.1. Kafka Publisher (`src/synthetic_streaming/kafka_publisher.py`)
- **Direct Code References in `src/`:**
  - `src/synthetic_streaming/generator_service.py` lines 31–35, 95, 98, 100.
  - `scripts/run_generator.py` line 20.
  - `src/synthetic_streaming/worker.py` line 38.
- **Why It Must Be Removed / Decoupled:**
  Line 11 has an unhandled top-level import: `import confluent_kafka`. In the target architecture:
  > Downstream consumers extract transactions via REST API (Source A) or direct SQL access to PostgreSQL (Source B). Direct Kafka streaming is not part of the source provider requirements.
  Because `confluent_kafka` is not installed in the production Docker image, the image cannot boot.
- **Risk Assessment:**
  Low risk. The REST API serves persisted transactions from PostgreSQL. Removing Kafka from the core startup path restores container bootability.

### 3.2. Worker Daemon (`src/synthetic_streaming/worker.py`)
- **Direct Code References in `src/`:**
  - `src/synthetic_streaming/api.py` lines 141, 190, 199, 212, 398, 429, 454, 473, 485.
  - `scripts/run_generator.py` line 21, 90–95.
- **Why It Must Be Removed:**
  Target Architecture Section 1 mandates:
  > The application container contains only the synthetic data provider application... One application image for the provider.
  Having a separate worker process on port 8001 communicating with the API on port 8000 via internal HTTP proxy introduces network fragility, duplicate endpoints, and half-implemented proxy logic (e.g. `POST /api/v1/generator/generate` is not implemented for the worker and returns HTTP 501).
- **Risk Assessment:**
  Low risk. Consolidating the generator service directly into the API application simplifies runtime configuration and makes batch generation reliable.

### 3.3. 2.35 GB CSV File (`credit_card_transactions-ibm_v2.csv`)
- **Direct Code References in `src/`:**
  - `src/synthetic_streaming/baseline_loader.py` line 154:
    ```python
    tx_file = self.data_dir / "User0_credit_card_transactions.csv"
    if not tx_file.exists():
        tx_file = self.data_dir / "credit_card_transactions-ibm_v2.csv"
    ```
- **Why It Must Be Removed from Git Context:**
  `BaselineLoader` explicitly checks for `User0_credit_card_transactions.csv` (1.9 MB) first. The 2.35 GB file is never loaded into memory (it would cause out-of-memory errors on small container instances). Keeping a 2.35 GB file untracked in the root directory balloons image build contexts and violates repository hygiene.
- **Risk Assessment:**
  Zero risk. `User0_credit_card_transactions.csv` provides identical schema and sufficient empirical merchant samples.

---

## 4. Approval Gate Recommendation

The items listed above are ready for safe deprecation and removal in **Phase 7** of the Refactoring Plan, only after Phases 1–6 (crash fixes, persistence atomicity, and API hardening) are implemented and verified.
