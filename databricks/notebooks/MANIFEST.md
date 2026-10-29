# Databricks Notebooks Manifest

This directory contains the production-grade historical and streaming lakehouse notebooks for Databricks.

## Historical Medallion Notebooks (`notebooks/historical/`)

| Notebook | Purpose | Status |
|---|---|---|
| `01_SIC8_Historical_Source_Profiling_v1_1.ipynb` | Source profiling, schema inference, and data distributions | Production Ready |
| `01_Source_Exploration.ipynb` | Historical dataset exploration & profiling | Reference |
| `02_SIC8_Historical_Canonical_Mapping.ipynb` | Canonical schema mapping and type normalization | Production Ready |
| `03_SIC8_Historical_Silver_Processing.ipynb` | Silver layer ingestion, cleaning, deduplication | Production Ready |
| `03.1_SIC8_Historical_Silver_Ready_to_Publish_v1_0.ipynb` | Silver Delta publication ready pipeline | Production Ready |
| `04_SIC8_Historical_Fraud_Detection.ipynb` | Fraud feature engineering and baseline scoring | Production Ready |
| `04.1_SIC8_Historical_Fraud_Detection_Full_Silver_v1_0.ipynb` | Full Silver fraud detection pipeline | Production Ready |
| `SIC8_01_Historical_Source_Profiling.ipynb` | Comprehensive source profiling | Production Ready |
| `SIC8_05_Historical_ML_Baseline_Option_C_v1.ipynb` | Historical ML baseline (Option C) | Production Ready |
| `Untitled-Copy1.ipynb` | Original user-provided exploratory notebook | Preserved Historical Reference |

## Streaming Lakehouse Notebooks (`notebooks/streaming/`)

| Notebook / Job | Purpose | Status |
|---|---|---|
| `08_Continuous_End_to_End_Streaming.ipynb` | Continuous end-to-end streaming ingestion & processing | Production Ready |
| `VALIDSIC8_08A_Streaming_S3_New_Files_AutoLoader_v3_1_Parser_Fix.ipynb` | S3 AutoLoader streaming parser with v3.1 envelope fixes | Production Ready |
| `VALIDSIC8_08B_Streaming_Silver_Sayed_Contract_v2_1_Refund_Fix.ipynb` | Streaming Silver processing with Sayed contract v2.1 & refund handling | Production Ready |
| `08C_Streaming_DLQ_Remediation_and_Redrive.ipynb` | Dead-Letter Queue remediation, quarantine inspection & redrive | Production Ready |
| `sic8_continuous_streaming_job.json` | Databricks Job definition for continuous streaming cluster execution | Production Ready |
