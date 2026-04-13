# Metadata-Driven Pipeline — Databricks on Azure

## Overview

This project demonstrates a **metadata-driven, config-first pipeline** running natively on Databricks, replacing Azure Data Factory (ADF), Azure SQL MetaDB, and custom notebooks.

All config lives in a single YAML file (`conf/sources.yml`). A generic setup notebook loads it into a Delta table with a **flexible MAP schema** — `source_name STRING, is_active BOOLEAN, properties MAP<STRING, STRING>`. The table never needs ALTER TABLE when a new pipeline requires new properties.

Sources share the same ADLS container but each declares its own **prefix** (e.g. `domain/source_name`). The setup notebook assembles full paths as `staging_root/prefix` and `raw_root/prefix`.

`databricks.yml` only has 3 environment-level variables (`catalog`, `config_schema`, `bronze_schema`). Everything else flows from the YAML → Delta table → notebooks.

Open `docs/explainer.html` in a browser for a visual walkthrough with architecture diagrams.

## Project Structure

```
metadata_driven_pipeline/
  databricks.yml                 # DABs bundle — 3 env variables, pipeline, job
  conf/
    sources.yml                  # All config: settings + sources → Delta table
  docs/
    explainer.html               # Self-contained architecture explainer
  notebooks/
    setup_config.py              # Generic: YAML → config table + mask fn
    generate_sample_data.py      # Demo only: simulates MFT file drops
    encrypt_staging_to_raw.py    # Task 1: AES-GCM file-level encryption
    apply_column_masks.py        # Task 3: UC column masks + serving views
    inspect_pipeline_health.py   # Run after pipeline: event logs, file audit
    pipeline/
      ingest_and_decrypt.py      # Auto Loader + AES-GCM decrypt + parse
      bronze_tables.py           # Non-PII columns + data quality
      pdv_tables.py              # PII columns encrypted in-table
      derived_joins.py           # Cross-source join tables
```

### What lives where

| Setting | Where | Why |
|---------|-------|-----|
| `catalog`, `config_schema`, `bronze_schema` | `databricks.yml` | Environment-level — changes between dev/prod |
| `staging_root`, `raw_root`, `ad_group`, `encryption_key` | `sources.yml` → settings | Pipeline config — ADLS paths, encryption, access control |
| `prefix`, `schema`, `bronze_columns`, `pii_columns`, ... | `sources.yml` → sources | Source-specific — any key-value pair, extensible |

### What is generic vs. domain-specific

| Notebook | Generic? | Notes |
|----------|----------|-------|
| `setup_config.py` | Yes | Reads any `sources.yml`, creates MAP-based config table. Never needs editing. |
| `encrypt_staging_to_raw.py` | Yes | Reads config table, processes all active sources. |
| `pipeline/ingest_and_decrypt.py` | Yes | Schema from config MAP — no hardcoded columns. |
| `pipeline/bronze_tables.py` | Yes | Column selection from config MAP. |
| `pipeline/pdv_tables.py` | Yes | PII columns from config MAP. |
| `apply_column_masks.py` | Yes | Discovers PDV tables and PII columns from config MAP. |
| `pipeline/derived_joins.py` | **No** | Domain-specific joins. Copy + adapt per domain. |
| `generate_sample_data.py` | **No** | Demo only — simulates MFT. Not deployed in production. |

## What Gets Eliminated

| Current Component                      | Replacement                          | Benefit                                |
|----------------------------------------|--------------------------------------|----------------------------------------|
| Azure SQL MetaDB (3 tables)            | 1 Delta config table (flexible MAP)  | Git-controlled, extensible, no external DB |
| Ingested Dataset Table (file tracking) | Auto Loader checkpoints              | Automatic, exact-once, zero code       |
| Audit Table (success/failure)          | Pipeline event log + `_metadata` columns | Built-in, queryable, richer         |
| ADF orchestration                      | Lakeflow Job (3-4 tasks)             | Native Databricks, serverless          |
| Custom notebook per source             | Generic reusable notebooks           | Config-driven — no code changes        |
| SHA-2 hashing in PDV                   | AES-GCM encryption (reversible)      | Authorised users can decrypt at query  |
| CASE expressions for PII access        | UC Column Masks                      | Engine-enforced, tamper-proof          |
| Manual deployment                      | `databricks bundle deploy`           | One command, reproducible              |

## How to Run

```bash
# Deploy the bundle
databricks bundle deploy -t dev

# Generate sample data (demo only — simulates MFT dropping files)
# Run generate_sample_data.py manually from the workspace

# Run the full pipeline (load config → encrypt → Lakeflow → column masks)
databricks bundle run my_pipeline -t dev
```

The `load_config` task checks if the config table already exists. If it does, setup is skipped (no pip install, no cleanup, no writes). To force a reload after editing `sources.yml`, set `force_refresh` to `true` on the job run.

## Adding a New Source

1. Add a block to `conf/sources.yml` with a `prefix` for the ADLS path
2. Re-deploy: `databricks bundle deploy`
3. Run the pipeline with `force_refresh=true` to reload config
4. **No notebook code changes. No table schema changes.**

## Creating a New Domain Pipeline

1. Create a new folder with a new `databricks.yml` and `conf/sources.yml`
2. Copy the generic notebooks (or reference shared ones)
3. In `databricks.yml`, pick which pipeline notebooks to include (e.g. skip `pdv_tables.py` if no PII)
4. Deploy: `databricks bundle deploy -t dev`

See the `smart_meter_demo/` folder for a minimal example.

## Fault Tolerance

If a file arrives malformed or with the wrong schema:

- **Encryption step**: each source is processed independently — one failure doesn't block the others
- **Lakeflow pipeline**: `rescuedDataColumn` captures rows that don't match the schema
- **Bronze**: `expect_or_drop` filters rows with NULL primary keys (parsing failures)
- **Event log**: all expectation pass/fail counts are logged per table, per run
- **Restart**: re-run the pipeline — Auto Loader checkpoints resume from where it left off

Run `inspect_pipeline_health.py` after the pipeline to see event logs, dropped rows, and file-level audit.

## Databricks Documentation (Azure)

### Core Concepts

| Topic | Link |
|-------|------|
| **Auto Loader** — incremental file ingestion | https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/ |
| **Lakeflow Declarative Pipelines** (formerly DLT) | https://learn.microsoft.com/en-us/azure/databricks/lakeflow-pipelines/ |
| **Databricks Asset Bundles (DABs)** | https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/ |
| **Unity Catalog** | https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/ |
| **Lakeflow Jobs** — workflow orchestration | https://learn.microsoft.com/en-us/azure/databricks/jobs/ |

### Security & PII

| Topic | Link |
|-------|------|
| **Column Masks** — row/column level security | https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/row-and-column-filters/ |
| **Databricks Secrets** (backed by Azure Key Vault) | https://learn.microsoft.com/en-us/azure/databricks/security/secrets/ |
| **Azure Key Vault-backed secret scopes** | https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#azure-key-vault-backed-scopes |
| **is_account_group_member()** — AD group checks | https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/is_account_group_member |
| **AES encryption functions** | https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/aes_encrypt |

### Auto Loader Deep Dive

| Topic | Link |
|-------|------|
| **Auto Loader options reference** | https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options |
| **Auto Loader schema evolution** | https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema |
| **cloudFiles.format options** (json, csv, binary, etc.) | https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options#file-format-options |
| **Rescued data column** — handling schema mismatches | https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema#rescued-data-column |

### Lakeflow Pipelines

| Topic | Link |
|-------|------|
| **Data quality expectations** (expect, expect_or_drop) | https://learn.microsoft.com/en-us/azure/databricks/lakeflow-pipelines/expectations |
| **Pipeline event log** — monitoring and audit | https://learn.microsoft.com/en-us/azure/databricks/lakeflow-pipelines/observability |
| **event_log() table function** | https://learn.microsoft.com/en-us/azure/databricks/lakeflow-pipelines/observability#query-the-event-log |
| **Full refresh vs incremental** | https://learn.microsoft.com/en-us/azure/databricks/lakeflow-pipelines/updates#--full-refresh |

### DABs & Deployment

| Topic | Link |
|-------|------|
| **Bundle configuration reference** | https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings |
| **Variables in bundles** | https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings#variables |
| **Targets (dev/prod environments)** | https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings#targets |
| **CI/CD with bundles** | https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/ci-cd |

### Storage & Connectivity

| Topic | Link |
|-------|------|
| **Access ADLS from Databricks** | https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage |
| **Unity Catalog external locations** | https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-external-locations |
| **Volumes** — managed file storage in UC | https://learn.microsoft.com/en-us/azure/databricks/volumes/ |
