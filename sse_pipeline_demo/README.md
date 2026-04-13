# SSE Metadata-Driven Pipeline Demo

## Overview

This demo modernises SSE's current pipeline — replacing Azure Data Factory (ADF), Azure SQL MetaDB, and custom notebooks — with a **metadata-driven, config-first approach** running natively on Databricks.

All config lives in a single YAML file (`conf/sources.yml`). A generic setup notebook loads it into a Delta table with a **flexible MAP schema** — `source_name STRING, is_active BOOLEAN, properties MAP<STRING, STRING>`. The table never needs ALTER TABLE when a new pipeline requires new properties.

Sources share the same storage container but each declares its own **prefix** (e.g. `amazon_connect/contact_records`). The setup notebook assembles full paths as `staging_root/prefix` and `raw_root/prefix` — mirroring real S3/ADLS layouts.

`databricks.yml` only has 3 environment-level variables (`catalog`, `config_schema`, `bronze_schema`). Everything else flows from the YAML → Delta table → notebooks.

Open `docs/explainer.html` in a browser for a visual walkthrough with architecture diagrams.

## Project Structure

```
sse_pipeline_demo/
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
    inspect_pipeline_health.py   # Run after pipeline: event logs, rescued data, file audit
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
| `volume`, `staging_root`, `raw_root`, `ad_group`, `encryption_key` | `sources.yml` → settings | Pipeline config — same across environments |
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
| `pipeline/derived_joins.py` | **No** | Domain-specific joins (Amazon Connect). Copy + adapt per domain. |
| `generate_sample_data.py` | **No** | Demo only — simulates MFT. Not deployed in production. |

## What Gets Eliminated

| Current Component                      | Replacement                          | Benefit                                |
|----------------------------------------|--------------------------------------|----------------------------------------|
| Azure SQL MetaDB (3 tables)            | 1 Delta config table (flexible MAP)  | Git-controlled, extensible, no external DB |
| Ingested Dataset Table (file tracking) | Auto Loader checkpoints              | Automatic, exact-once, zero code       |
| Audit Table (success/failure)          | Pipeline event log + `_metadata` columns | Built-in, queryable, richer         |
| ADF orchestration                      | Lakeflow Job (3 tasks)               | Native Databricks                      |
| Custom notebook per source             | Generic reusable notebooks           | Config-driven — no code changes        |
| SHA-2 hashing in PDV                   | AES-GCM encryption (reversible)      | Authorised users can decrypt at query  |
| CASE expressions for PII access        | UC Column Masks                      | Engine-enforced, tamper-proof          |
| Manual deployment                      | `databricks bundle deploy`           | One command, reproducible              |

## How to Run

```bash
# Deploy the bundle
databricks bundle deploy --profile=tko-day3

# Generate sample data (demo only — simulates MFT dropping files)
# Run the generate_sample_data.py notebook manually from the workspace

# Run the full pipeline (load config → encrypt → pipeline → column masks)
# First run creates the config table; subsequent runs skip setup automatically.
databricks bundle run sse_pipeline --profile=tko-day3
```

The `load_config` task checks if the config table already exists. If it does, setup is skipped entirely (no pip install, no cleanup, no writes). To force a reload after editing `sources.yml`, set `force_refresh` to `true` on the job run.

## Adding a New Source (what a developer does)

1. Add a block to `conf/sources.yml` with a `prefix` for the storage path
2. Re-deploy: `databricks bundle deploy`
3. Run the pipeline with `force_refresh=true` to reload config from the updated YAML
4. Subsequent runs skip setup automatically
5. **No notebook code changes. No table schema changes.**

## Fault Tolerance

If a file arrives malformed or with the wrong schema:

- **Encryption step**: each source is processed independently — one failure doesn't block the others
- **Pipeline**: `rescuedDataColumn` captures rows that don't match the schema (not lost, just flagged)
- **Bronze**: `expect_or_drop` filters rows with NULL primary keys (parsing failures)
- **Event log**: all expectation pass/fail counts are logged per table, per run
- **Restart**: re-run the pipeline — Auto Loader checkpoints resume from the exact file that failed

Run `inspect_pipeline_health.py` after the pipeline to see event logs, rescued data, dropped rows, and file-level audit.

To test: run `generate_sample_data.py` with `include_bad_data=true`, then run the pipeline and inspect.

## Creating a Whole New Domain Pipeline

1. Write a new `sources.yml` for the domain (including settings)
2. In `databricks.yml`, add a new pipeline — pick which pipeline notebooks to include
3. Copy and adapt `derived_joins.py` if the new domain has cross-source joins
4. Deploy — all generic notebooks are reused as-is
