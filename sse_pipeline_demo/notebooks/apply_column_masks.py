# Databricks notebook source

# MAGIC %md
# MAGIC # Stage 3: Apply Column Masks + Create Serving Views
# MAGIC
# MAGIC **Purpose**: Post-pipeline task that runs after `sse_connect_pipeline` completes.
# MAGIC
# MAGIC Two things happen here:
# MAGIC 1. **Column masks** are attached to all PII columns in every PDV table.
# MAGIC    Unity Catalog calls `pii_decrypt()` at query time for every row — authorized users
# MAGIC    see decrypted plaintext, everyone else sees `***REDACTED***`.
# MAGIC 2. **Serving views** are created as proper Unity Catalog views (not pipeline views),
# MAGIC    joining bronze + PDV. The column mask on PDV applies transparently through the view.
# MAGIC
# MAGIC This notebook is **fully config-driven** — it reads `pipeline_config` to discover
# MAGIC which PDV tables exist and which columns need masks. Adding a new PII source = no code change.

# COMMAND ----------

import json

dbutils.widgets.text("config_table",  "")
dbutils.widgets.text("bronze_schema", "")

config_table  = dbutils.widgets.get("config_table")
bronze_schema = dbutils.widgets.get("bronze_schema")

assert config_table,  "config_table parameter is required"
assert bronze_schema, "bronze_schema parameter is required"

catalog = config_table.split(".")[0]

print(f"Config table  : {config_table}")
print(f"Bronze schema : {bronze_schema}")
print(f"Catalog       : {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read settings from config table

# COMMAND ----------

from pyspark.sql.functions import col

settings_row = spark.table(config_table).filter(col("source_name") == "_settings").first()
settings = dict(settings_row.properties)

config_schema  = config_table.split(".")[1]
mask_fn        = f"{catalog}.{config_schema}.pii_decrypt"

print(f"Mask function : {mask_fn} (created by setup_config.py)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Apply column masks to PDV tables
# MAGIC
# MAGIC The `pii_decrypt` function was already created by `setup_config.py`.
# MAGIC This step just attaches it to the PII columns.

# COMMAND ----------

config_rows = (
    spark.table(config_table)
    .filter(col("source_name") != "_settings")
    .filter(col("is_active") == True)
    .collect()
)

pdv_rows = [r for r in config_rows if dict(r.properties).get("pdv_table")]
print(f"Sources with PDV tables: {[r.source_name for r in pdv_rows]}\n")

for cfg in pdv_rows:
    props    = dict(cfg.properties)
    pii_cols = json.loads(props["pii_columns"])
    pdv_full = f"{catalog}.{bronze_schema}.{props['pdv_table']}"

    for pii_col in pii_cols:
        spark.sql(f"""
            ALTER TABLE {pdv_full}
            ALTER COLUMN {pii_col}
            SET MASK {mask_fn}
        """)
        print(f"  Mask applied: {pdv_full}.{pii_col}")

print("\nAll column masks applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create serving views

# COMMAND ----------

for cfg in pdv_rows:
    props        = dict(cfg.properties)
    pii_cols     = json.loads(props["pii_columns"])
    bronze_full  = f"{catalog}.{bronze_schema}.{props['bronze_table']}"
    pdv_full     = f"{catalog}.{bronze_schema}.{props['pdv_table']}"
    view_full    = f"{catalog}.{bronze_schema}.serving_{cfg.source_name}"
    join_key     = props["join_key"]
    pii_col_list = ", ".join([f"p.{c}" for c in pii_cols])

    spark.sql(f"""
        CREATE OR REPLACE VIEW {view_full}
        COMMENT 'Serving view for {cfg.source_name}: bronze columns + PII columns (masked by UC column mask)'
        AS
        SELECT
          b.*,
          {pii_col_list}
        FROM {bronze_full} b
        JOIN {pdv_full} p ON b.{join_key} = p.{join_key}
    """)
    print(f"  Serving view created: {view_full}")

print("\nAll serving views ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify

# COMMAND ----------

print("=== Summary ===\n")
for cfg in pdv_rows:
    props    = dict(cfg.properties)
    pii_cols = json.loads(props["pii_columns"])
    pdv_full = f"{catalog}.{bronze_schema}.{props['pdv_table']}"
    view_full = f"{catalog}.{bronze_schema}.serving_{cfg.source_name}"
    print(f"Source      : {cfg.source_name}")
    print(f"PDV table   : {pdv_full}")
    print(f"PII cols    : {pii_cols}")
    print(f"Serving view: {view_full}")
    print(f"Mask fn     : {mask_fn}")
    print()

# COMMAND ----------

for cfg in pdv_rows:
    props = dict(cfg.properties)
    view_full = f"{catalog}.{bronze_schema}.serving_{cfg.source_name}"
    print(f"\n=== {view_full} ===")
    display(spark.table(view_full))
