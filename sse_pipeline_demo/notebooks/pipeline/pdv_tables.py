# Databricks notebook source

# MAGIC %md
# MAGIC # Pipeline: Personal Data Vault (AES-GCM encrypted PII)
# MAGIC
# MAGIC **Responsibility**: For sources that have PII (`pdv_table` in properties),
# MAGIC read from `raw_*` temp tables, encrypt each PII column with AES-GCM, and persist
# MAGIC as PDV streaming tables. Column masks are applied post-pipeline by `apply_column_masks.py`.
# MAGIC
# MAGIC **Reuse**: Include only in pipelines that handle PII data.
# MAGIC Omit from pipelines where no sources have PII — no config change needed, just remove this notebook
# MAGIC from the pipeline's library list in `databricks.yml`.
# MAGIC
# MAGIC **Outputs**: `pdv_{table_name}` per PII source — persisted, PII columns AES-GCM encrypted as base64 strings.

# COMMAND ----------

import dlt
import json
from pyspark.sql.functions import col, base64, expr

config_table = spark.conf.get("config_table")

settings = dict(
    spark.table(config_table)
    .filter(col("source_name") == "_settings")
    .first()
    .properties
)
encryption_key = settings["encryption_key"]

config_rows = [
    r for r in (
        spark.table(config_table)
        .filter(col("source_name") != "_settings")
        .filter(col("is_active") == True)
        .collect()
    )
    if dict(r.properties).get("pdv_table")
]

print(f"[pdv] PDV tables to create: {[dict(r.properties)['pdv_table'] for r in config_rows]}")

# COMMAND ----------

for cfg in config_rows:
    source_name = cfg.source_name
    props       = dict(cfg.properties)
    pdv_table   = props["pdv_table"]
    pii_cols    = json.loads(props["pii_columns"])

    @dlt.table(
        name=pdv_table,
        comment=(
            f"PDV {source_name} — PII columns ({', '.join(pii_cols)}) encrypted with AES-GCM. "
            f"Column masks applied post-pipeline by apply_column_masks.py."
        ),
    )
    def create_pdv(src=source_name, pii=pii_cols, key=encryption_key):
        df = dlt.read_stream(f"raw_{src}")
        for pii_col in pii:
            df = df.withColumn(
                pii_col,
                base64(expr(f"aes_encrypt({pii_col}, unhex('{key}'), 'GCM')"))
            )
        return df
