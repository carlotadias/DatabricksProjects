# Databricks notebook source

# MAGIC %md
# MAGIC # Pipeline: Ingest (Raw → Decrypted + Parsed)
# MAGIC
# MAGIC **Responsibility**: Read AES-GCM encrypted files from the Raw layer, decrypt in-memory,
# MAGIC parse JSON using the schema from `pipeline_config.properties`, and expose a temporary
# MAGIC `raw_*` table per source for downstream notebooks in the same pipeline.
# MAGIC
# MAGIC **Reuse**: Include this notebook in any pipeline that reads from the encrypted Raw layer.
# MAGIC Pair with `bronze_tables.py` (always), `pdv_tables.py` (if PII present), `derived_joins.py` (if joins needed).
# MAGIC
# MAGIC **Outputs**: `raw_{source_name}` — temporary, not persisted to storage.

# COMMAND ----------

import dlt
import json
from pyspark.sql.functions import col, expr, from_json, current_timestamp, explode, split, trim
from pyspark.sql.types import StructType

config_table = spark.conf.get("config_table")

settings = dict(
    spark.table(config_table)
    .filter(col("source_name") == "_settings")
    .first()
    .properties
)
encryption_key = settings["encryption_key"]

config_rows = (
    spark.table(config_table)
    .filter(col("source_name") != "_settings")
    .filter(col("is_active") == True)
    .collect()
)

print(f"[ingest] Sources to ingest: {[r.source_name for r in config_rows]}")

# COMMAND ----------

for cfg in config_rows:
    source_name = cfg.source_name
    props       = dict(cfg.properties)
    raw_path    = props["raw_path"]
    schema      = StructType.fromDDL(props["schema"])

    @dlt.table(
        name=f"raw_{source_name}",
        comment=f"Temporary: AES-GCM decrypted + JSON-parsed {source_name}. Not persisted to storage.",
        temporary=True,
    )
    def create_raw(raw_path=raw_path, schema=schema, key=encryption_key):
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.partitionColumns", "")
            .option("rescuedDataColumn", "_rescue")
            .load(raw_path)
            .withColumn(
                "decrypted_json",
                expr(f"CAST(aes_decrypt(unbase64(encrypted_content), unhex('{key}'), 'GCM') AS STRING)")
            )
            # Each encrypted blob is one original JSONL file (multiple lines).
            # Split into individual records so from_json can parse each one.
            .withColumn("json_line", explode(split(col("decrypted_json"), "\n")))
            .filter(trim(col("json_line")) != "")
            .withColumn("parsed", from_json(col("json_line"), schema))
            .select("parsed.*", col("source_file"), col("source_modified"), "_rescue")
            .withColumn("_ingested_at", current_timestamp())
        )
