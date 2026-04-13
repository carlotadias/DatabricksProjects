# Databricks notebook source

# MAGIC %md
# MAGIC # Stage 1: Staging → Raw (File Encryption)
# MAGIC
# MAGIC **Purpose**: Auto Loader watches staging folders, reads new JSON files as binary, encrypts
# MAGIC the entire file content using AES-GCM, and writes the encrypted payload to the Raw volume.
# MAGIC
# MAGIC This notebook is **generic** — it reads the `pipeline_config` Delta table and processes
# MAGIC whichever sources are active. Encryption key comes from the `_settings` row.
# MAGIC
# MAGIC **Replaces**: The ADF for-each loop + Azure SQL Ingested Dataset Table.

# COMMAND ----------

from pyspark.sql.functions import col, base64, expr

dbutils.widgets.text("config_table", "")
config_table = dbutils.widgets.get("config_table")
assert config_table, "config_table parameter is required"

print(f"Config table: {config_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load settings and source config

# COMMAND ----------

settings_row = spark.table(config_table).filter(col("source_name") == "_settings").first()
settings = dict(settings_row.properties)
encryption_key = settings["encryption_key"]

sources = (
    spark.table(config_table)
    .filter(col("source_name") != "_settings")
    .filter(col("is_active") == True)
    .collect()
)

print(f"Encryption key: {'*' * 28}{encryption_key[-4:]}")
print(f"{len(sources)} active source(s) to encrypt:")
for s in sources:
    props = dict(s.properties)
    print(f"  - {s.source_name}: {props['staging_path']} → {props['raw_path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encrypt staging files → Raw
# MAGIC
# MAGIC For each source:
# MAGIC 1. Auto Loader detects new files (binaryFile format → reads entire file as bytes)
# MAGIC 2. AES-GCM encrypt the content
# MAGIC 3. Write encrypted file to raw volume
# MAGIC
# MAGIC Auto Loader checkpoints ensure each file is processed **exactly once** — this is the
# MAGIC mechanism that replaces the Azure SQL Ingested Dataset Table.

# COMMAND ----------

streams = []
failed_sources = []

for source in sources:
    props = dict(source.properties)
    source_name   = source.source_name
    staging_path  = props["staging_path"]
    raw_path      = props["raw_path"]
    checkpoint_path = f"{raw_path}/_checkpoint"

    print(f"\n--- Encrypting: {source_name} ---")
    print(f"  Staging : {staging_path}")
    print(f"  Raw     : {raw_path}")
    print(f"  Ckpt    : {checkpoint_path}")

    try:
        stream = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.partitionColumns", "")
            .load(staging_path)
            .withColumn(
                "encrypted_content",
                base64(
                    expr(f"aes_encrypt(content, unhex('{encryption_key}'), 'GCM')")
                )
            )
            .select(
                col("path").alias("source_file"),
                col("modificationTime").alias("source_modified"),
                col("length").alias("source_size_bytes"),
                "encrypted_content"
            )
            .writeStream
            .format("json")
            .option("checkpointLocation", checkpoint_path)
            .option("path", raw_path)
            .trigger(availableNow=True)
            .queryName(f"encrypt_{source_name}")
            .start()
        )
        streams.append((source_name, stream))
        print(f"  Stream started: {stream.name}")
    except Exception as e:
        failed_sources.append((source_name, str(e)))
        print(f"  FAILED to start stream: {e}")

# COMMAND ----------

for source_name, stream in streams:
    try:
        stream.awaitTermination()
        if stream.exception():
            raise stream.exception()
        print(f"  {source_name}: encryption complete")
    except Exception as e:
        failed_sources.append((source_name, str(e)))
        print(f"  {source_name}: FAILED — {e}")

if failed_sources:
    print(f"\nWARNING: {len(failed_sources)} source(s) failed encryption:")
    for name, err in failed_sources:
        print(f"  - {name}: {err}")
    print("Successful sources will continue through the pipeline.")
    print("Fix the issue and re-run — Auto Loader checkpoints will resume from the failed point.")
else:
    print("\nAll sources encrypted successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: Raw files exist

# COMMAND ----------

for source in sources:
    props = dict(source.properties)
    raw_path = props["raw_path"]
    source_name = source.source_name
    try:
        files = [f for f in dbutils.fs.ls(raw_path) if not f.name.startswith("_")]
        print(f"{source_name}: {len(files)} encrypted file(s) in {raw_path}")
        for f in files:
            print(f"  {f.name} ({f.size} bytes)")
    except Exception as e:
        print(f"{source_name}: No files yet — {e}")
