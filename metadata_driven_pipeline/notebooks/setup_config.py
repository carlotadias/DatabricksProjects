# Databricks notebook source

# MAGIC %md
# MAGIC # Setup: Load Pipeline Config from YAML
# MAGIC
# MAGIC **This notebook is generic infrastructure.** It reads `conf/sources.yml` and loads
# MAGIC everything into a `pipeline_config` Delta table with a flexible schema:
# MAGIC
# MAGIC ```
# MAGIC source_name  STRING              -- unique key ("_settings" for pipeline-level config)
# MAGIC is_active    BOOLEAN             -- enable/disable without deleting
# MAGIC properties   MAP<STRING, STRING> -- all config as key-value pairs
# MAGIC ```
# MAGIC
# MAGIC The MAP column means the table never needs ALTER TABLE when a new pipeline
# MAGIC requires additional properties. Just add the key to the YAML.
# MAGIC
# MAGIC Sources share the same storage container but each has its own prefix —
# MAGIC mirroring real-world S3/ADLS layouts.  Full paths (staging_root/prefix,
# MAGIC raw_root/prefix) are assembled here and stored in the properties MAP.
# MAGIC
# MAGIC Only two parameters come from `databricks.yml` (environment-level):
# MAGIC **catalog** and **config_schema**. Everything else comes from the YAML.

# COMMAND ----------

dbutils.widgets.text("catalog",        "")
dbutils.widgets.text("config_schema",  "")
dbutils.widgets.text("bronze_schema",  "")
dbutils.widgets.text("force_refresh",  "false")
dbutils.widgets.text("config_file",    "sources.yml")

catalog        = dbutils.widgets.get("catalog")
config_schema  = dbutils.widgets.get("config_schema")
bronze_schema  = dbutils.widgets.get("bronze_schema")
force_refresh  = dbutils.widgets.get("force_refresh").strip().lower() == "true"

assert catalog,       "catalog parameter is required"
assert config_schema, "config_schema parameter is required"
assert bronze_schema, "bronze_schema parameter is required"

print(f"Catalog       : {catalog}")
print(f"Config schema : {config_schema}")
print(f"Force refresh : {force_refresh}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Early exit — skip if config table already exists
# MAGIC
# MAGIC On normal pipeline runs the config table is already populated.
# MAGIC Pass `force_refresh=true` after editing `sources.yml` to reload.

# COMMAND ----------

config_table = f"{catalog}.{config_schema}.pipeline_config"

if not force_refresh and spark.catalog.tableExists(config_table):
    row_count = spark.table(config_table).count()
    print(f"Config table {config_table} already exists ({row_count} rows) — skipping setup.")
    print("Pass force_refresh=true to reload config from YAML.")
    dbutils.notebook.exit(f"SKIPPED — config table already exists ({row_count} rows).")

print(f"Config table not found or force_refresh=true — running full setup.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Locate and read sources.yml

# COMMAND ----------

# MAGIC %pip install pyyaml -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import yaml, json, os

catalog       = dbutils.widgets.get("catalog")
config_schema = dbutils.widgets.get("config_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
bundle_root = os.path.dirname(os.path.dirname(notebook_path))

config_file = dbutils.widgets.get("config_file")
yaml_candidates = [
    f"/Workspace{bundle_root}/conf/{config_file}",
]

config_yaml = None
for path in yaml_candidates:
    try:
        with open(path, "r") as f:
            config_yaml = yaml.safe_load(f)
            print(f"Loaded config from: {path}")
            break
    except FileNotFoundError:
        continue

if config_yaml is None:
    raise FileNotFoundError(
        f"sources.yml not found. Looked in: {yaml_candidates}. "
        "Make sure the bundle is deployed."
    )

settings = config_yaml.get("settings", {})
sources  = config_yaml.get("sources", [])

volume = settings.get("volume", "pipeline_data")
volume_path = f"/Volumes/{catalog}/{config_schema}/{volume}"

template_vars = {"${catalog}": catalog, "${config_schema}": config_schema}
staging_root = settings.get("staging_root", f"{volume_path}/staging")
raw_root     = settings.get("raw_root", f"{volume_path}/raw")
for k, v in template_vars.items():
    staging_root = staging_root.replace(k, v)
    raw_root     = raw_root.replace(k, v)

print(f"Volume       : {volume_path}")
print(f"Staging root : {staging_root}")
print(f"Raw root     : {raw_root}")
print(f"Settings     : {list(settings.keys())}")
print(f"Sources      : {[s['name'] for s in sources]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create schema, volume, and config table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{config_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{config_schema}.{volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2a — Clean previous run artifacts
# MAGIC
# MAGIC Drop old tables/views in the bronze schema, clear raw files and checkpoints
# MAGIC so Auto Loader reprocesses everything from staging.

# COMMAND ----------

try:
    existing_tables = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {catalog}.{bronze_schema}").collect()
    ]
    for t in existing_tables:
        full_name = f"{catalog}.{bronze_schema}.{t}"
        obj_type = "VIEW" if "serving_" in t else "TABLE"
        spark.sql(f"DROP {obj_type} IF EXISTS {full_name}")
        print(f"  Dropped {obj_type.lower()}: {full_name}")
except Exception as e:
    print(f"  Bronze schema cleanup skipped: {e}")

for src in sources:
    prefix = src.get("prefix", src["name"])
    raw_dir = f"{raw_root}/{prefix}"
    try:
        dbutils.fs.rm(raw_dir, recurse=True)
        print(f"  Cleared raw + checkpoints: {raw_dir}")
    except Exception:
        pass

print("Cleanup done — staging untouched (data already landed).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2b — Create config table

# COMMAND ----------

config_table = f"{catalog}.{config_schema}.pipeline_config"

spark.sql(f"DROP TABLE IF EXISTS {config_table}")

spark.sql(f"""
CREATE TABLE {config_table} (
  source_name  STRING                COMMENT 'Unique key (_settings for pipeline-level config)',
  is_active    BOOLEAN               COMMENT 'Enable/disable without deleting',
  properties   MAP<STRING, STRING>   COMMENT 'All config as key-value pairs — extensible without schema changes'
)
COMMENT 'Pipeline config — loaded from conf/sources.yml. Schema is flexible via MAP column.'
""")

print(f"Config table created: {config_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Load settings + sources into config table

# COMMAND ----------

from pyspark.sql import Row

rows = []

settings_props = {k: str(v) for k, v in settings.items()}
settings_props["volume_path"]  = volume_path
settings_props["staging_root"] = staging_root
settings_props["raw_root"]     = raw_root
rows.append(Row(source_name="_settings", is_active=True, properties=settings_props))

for src in sources:
    props = {}
    for key, val in src.items():
        if key in ("name", "prefix"):
            continue
        if isinstance(val, list):
            props[key] = json.dumps(val)
        elif isinstance(val, str):
            props[key] = val.strip()
        else:
            props[key] = str(val)

    prefix = src.get("prefix", src["name"])
    props["prefix"]       = prefix
    props["staging_path"] = f"{staging_root}/{prefix}"
    props["raw_path"]     = f"{raw_root}/{prefix}"
    props["bronze_table"] = f"bronze_{src['name']}"

    rows.append(Row(source_name=src["name"], is_active=True, properties=props))

spark.createDataFrame(rows).write.mode("overwrite").saveAsTable(config_table)

print(f"\n{len(rows)} rows loaded into {config_table}:")
for r in rows:
    tag = "SETTINGS" if r.source_name == "_settings" else "SOURCE"
    print(f"  [{tag}] {r.source_name}: {list(r.properties.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Register the column mask function
# MAGIC
# MAGIC Reads `ad_group` and `encryption_key` from the `_settings` row — not hardcoded.

# COMMAND ----------

ad_group       = settings_props["ad_group"]
encryption_key = settings_props["encryption_key"]
mask_fn        = f"{catalog}.{config_schema}.pii_decrypt"

spark.sql(f"""
CREATE OR REPLACE FUNCTION {mask_fn}(encrypted_value STRING)
RETURNS STRING
COMMENT 'Column mask: AES-GCM decrypts PII for members of {ad_group}, redacts for everyone else'
RETURN
  CASE
    WHEN is_account_group_member('{ad_group}')
    THEN TRY_CAST(
           aes_decrypt(unbase64(encrypted_value), unhex('{encryption_key}'), 'GCM')
           AS STRING
         )
    ELSE '***REDACTED***'
  END
""")

print(f"Column mask function: {mask_fn}")

# COMMAND ----------

display(spark.table(config_table))

# COMMAND ----------

dbutils.notebook.exit(f"Config loaded — {len(sources)} sources + settings, mask function registered.")
