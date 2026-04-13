# Databricks notebook source

# MAGIC %md
# MAGIC # Pipeline: Derived Tables (Domain-specific joins and aggregations)
# MAGIC
# MAGIC **Responsibility**: Create derived tables that combine multiple bronze sources.
# MAGIC These are domain-specific — they know about the relationship between tables
# MAGIC within a particular data domain (e.g., Amazon Connect call centre data).
# MAGIC
# MAGIC **Reuse**: Copy and adapt this notebook per domain. The ingest/bronze/pdv notebooks
# MAGIC are fully generic — only this one changes when you have a new domain with different join logic.
# MAGIC
# MAGIC **Inputs**: Bronze tables produced by `bronze_tables.py` in the same pipeline.
# MAGIC **Outputs**: Derived tables (no PII — joins on bronze only).

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amazon Connect — Agent Call Summary
# MAGIC
# MAGIC Joins `bronze_contact_records` + `bronze_agent_events` to produce a per-call summary
# MAGIC of agent activity. No PII in either input (both are bronze tables).

# COMMAND ----------

@dlt.table(
    name="bronze_agent_call_summary",
    comment=(
        "Agent call handling summary — LEFT JOIN of bronze_contact_records + bronze_agent_events "
        "(ON_CALL events only). No PII."
    ),
)
def bronze_agent_call_summary():
    contacts = dlt.read("bronze_contact_records")
    events = (
        dlt.read("bronze_agent_events")
        .filter(col("event_type") == "STATUS_CHANGE")
        .filter(col("status") == "ON_CALL")
    )
    return (
        contacts
        .join(events, contacts.agent_id == events.agent_id, "left")
        .select(
            contacts.contact_id,
            contacts.agent_id,
            events.agent_name,
            contacts.channel,
            contacts.queue_name,
            contacts.start_time,
            contacts.end_time,
            contacts.duration_seconds,
            contacts.hold_duration_seconds,
            contacts.after_call_work_seconds,
            contacts.disconnect_reason,
            contacts.outcome,
            events.timestamp.alias("agent_event_timestamp"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add further derived tables for this domain below
# MAGIC
# MAGIC Examples:
# MAGIC - `silver_contact_sentiment` — aggregate sentiment scores per contact from `bronze_transcripts`
# MAGIC - `silver_queue_daily_stats` — daily call volume + avg duration per queue
# MAGIC - `gold_agent_performance` — weekly agent KPIs
