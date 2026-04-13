# Databricks notebook source

# MAGIC %md
# MAGIC # Generate Sample Data (Demo Only)
# MAGIC
# MAGIC **This notebook simulates GoAnywhere MFT dropping JSON files into staging volumes.**
# MAGIC In production this notebook does not exist — the MFT writes directly to volumes.
# MAGIC
# MAGIC Run this BEFORE the pipeline — the data must exist first, then the pipeline processes it.
# MAGIC This notebook is completely independent of the pipeline config table.

# COMMAND ----------

dbutils.widgets.text("catalog",       "carlota_tko_day3_catalog")
dbutils.widgets.text("config_schema", "sse_demo")
dbutils.widgets.text("volume",        "sse_data")

catalog       = dbutils.widgets.get("catalog")
config_schema = dbutils.widgets.get("config_schema")
volume        = dbutils.widgets.get("volume")

volume_path = f"/Volumes/{catalog}/{config_schema}/{volume}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{config_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{config_schema}.{volume}")

staging_root = f"{volume_path}/staging"

prefixes = {
    "contact_records": "amazon_connect/contact_records",
    "agent_events":    "amazon_connect/agent_events",
    "transcripts":     "amazon_connect/transcripts",
}

for prefix in prefixes.values():
    try:
        dbutils.fs.rm(f"{staging_root}/{prefix}", recurse=True)
    except Exception:
        pass

print(f"Staging root: {staging_root} (cleared)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amazon Connect — Contact Records

# COMMAND ----------

import json, os

def write_json_lines(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"  {len(records)} records → {path}")

contacts = [
    {"contact_id": "CT-001", "channel": "VOICE", "initiation_method": "INBOUND", "queue_name": "Customer Service", "agent_id": "AGT-001", "customer_phone": "+44 7700 900123", "customer_name": "Alice Smith", "start_time": "2025-01-15T10:00:00Z", "end_time": "2025-01-15T10:15:30Z", "duration_seconds": 930, "hold_duration_seconds": 45, "after_call_work_seconds": 120, "disconnect_reason": "CUSTOMER_DISCONNECT", "outcome": "RESOLVED"},
    {"contact_id": "CT-002", "channel": "VOICE", "initiation_method": "INBOUND", "queue_name": "Billing", "agent_id": "AGT-002", "customer_phone": "+44 7700 900456", "customer_name": "Bob Jones", "start_time": "2025-01-15T10:05:00Z", "end_time": "2025-01-15T10:25:00Z", "duration_seconds": 1200, "hold_duration_seconds": 180, "after_call_work_seconds": 90, "disconnect_reason": "AGENT_DISCONNECT", "outcome": "ESCALATED"},
    {"contact_id": "CT-003", "channel": "CHAT", "initiation_method": "INBOUND", "queue_name": "Technical Support", "agent_id": "AGT-001", "customer_phone": "+44 7700 900789", "customer_name": "Carol Williams", "start_time": "2025-01-15T11:00:00Z", "end_time": "2025-01-15T11:08:00Z", "duration_seconds": 480, "hold_duration_seconds": 0, "after_call_work_seconds": 60, "disconnect_reason": "CUSTOMER_DISCONNECT", "outcome": "RESOLVED"},
    {"contact_id": "CT-004", "channel": "VOICE", "initiation_method": "CALLBACK", "queue_name": "Customer Service", "agent_id": "AGT-003", "customer_phone": "+44 7700 900321", "customer_name": "David Brown", "start_time": "2025-01-15T14:00:00Z", "end_time": "2025-01-15T14:22:00Z", "duration_seconds": 1320, "hold_duration_seconds": 60, "after_call_work_seconds": 180, "disconnect_reason": "CUSTOMER_DISCONNECT", "outcome": "RESOLVED"},
    {"contact_id": "CT-005", "channel": "VOICE", "initiation_method": "INBOUND", "queue_name": "Billing", "agent_id": "AGT-002", "customer_phone": "+44 7700 900654", "customer_name": "Eve Davis", "start_time": "2025-01-15T15:30:00Z", "end_time": "2025-01-15T15:45:00Z", "duration_seconds": 900, "hold_duration_seconds": 30, "after_call_work_seconds": 60, "disconnect_reason": "CUSTOMER_DISCONNECT", "outcome": "RESOLVED"},
]
write_json_lines(f"{staging_root}/{prefixes['contact_records']}/batch_001.json", contacts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amazon Connect — Agent Events

# COMMAND ----------

agent_events = [
    {"event_id": "EVT-001", "agent_id": "AGT-001", "agent_name": "John Parker", "agent_email": "john.parker@sse.co.uk", "event_type": "LOGIN", "status": "AVAILABLE", "queue_name": "Customer Service", "timestamp": "2025-01-15T08:00:00Z"},
    {"event_id": "EVT-002", "agent_id": "AGT-002", "agent_name": "Sarah Mitchell", "agent_email": "sarah.mitchell@sse.co.uk", "event_type": "LOGIN", "status": "AVAILABLE", "queue_name": "Billing", "timestamp": "2025-01-15T08:05:00Z"},
    {"event_id": "EVT-003", "agent_id": "AGT-003", "agent_name": "James Wilson", "agent_email": "james.wilson@sse.co.uk", "event_type": "LOGIN", "status": "AVAILABLE", "queue_name": "Customer Service", "timestamp": "2025-01-15T08:10:00Z"},
    {"event_id": "EVT-004", "agent_id": "AGT-001", "agent_name": "John Parker", "agent_email": "john.parker@sse.co.uk", "event_type": "STATUS_CHANGE", "status": "ON_CALL", "queue_name": "Customer Service", "timestamp": "2025-01-15T10:00:00Z"},
    {"event_id": "EVT-005", "agent_id": "AGT-002", "agent_name": "Sarah Mitchell", "agent_email": "sarah.mitchell@sse.co.uk", "event_type": "STATUS_CHANGE", "status": "ON_CALL", "queue_name": "Billing", "timestamp": "2025-01-15T10:05:00Z"},
    {"event_id": "EVT-006", "agent_id": "AGT-001", "agent_name": "John Parker", "agent_email": "john.parker@sse.co.uk", "event_type": "STATUS_CHANGE", "status": "AFTER_CALL_WORK", "queue_name": "Customer Service", "timestamp": "2025-01-15T10:15:30Z"},
    {"event_id": "EVT-007", "agent_id": "AGT-001", "agent_name": "John Parker", "agent_email": "john.parker@sse.co.uk", "event_type": "STATUS_CHANGE", "status": "AVAILABLE", "queue_name": "Customer Service", "timestamp": "2025-01-15T10:17:30Z"},
    {"event_id": "EVT-008", "agent_id": "AGT-003", "agent_name": "James Wilson", "agent_email": "james.wilson@sse.co.uk", "event_type": "STATUS_CHANGE", "status": "ON_BREAK", "queue_name": "Customer Service", "timestamp": "2025-01-15T12:00:00Z"},
]
write_json_lines(f"{staging_root}/{prefixes['agent_events']}/batch_001.json", agent_events)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amazon Connect — Transcripts

# COMMAND ----------

transcripts = [
    {"transcript_id": "TR-001", "contact_id": "CT-001", "turn_number": 1, "participant": "AGENT", "content": "Good morning, this is John from SSE Customer Service. How can I help you today?", "sentiment": "POSITIVE", "start_offset_ms": 0, "end_offset_ms": 4200},
    {"transcript_id": "TR-002", "contact_id": "CT-001", "turn_number": 2, "participant": "CUSTOMER", "content": "Hi, my name is Alice Smith, account number SSE-445912. I have a question about my latest bill.", "sentiment": "NEUTRAL", "start_offset_ms": 4500, "end_offset_ms": 9800},
    {"transcript_id": "TR-003", "contact_id": "CT-001", "turn_number": 3, "participant": "AGENT", "content": "Of course Alice, let me pull up your account. I can see your bill from December.", "sentiment": "POSITIVE", "start_offset_ms": 10000, "end_offset_ms": 16500},
    {"transcript_id": "TR-004", "contact_id": "CT-002", "turn_number": 1, "participant": "AGENT", "content": "Hello, SSE Billing department, Sarah speaking. How may I assist you?", "sentiment": "POSITIVE", "start_offset_ms": 0, "end_offset_ms": 3800},
    {"transcript_id": "TR-005", "contact_id": "CT-002", "turn_number": 2, "participant": "CUSTOMER", "content": "Yes, this is Bob Jones. I've been charged twice for my gas supply this month.", "sentiment": "NEGATIVE", "start_offset_ms": 4000, "end_offset_ms": 10200},
    {"transcript_id": "TR-006", "contact_id": "CT-003", "turn_number": 1, "participant": "AGENT", "content": "Hi, you've reached SSE Technical Support. I'm John, how can I help?", "sentiment": "POSITIVE", "start_offset_ms": 0, "end_offset_ms": 3500},
    {"transcript_id": "TR-007", "contact_id": "CT-003", "turn_number": 2, "participant": "CUSTOMER", "content": "Hi John, I'm Carol Williams. My smart meter stopped sending readings about a week ago.", "sentiment": "NEUTRAL", "start_offset_ms": 3800, "end_offset_ms": 8500},
    {"transcript_id": "TR-008", "contact_id": "CT-004", "turn_number": 1, "participant": "AGENT", "content": "Good afternoon, this is James from SSE. I'm returning your call, Mr. Brown.", "sentiment": "POSITIVE", "start_offset_ms": 0, "end_offset_ms": 4000},
]
write_json_lines(f"{staging_root}/{prefixes['transcripts']}/batch_001.json", transcripts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fault tolerance test — malformed batch
# MAGIC
# MAGIC Simulates a file that arrives with corrupt JSON and wrong-schema records.
# MAGIC The pipeline should still process the other sources and the good records.

# COMMAND ----------

dbutils.widgets.text("include_bad_data", "false")
include_bad = dbutils.widgets.get("include_bad_data").strip().lower() == "true"

if include_bad:
    bad_records_path = f"{staging_root}/{prefixes['contact_records']}/batch_002_BAD.json"
    os.makedirs(os.path.dirname(bad_records_path), exist_ok=True)
    with open(bad_records_path, "w") as f:
        f.write('{"contact_id": "CT-BAD-001", "channel": "VOICE", "customer_phone": "+44 0000", "customer_name": "Bad Record", "duration_seconds": "NOT_A_NUMBER"}\n')
        f.write('this is not valid json at all\n')
        f.write('{"completely_wrong_schema": true, "random_field": 42}\n')
    print(f"Bad data written: {bad_records_path}")
    print("  - Row 1: wrong type (duration_seconds as string)")
    print("  - Row 2: invalid JSON")
    print("  - Row 3: completely wrong schema")
else:
    print("Skipping bad data generation (pass include_bad_data=true to enable).")

# COMMAND ----------

print(f"\nSample data written to staging volumes:")
print(f"  {len(contacts)} contact records")
print(f"  {len(agent_events)} agent events")
print(f"  {len(transcripts)} transcripts")
if include_bad:
    print(f"  3 bad records (batch_002_BAD.json in contact_records)")
print(f"\nIn production, GoAnywhere MFT writes directly — this notebook is not needed.")

dbutils.notebook.exit("Sample data generated.")
