# Databricks notebook source

# MAGIC %md
# MAGIC # Generate Sample Data (Demo Only)
# MAGIC
# MAGIC **This notebook simulates an MFT dropping JSON files into staging volumes.**
# MAGIC In production this notebook does not exist — the MFT writes directly to storage.
# MAGIC
# MAGIC Run this BEFORE the pipeline — the data must exist first, then the pipeline processes it.

# COMMAND ----------

dbutils.widgets.text("catalog",       "")
dbutils.widgets.text("config_schema", "")
dbutils.widgets.text("volume",        "pipeline_data")

catalog       = dbutils.widgets.get("catalog")
config_schema = dbutils.widgets.get("config_schema")
volume        = dbutils.widgets.get("volume")

volume_path = f"/Volumes/{catalog}/{config_schema}/{volume}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{config_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{config_schema}.{volume}")

staging_root = f"{volume_path}/staging"

prefixes = {
    "orders":          "ecommerce/orders",
    "shipments":       "ecommerce/shipments",
    "support_tickets": "ecommerce/support_tickets",
}

for prefix in prefixes.values():
    try:
        dbutils.fs.rm(f"{staging_root}/{prefix}", recurse=True)
    except Exception:
        pass

print(f"Staging root: {staging_root} (cleared)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders

# COMMAND ----------

import json, os

def write_json_lines(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"  {len(records)} records → {path}")

orders = [
    {"order_id": "ORD-001", "customer_id": "CUST-001", "customer_email": "alice@example.com", "customer_name": "Alice Smith", "product": "Laptop Pro 15", "quantity": 1, "amount": 1299.99, "currency": "GBP", "order_date": "2025-01-15T10:00:00Z", "status": "DELIVERED"},
    {"order_id": "ORD-002", "customer_id": "CUST-002", "customer_email": "bob@example.com", "customer_name": "Bob Jones", "product": "Wireless Mouse", "quantity": 2, "amount": 49.98, "currency": "GBP", "order_date": "2025-01-15T11:30:00Z", "status": "SHIPPED"},
    {"order_id": "ORD-003", "customer_id": "CUST-003", "customer_email": "carol@example.com", "customer_name": "Carol Williams", "product": "USB-C Hub", "quantity": 1, "amount": 79.99, "currency": "GBP", "order_date": "2025-01-15T14:00:00Z", "status": "PROCESSING"},
    {"order_id": "ORD-004", "customer_id": "CUST-001", "customer_email": "alice@example.com", "customer_name": "Alice Smith", "product": "Monitor Stand", "quantity": 1, "amount": 45.00, "currency": "GBP", "order_date": "2025-01-16T09:00:00Z", "status": "DELIVERED"},
    {"order_id": "ORD-005", "customer_id": "CUST-004", "customer_email": "david@example.com", "customer_name": "David Brown", "product": "Keyboard MX", "quantity": 1, "amount": 109.99, "currency": "GBP", "order_date": "2025-01-16T15:30:00Z", "status": "DELIVERED"},
]
write_json_lines(f"{staging_root}/{prefixes['orders']}/batch_001.json", orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shipments

# COMMAND ----------

shipments = [
    {"shipment_id": "SHP-001", "order_id": "ORD-001", "carrier": "Royal Mail", "tracking_number": "RM123456789GB", "shipped_date": "2025-01-15T16:00:00Z", "delivered_date": "2025-01-17T10:30:00Z", "status": "DELIVERED"},
    {"shipment_id": "SHP-002", "order_id": "ORD-002", "carrier": "DPD", "tracking_number": "DPD987654321", "shipped_date": "2025-01-16T09:00:00Z", "delivered_date": None, "status": "IN_TRANSIT"},
    {"shipment_id": "SHP-003", "order_id": "ORD-004", "carrier": "Royal Mail", "tracking_number": "RM111222333GB", "shipped_date": "2025-01-16T14:00:00Z", "delivered_date": "2025-01-18T11:00:00Z", "status": "DELIVERED"},
    {"shipment_id": "SHP-004", "order_id": "ORD-005", "carrier": "Hermes", "tracking_number": "HRM445566778", "shipped_date": "2025-01-17T08:00:00Z", "delivered_date": "2025-01-19T09:15:00Z", "status": "DELIVERED"},
]
write_json_lines(f"{staging_root}/{prefixes['shipments']}/batch_001.json", shipments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Support Tickets

# COMMAND ----------

tickets = [
    {"ticket_id": "TKT-001", "customer_id": "CUST-002", "customer_email": "bob@example.com", "subject": "Missing item in order", "description": "My order ORD-002 only had 1 mouse instead of 2.", "priority": "HIGH", "status": "OPEN", "created_at": "2025-01-17T10:00:00Z", "resolved_at": None},
    {"ticket_id": "TKT-002", "customer_id": "CUST-003", "customer_email": "carol@example.com", "subject": "Order not shipped", "description": "ORD-003 has been processing for 2 days. When will it ship?", "priority": "MEDIUM", "status": "OPEN", "created_at": "2025-01-17T14:30:00Z", "resolved_at": None},
    {"ticket_id": "TKT-003", "customer_id": "CUST-001", "customer_email": "alice@example.com", "subject": "Return request", "description": "I'd like to return the Monitor Stand from ORD-004.", "priority": "LOW", "status": "RESOLVED", "created_at": "2025-01-19T09:00:00Z", "resolved_at": "2025-01-19T15:00:00Z"},
]
write_json_lines(f"{staging_root}/{prefixes['support_tickets']}/batch_001.json", tickets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fault tolerance test — malformed batch

# COMMAND ----------

dbutils.widgets.text("include_bad_data", "false")
include_bad = dbutils.widgets.get("include_bad_data").strip().lower() == "true"

if include_bad:
    bad_records_path = f"{staging_root}/{prefixes['orders']}/batch_002_BAD.json"
    os.makedirs(os.path.dirname(bad_records_path), exist_ok=True)
    with open(bad_records_path, "w") as f:
        f.write('{"order_id": "ORD-BAD-001", "customer_id": "CUST-999", "amount": "NOT_A_NUMBER"}\n')
        f.write('this is not valid json at all\n')
        f.write('{"completely_wrong_schema": true, "random_field": 42}\n')
    print(f"Bad data written: {bad_records_path}")
else:
    print("Skipping bad data generation (pass include_bad_data=true to enable).")

# COMMAND ----------

print(f"\nSample data written to staging volumes:")
print(f"  {len(orders)} orders")
print(f"  {len(shipments)} shipments")
print(f"  {len(tickets)} support tickets")
if include_bad:
    print(f"  3 bad records (batch_002_BAD.json in orders)")
print(f"\nIn production, the MFT writes directly — this notebook is not needed.")

dbutils.notebook.exit("Sample data generated.")
