# Databricks notebook source

# MAGIC %md
# MAGIC # Pipeline: Derived Tables (Domain-specific joins and aggregations)
# MAGIC
# MAGIC **Responsibility**: Create derived tables that combine multiple bronze sources.
# MAGIC These are domain-specific — they know about the relationship between tables
# MAGIC within a particular data domain.
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
# MAGIC ## Order Fulfilment Summary
# MAGIC
# MAGIC Joins `bronze_orders` + `bronze_shipments` to produce a per-order fulfilment view.
# MAGIC No PII in either input (both are bronze tables).

# COMMAND ----------

@dlt.table(
    name="bronze_order_fulfilment",
    comment=(
        "Order fulfilment summary — LEFT JOIN of bronze_orders + bronze_shipments. "
        "Shows order status alongside shipping info. No PII."
    ),
)
def bronze_order_fulfilment():
    orders = dlt.read("bronze_orders")
    shipments = dlt.read("bronze_shipments")
    return (
        orders
        .join(shipments, orders.order_id == shipments.order_id, "left")
        .select(
            orders.order_id,
            orders.customer_id,
            orders.product,
            orders.quantity,
            orders.amount,
            orders.order_date,
            orders.status.alias("order_status"),
            shipments.carrier,
            shipments.tracking_number,
            shipments.shipped_date,
            shipments.delivered_date,
            shipments.status.alias("shipment_status"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add further derived tables for this domain below
# MAGIC
# MAGIC Examples:
# MAGIC - `silver_customer_orders` — aggregate order totals per customer
# MAGIC - `silver_ticket_resolution` — join support tickets with orders for context
# MAGIC - `gold_daily_revenue` — daily revenue by product
