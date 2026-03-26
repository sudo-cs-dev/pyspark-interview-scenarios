# Databricks notebook source
# MAGIC %md
# MAGIC # Exploding Nested JSON in PySpark
# MAGIC
# MAGIC **Series:** PySpark Interview Scenarios #05 — [Chandra Shekhar Som](https://www.linkedin.com/in/c-shekhar1029/)
# MAGIC
# MAGIC Flatten nested JSON (structs + arrays) into a queryable flat table.
# MAGIC Covers dot notation, `explode` vs `explode_outer`, `from_json` for
# MAGIC string columns, and saving to Delta Lake on Unity Catalog.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

CATALOG = "main"
SCHEMA  = "gold"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Sample Nested JSON Data
# MAGIC
# MAGIC Simulating an API response — each order has a nested **customer** object
# MAGIC and an **items** array. We deliberately include:
# MAGIC - Order 1001: 2 items (normal case)
# MAGIC - Order 1002: 1 item (normal case)
# MAGIC - Order 1003: empty items `[]` (edge case)
# MAGIC - Order 1004: null items (edge case)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, ArrayType
)
import pyspark.sql.functions as F

json_data = [
    (1001, {"name": "Alice", "city": "Mumbai"},
     [{"product": "Laptop", "qty": 1, "price": 75000},
      {"product": "Mouse",  "qty": 2, "price": 500}]),
    (1002, {"name": "Bob", "city": "Delhi"},
     [{"product": "Keyboard", "qty": 1, "price": 2000}]),
    (1003, {"name": "Carol", "city": "Pune"},
     []),                                                   # empty array
    (1004, {"name": "Dave", "city": "Hyderabad"},
     None),                                                 # null
]

schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer", StructType([
        StructField("name", StringType()),
        StructField("city", StringType()),
    ])),
    StructField("items", ArrayType(
        StructType([
            StructField("product", StringType()),
            StructField("qty",     IntegerType()),
            StructField("price",   IntegerType()),
        ])
    )),
])

df = spark.createDataFrame(json_data, schema)

print("Raw nested data")
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inspect the Schema
# MAGIC
# MAGIC Always start here in an interview. `printSchema()` tells you which fields
# MAGIC are **structs** (flatten with dot notation) vs **arrays** (need `explode`).
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- order_id: integer
# MAGIC  |-- customer: struct          ← dot notation
# MAGIC  |    |-- name: string
# MAGIC  |    |-- city: string
# MAGIC  |-- items: array              ← needs explode
# MAGIC  |    |-- element: struct
# MAGIC  |    |    |-- product: string
# MAGIC  |    |    |-- qty: integer
# MAGIC  |    |    |-- price: integer
# MAGIC  ```

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Step 1 — Flatten Structs with Dot Notation
# MAGIC
# MAGIC Struct fields are accessed using `col("parent.child")`. This is cheap —
# MAGIC no row multiplication, just column extraction at the schema level.

# COMMAND ----------

df_flat = df.select(
    F.col("order_id"),
    F.col("customer.name").alias("cust_name"),
    F.col("customer.city").alias("cust_city"),
    F.col("items"),
)

print("Step 1: Struct fields flattened, items array still intact")
display(df_flat)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Step 2 — Explode the Array
# MAGIC
# MAGIC ### 4a. The WRONG way — `explode()` drops rows silently

# COMMAND ----------

df_exploded_bad = df_flat.withColumn(
    "item", F.explode("items")
).drop("items")

print("explode() — Carol (empty []) and Dave (null) are GONE")
print(f"   Row count: {df_exploded_bad.count()} (should be 5 if we count Carol & Dave)")
display(df_exploded_bad)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. The RIGHT way — `explode_outer()` preserves all rows

# COMMAND ----------

df_exploded = df_flat.withColumn(
    "item", F.explode_outer("items")
).drop("items")

print("explode_outer() — Carol & Dave preserved with nulls")
print(f"   Row count: {df_exploded.count()}")
display(df_exploded)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Step 3 — Flatten the Exploded Struct → Final Table
# MAGIC
# MAGIC The `item` column is still a struct. Use dot notation one more time
# MAGIC to pull out `product`, `qty`, `price`.

# COMMAND ----------

df_final = df_exploded.select(
    "order_id",
    "cust_name",
    "cust_city",
    F.col("item.product").alias("product"),
    F.col("item.qty").alias("qty"),
    F.col("item.price").alias("price"),
)

print("Final flat table — one row per item")
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expected Output
# MAGIC
# MAGIC | order_id | cust_name | cust_city | product  | qty  | price |
# MAGIC |----------|-----------|-----------|----------|------|-------|
# MAGIC | 1001     | Alice     | Mumbai    | Laptop   | 1    | 75000 |
# MAGIC | 1001     | Alice     | Mumbai    | Mouse    | 2    | 500   |
# MAGIC | 1002     | Bob       | Delhi     | Keyboard | 1    | 2000  |
# MAGIC | 1003     | Carol     | Pune      | null     | null | null  |
# MAGIC | 1004     | Dave      | Hyderabad | null     | null | null  |
# MAGIC
# MAGIC Carol and Dave are preserved with nulls — **no silent data loss**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bonus — Parsing JSON String Columns with `from_json()`
# MAGIC
# MAGIC Real-world data from Kafka, REST APIs, or raw files often arrives as a
# MAGIC **string** column containing JSON — not pre-parsed structs. You need
# MAGIC `from_json()` with an explicit schema to parse it first.

# COMMAND ----------

# Simulate JSON arriving as raw strings (like from Kafka topic or API log)
raw_data = [
    ('{"order_id": 2001, "customer": {"name": "Eve", "city": "Chennai"}, "items": [{"product": "Tablet", "qty": 1, "price": 30000}]}',),
    ('{"order_id": 2002, "customer": {"name": "Frank", "city": "Kolkata"}, "items": [{"product": "Charger", "qty": 3, "price": 800}, {"product": "Cable", "qty": 5, "price": 200}]}',),
]

df_raw = spark.createDataFrame(raw_data, ["raw_json"])
print("Raw JSON as string column")
display(df_raw)

# COMMAND ----------

# Define the schema — from_json needs this to know how to parse
json_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer", StructType([
        StructField("name", StringType()),
        StructField("city", StringType()),
    ])),
    StructField("items", ArrayType(
        StructType([
            StructField("product", StringType()),
            StructField("qty",     IntegerType()),
            StructField("price",   IntegerType()),
        ])
    )),
])

# Parse string → struct
df_parsed = df_raw.withColumn(
    "data", F.from_json(F.col("raw_json"), json_schema)
).select("data.*")

print("Parsed from string to struct")
df_parsed.printSchema()
display(df_parsed)

# COMMAND ----------

# Apply the same flatten + explode pattern
df_from_string = (
    df_parsed
    .select(
        F.col("order_id"),
        F.col("customer.name").alias("cust_name"),
        F.col("customer.city").alias("cust_city"),
        F.explode_outer("items").alias("item"),
    )
    .select(
        "order_id",
        "cust_name",
        "cust_city",
        F.col("item.product").alias("product"),
        F.col("item.qty").alias("qty"),
        F.col("item.price").alias("price"),
    )
)

print("Final flat table from string JSON")
display(df_from_string)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save to Delta Lake (Gold Layer)

# COMMAND ----------

TABLE = f"{CATALOG}.{SCHEMA}.fact_order_items"

df_final.write.format("delta").mode("overwrite").saveAsTable(TABLE)

print(f"Saved to {TABLE}")
display(spark.table(TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Bonus — Generic Recursive Flattener
# MAGIC
# MAGIC For deeply nested JSON with many levels, you can write a recursive
# MAGIC function that auto-flattens all struct fields. Useful in production
# MAGIC when schemas are wide and you don't want to hand-write every column.

# COMMAND ----------

def flatten_df(df):
    """Recursively flatten all StructType columns."""
    from pyspark.sql.types import StructType

    flat_cols = []
    nested = False

    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            nested = True
            for sub in field.dataType.fields:
                flat_cols.append(
                    F.col(f"{field.name}.{sub.name}").alias(f"{field.name}_{sub.name}")
                )
        else:
            flat_cols.append(F.col(field.name))

    df_flat = df.select(flat_cols)

    # Recurse if there are still nested structs
    if nested:
        return flatten_df(df_flat)
    return df_flat


# Demo: flatten original df (structs only, not arrays)
print("Auto-flattened structs (recursive)")
display(flatten_df(df.drop("items")))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **`printSchema()` first** — understand which fields are structs vs arrays
# MAGIC    before writing any code.
# MAGIC
# MAGIC 2. **Structs → dot notation** — cheap, no row multiplication.
# MAGIC    `col("customer.name")` extracts nested fields.
# MAGIC
# MAGIC 3. **Arrays → `explode_outer()`** — creates one row per element.
# MAGIC    Always use `explode_outer`, not `explode`, to prevent silent data loss
# MAGIC    on null/empty arrays.
# MAGIC
# MAGIC 4. **String JSON → `from_json()`** — real APIs deliver JSON as strings.
# MAGIC    Define an explicit `StructType` schema and parse before flattening.
# MAGIC
# MAGIC 5. **Order matters** — flatten structs first (cheap), then explode arrays
# MAGIC    (row multiplication). Filter before explode when possible.
# MAGIC
# MAGIC ---
# MAGIC *Part of the PySpark Interview Scenarios series by [Chandra Shekhar Som](https://www.linkedin.com/in/c-shekhar1029/)*
