# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 2 — Delta Lake MERGE in PySpark
# MAGIC
# MAGIC **Series:** PySpark Interview Scenarios #04 — [Chandra Shekhar Som](https://www.linkedin.com/in/c-shekhar1029/)
# MAGIC
# MAGIC Implements Slowly Changing Dimension Type 2 using Delta Lake on Databricks
# MAGIC with Unity Catalog. Demonstrates the correct **two-step pattern** (MERGE to
# MAGIC expire + INSERT for new versions) and covers common pitfalls like the
# MAGIC lazy-evaluation trap.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup — Catalog, Schema & Config

# COMMAND ----------

# Change these to match your environment
CATALOG = "main"
SCHEMA  = "gold"
TABLE   = "dim_customer"

FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

# Clean slate for reproducible demo
spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the Target Dimension Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType
from datetime import date

data = [
    (101, "Alice", "Mumbai",  date(2023, 1, 15), date(9999, 12, 31), True),
    (102, "Bob",   "Delhi",   date(2023, 3, 20), date(9999, 12, 31), True),
    (103, "Carol", "Chennai", date(2022, 6, 1),  date(2023, 11, 30), False),
    (104, "Carol", "Pune",    date(2023, 12, 1), date(9999, 12, 31), True),
]

schema = StructType([
    StructField("id",         IntegerType(), False),
    StructField("name",       StringType(),  False),
    StructField("city",       StringType(),  False),
    StructField("eff_start",  DateType(),    False),
    StructField("eff_end",    DateType(),    False),
    StructField("is_current", BooleanType(), False),
])

df = spark.createDataFrame(data, schema)
df.write.format("delta").mode("overwrite").saveAsTable(FULL_TABLE)

print(f"Target table created: {FULL_TABLE}")
display(spark.table(FULL_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simulate Incoming Source Records

# COMMAND ----------

source_data = [
    (101, "Alice", "Bangalore"),   # city changed: Mumbai → Bangalore
    (102, "Bob",   "Delhi"),       # no change
    (105, "Dave",  "Hyderabad"),   # brand-new customer
]

source_schema = StructType([
    StructField("id",   IntegerType(), False),
    StructField("name", StringType(),  False),
    StructField("city", StringType(),  False),
])

source = spark.createDataFrame(source_data, source_schema)

print("Source (incoming) records")
display(source)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Detect Changed & New Records
# MAGIC
# MAGIC **Critical:** We `.collect()` both DataFrames **before** the MERGE modifies
# MAGIC the target table. Without this, Spark's lazy evaluation would re-read the
# MAGIC mutated target and return empty results — the #1 SCD2 pitfall.

# COMMAND ----------

from delta.tables import DeltaTable
import pyspark.sql.functions as F

target    = DeltaTable.forName(spark, FULL_TABLE)
today     = F.current_date()
yesterday = F.date_sub(today, 1)

# --- Changed records (city differs for active rows) ---
changes = (
    source.alias("s")
    .join(
        target.toDF().alias("t"),
        (F.col("s.id") == F.col("t.id"))
        & (F.col("t.is_current") == True)
        & (F.col("s.city") != F.col("t.city")),
    )
    .select("s.*")
)

# Materialize BEFORE the MERGE mutates the target
changes_collected = spark.createDataFrame(changes.collect(), source.schema)
print(f"Found {changes_collected.count()} changed records")
display(changes_collected)

# COMMAND ----------

# --- Brand-new records (ID not in target at all) ---
new_records = source.alias("s").join(
    target.toDF().alias("t"),
    F.col("s.id") == F.col("t.id"),
    "left_anti",
)
new_collected = spark.createDataFrame(new_records.collect(), source.schema)

print(f"Found {new_collected.count()} new records")
display(new_collected)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE — Expire Old Rows Only
# MAGIC
# MAGIC A single MERGE **cannot** both UPDATE and INSERT for the same business key.
# MAGIC Delta routes each source row to *either* the matched or the not-matched
# MAGIC clause — never both. So we split the work:
# MAGIC
# MAGIC | Step | Operation | What it does |
# MAGIC |------|-----------|-------------|
# MAGIC | 4    | MERGE     | Expire old active rows (`whenMatchedUpdate` only) |
# MAGIC | 5    | INSERT    | Append new versions of changed records |
# MAGIC | 6    | INSERT    | Append brand-new customers |

# COMMAND ----------

target.alias("t").merge(
    changes_collected.alias("s"),
    "t.id = s.id AND t.is_current = true",
).whenMatchedUpdate(
    set={
        "eff_end":    yesterday,
        "is_current": F.lit(False),
    }
).execute()

print("Old rows expired")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. INSERT — New Versions of Changed Records

# COMMAND ----------

if changes_collected.count() > 0:
    insert_changed = (
        changes_collected
        .withColumn("eff_start",  today)
        .withColumn("eff_end",    F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    insert_changed.createOrReplaceTempView("v_changed")
    spark.sql(f"INSERT INTO {FULL_TABLE} SELECT * FROM v_changed")
    print("Changed versions inserted")
else:
    print("No changed records to insert")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. INSERT — Brand-New Customers

# COMMAND ----------

if new_collected.count() > 0:
    insert_new = (
        new_collected
        .withColumn("eff_start",  today)
        .withColumn("eff_end",    F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    insert_new.createOrReplaceTempView("v_new")
    spark.sql(f"INSERT INTO {FULL_TABLE} SELECT * FROM v_new")
    print("New customers inserted")
else:
    print("No new customers to insert")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate — Final Table

# COMMAND ----------

print(f"Final {FULL_TABLE} after SCD Type 2")
display(spark.table(FULL_TABLE).orderBy("id", "eff_start"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expected Output
# MAGIC
# MAGIC | id  | name  | city      | eff_start  | eff_end    | is_current |
# MAGIC |-----|-------|-----------|------------|------------|------------|
# MAGIC | 101 | Alice | Mumbai    | 2023-01-15 | *yesterday*| **false**  |
# MAGIC | 101 | Alice | Bangalore | *today*    | 9999-12-31 | **true**   |
# MAGIC | 102 | Bob   | Delhi     | 2023-03-20 | 9999-12-31 | true       |
# MAGIC | 103 | Carol | Chennai   | 2022-06-01 | 2023-11-30 | false      |
# MAGIC | 104 | Carol | Pune      | 2023-12-01 | 9999-12-31 | true       |
# MAGIC | 105 | Dave  | Hyderabad | *today*    | 9999-12-31 | **true**   |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Bonus — Delta History Audit

# COMMAND ----------

history = DeltaTable.forName(spark, FULL_TABLE).history()
display(history.select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Two-step pattern:** MERGE to expire → INSERT for new versions. A single
# MAGIC    MERGE cannot update + insert for the same business key.
# MAGIC
# MAGIC 2. **Materialize before mutating:** `.collect()` the changes DataFrame before
# MAGIC    the MERGE modifies the target. Spark's lazy evaluation will otherwise
# MAGIC    re-read the mutated table and return empty.
# MAGIC
# MAGIC 3. **Use `INSERT INTO` on serverless:** `saveAsTable("append")` and `.cache()`
# MAGIC    can behave unexpectedly on Databricks serverless. SQL `INSERT INTO` is the
# MAGIC    most reliable path.
# MAGIC
# MAGIC 4. **Handle new customers separately:** Use a `left_anti` join to find IDs
# MAGIC    that don't exist in the target, then append with a plain INSERT.
# MAGIC
# MAGIC ---
# MAGIC *Part of the PySpark Interview Scenarios series by [Chandra Shekhar Som](https://www.linkedin.com/in/c-shekhar1029/)*