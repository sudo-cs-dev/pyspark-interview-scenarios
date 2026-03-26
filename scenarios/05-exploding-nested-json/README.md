# #05 — Exploding Nested JSON in PySpark

> *"Flatten this API response into a queryable table with one row per item."*

**Difficulty:** Medium · **Frequency:** Very High · **Time to solve:** 10–15 min

---

## The Problem

You receive API responses where each record contains **nested objects** (structs) and **arrays of structs**. The analytics team needs a flat, queryable table. You need to handle structs, arrays, null/empty arrays, and sometimes raw JSON strings.

```json
{
  "order_id": 1001,
  "customer": {
    "name": "Alice",
    "city": "Mumbai"
  },
  "items": [
    { "product": "Laptop", "qty": 1, "price": 75000 },
    { "product": "Mouse",  "qty": 2, "price": 500 }
  ]
}
```

**Goal:** One row per item — so this single record becomes **2 rows** (Laptop + Mouse), each with the customer info denormalized.

## The Approach

```
Step 1: printSchema()              → Understand what's nested and how
Step 2: Dot notation               → Flatten struct fields (cheap, no row multiplication)
Step 3: explode_outer()            → One row per array element (preserves nulls)
Step 4: Dot notation again         → Flatten the exploded struct
Step 5: (Bonus) from_json()        → Parse raw JSON strings before flattening
```

```python
# Step 1: Flatten structs
df_flat = df.select(
    F.col("order_id"),
    F.col("customer.name").alias("cust_name"),
    F.col("customer.city").alias("cust_city"),
    F.col("items")
)

# Step 2: Explode array → one row per item
df_exploded = df_flat.withColumn(
    "item", F.explode_outer("items")
).drop("items")

# Step 3: Flatten the exploded struct
df_final = df_exploded.select(
    "order_id", "cust_name", "cust_city",
    F.col("item.product").alias("product"),
    F.col("item.qty").alias("qty"),
    F.col("item.price").alias("price")
)
```

## Before & After

**Before** — nested JSON (1 row):

| order_id | customer | items |
|---|---|---|
| 1001 | {Alice, Mumbai} | [{Laptop, 1, 75000}, {Mouse, 2, 500}] |

**After** — flat table (2 rows):

| order_id | cust_name | cust_city | product | qty | price |
|---|---|---|---|---|---|
| 1001 | Alice | Mumbai | Laptop | 1 | 75000 |
| 1001 | Alice | Mumbai | Mouse | 2 | 500 |

## The #1 Trap: `explode()` vs `explode_outer()`

This is the interview differentiator. What happens when items is **empty `[]` or null**?

| Function | Behavior on null/empty | Risk |
|---|---|---|
| `F.explode()` | **Drops the row entirely** | Silent data loss — orders with no items vanish |
| `F.explode_outer()` | **Keeps the row with nulls** | Safe — every order preserved |

```python
# BAD — Carol (empty []) and Dave (null) disappear
df.withColumn("item", F.explode("items"))

# GOOD — Carol and Dave stay with null item columns
df.withColumn("item", F.explode_outer("items"))
```

**With `explode_outer`** the full output is:

| order_id | cust_name | cust_city | product | qty | price |
|---|---|---|---|---|---|
| 1001 | Alice | Mumbai | Laptop | 1 | 75000 |
| 1001 | Alice | Mumbai | Mouse | 2 | 500 |
| 1002 | Bob | Delhi | Keyboard | 1 | 2000 |
| 1003 | Carol | Pune | null | null | null |
| 1004 | Dave | Hyderabad | null | null | null |

## Bonus: `from_json()` for String Columns

Real data from Kafka, REST APIs, or raw logs often arrives as a **string column** — not pre-parsed structs. You need `from_json()` with an explicit schema:

```python
from pyspark.sql.types import *

json_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer", StructType([
        StructField("name", StringType()),
        StructField("city", StringType())
    ])),
    StructField("items", ArrayType(StructType([
        StructField("product", StringType()),
        StructField("qty", IntegerType()),
        StructField("price", IntegerType())
    ])))
])

df_parsed = df_raw.withColumn(
    "data", F.from_json(F.col("raw_json"), json_schema)
).select("data.*")

# Then apply the same flatten + explode_outer pattern
```

## What's Also in the Notebook

- **Recursive generic flattener** — a `flatten_df()` function that auto-flattens all struct columns regardless of depth
- **Delta Lake save** — writing the final table to Unity Catalog gold layer
- **Edge case data** — orders with empty and null items arrays baked into sample data

## Interview Answer Framework

1. **Start with `printSchema()`** — "First I'd inspect the schema to understand which fields are structs, arrays, or maps."
2. **Flatten structs first, then explode** — order matters. Struct access is cheap. Explode multiplies rows — do it after filtering if possible.
3. **Use `explode_outer`, not `explode`** — mention data completeness proactively.
4. **Mention `from_json()` for string inputs** — knowing this shows real production experience with APIs and Kafka.

## How to Run

1. Import `exploding_nested_json.py` into your Databricks workspace
2. Update `CATALOG` / `SCHEMA` at the top
3. Run all cells — self-contained with sample data

## Requirements

- Databricks Runtime 17.0+ (Delta Lake 4+)
- Unity Catalog enabled workspace
- Works on serverless and classic compute

---

*Part of the PySpark Interview Scenarios series by [Chandra Shekhar Som](https://www.linkedin.com/in/c-shekhar1029/)*
