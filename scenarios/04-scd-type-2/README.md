# #04 — SCD Type 2 with Delta Lake MERGE

> *"A customer changes their address. How do you keep both versions in your dimension table?"*

**Difficulty:** Hard · **Frequency:** Very High · **Time to solve:** 15–20 min

---

## The Problem

Your `dim_customer` table has 10M rows. When a customer moves from Mumbai to Bangalore, you can't just overwrite — historical reports would break. You need to **expire the old row** and **insert a new version** while keeping the full audit trail intact.

This is **Slowly Changing Dimension Type 2** — the most asked data modeling question in Senior DE interviews.

## Why Most Solutions Are Wrong

Every tutorial and most candidates show a single MERGE like this:

```python
# ❌ THIS DOES NOT WORK
target.alias("t").merge(
    changes.alias("s"),
    "t.id = s.id AND t.is_current = true"
).whenMatchedUpdate(set={
    "eff_end": yesterday,
    "is_current": F.lit(False)
}).whenNotMatchedInsert(values={
    "id": F.col("s.id"), ...
    "is_current": F.lit(True)
}).execute()
```

**The problem:** Delta Lake routes each source row to *either* matched or not-matched — **never both**. Since Alice's ID exists and matches, it goes to `whenMatchedUpdate`. The `whenNotMatchedInsert` never fires. Alice's old row gets expired, but the new row is silently never created.

## The Correct Pattern

```
Step 1: .collect()  →  Materialize changes BEFORE modifying target
Step 2: MERGE       →  Expire old rows only (whenMatchedUpdate)
Step 3: INSERT INTO →  Append new versions of changed records
Step 4: INSERT INTO →  Append brand-new customers (left_anti join)
```

```python
# ✅ THE CORRECT WAY

# Step 1: Snapshot changes before MERGE mutates the target
changes_collected = spark.createDataFrame(changes.collect(), source.schema)

# Step 2: Expire old rows only
target.alias("t").merge(
    changes_collected.alias("s"),
    "t.id = s.id AND t.is_current = true"
).whenMatchedUpdate(set={
    "eff_end":    yesterday,
    "is_current": F.lit(False)
}).execute()

# Step 3: Insert new versions separately
insert_df = (changes_collected
    .withColumn("eff_start",  today)
    .withColumn("eff_end",    F.lit("9999-12-31").cast("date"))
    .withColumn("is_current", F.lit(True)))
insert_df.createOrReplaceTempView("v_changed")
spark.sql(f"INSERT INTO {FULL_TABLE} SELECT * FROM v_changed")

# Step 4: Handle brand-new customers (left_anti join + INSERT)
```

## Before & After

**Before** — Alice lives in Mumbai:

| id  | name  | city    | eff_start  | eff_end    | is_current |
|-----|-------|---------|------------|------------|------------|
| 101 | Alice | Mumbai  | 2023-01-15 | 9999-12-31 | true       |
| 102 | Bob   | Delhi   | 2023-03-20 | 9999-12-31 | true       |
| 103 | Carol | Chennai | 2022-06-01 | 2023-11-30 | false      |
| 104 | Carol | Pune    | 2023-12-01 | 9999-12-31 | true       |

**After** — Alice moved to Bangalore, new customer Dave arrives:

| id  | name  | city        | eff_start   | eff_end     | is_current |
|-----|-------|-------------|-------------|-------------|------------|
| 101 | Alice | Mumbai      | 2023-01-15  | *yesterday* | **false**  |
| 101 | Alice | **Bangalore** | *today*   | 9999-12-31  | **true**   |
| 102 | Bob   | Delhi       | 2023-03-20  | 9999-12-31  | true       |
| 103 | Carol | Chennai     | 2022-06-01  | 2023-11-30  | false      |
| 104 | Carol | Pune        | 2023-12-01  | 9999-12-31  | true       |
| 105 | Dave  | **Hyderabad** | *today*   | 9999-12-31  | **true**   |

## Pitfalls & Fixes

| #  | Pitfall                    | What Goes Wrong                                        | Fix                                              |
|----|----------------------------|--------------------------------------------------------|--------------------------------------------------|
| 1  | **Single MERGE**           | `whenNotMatchedInsert` never fires for existing keys   | Split into MERGE (expire) + INSERT (new version) |
| 2  | **Lazy evaluation trap**   | `changes` DF re-reads mutated target → returns empty   | `.collect()` before MERGE                        |
| 3  | **Serverless `.cache()`**  | `PERSIST TABLE not supported` error                    | Skip cache — `.collect()` already materialized   |
| 4  | **`saveAsTable` append**   | Inconsistent behavior on serverless compute            | Use SQL `INSERT INTO` instead                    |

## Interview Answer Framework

1. **Start with Type 1 vs Type 2** — "Type 1 overwrites, Type 2 versions. For audit trails, Type 2 is the standard."
2. **Draw the before/after table** — Show `eff_start`, `eff_end`, `is_current` columns.
3. **Explain why a single MERGE won't work** — This signals real production experience.
4. **Mention edge cases unprompted** — Lazy eval trap, same-day changes, idempotency, new-customer inserts. This is what gets you "strong hire."

## How to Run

1. Import `scd_type_2_delta_merge.py` into your Databricks workspace
2. Update `CATALOG`, `SCHEMA`, `TABLE` at the top of the notebook
3. Run all cells — self-contained with sample data