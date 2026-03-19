# PySpark Interview Scenarios

> Battle-tested PySpark code for the questions that actually come up in **Senior Data Engineer** interviews.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Databricks](https://img.shields.io/badge/Databricks-Runtime%2017.0%2B-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-4%2B-00ADD8?logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4%2B-E25A1C?logo=apachespark&logoColor=white)

---

## What Is This?

A growing collection of **self-contained Databricks notebooks** — each one tackles a real interview scenario end-to-end with working code, sample data, and the pitfalls most candidates miss.

No theory dumps. No "it depends." Just the code that works and the traps that don't.

## Scenarios

| #  | Scenario                     | Key Concepts                                  | Status        |
|----|------------------------------|-----------------------------------------------|---------------|
| 01 | Deduplication                | `row_number()`, window functions, idempotency | Coming soon   |
| 02 | Top N per Group              | `rank()` vs `dense_rank()`, partition pruning | Coming soon   |
| 03 | Handling Data Skew           | Salting, broadcast joins, AQE                 | Coming soon   |
| **04** | **[SCD Type 2 MERGE](scenarios/04-scd-type-2/)** | **Delta MERGE, two-step pattern, lazy eval trap** | **✅ Available** |
| 05 | Exploding Nested JSON        | `explode()`, `from_json()`, schema inference  | Coming soon   |

> Each scenario folder has its own README with the interview question, approach, pitfalls, and expected output.

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/sudo-cs-dev/pyspark-interview-scenarios.git

# 2. Import any .py file into your Databricks workspace
#    (Repos → Add Repo → paste the URL, or drag-drop the .py file)

# 3. Update CATALOG / SCHEMA / TABLE to match your environment

# 4. Run all cells — every notebook is self-contained with sample data
```

## Requirements

- **Databricks Runtime 17.3+** (Delta Lake 4.0+)
- **Unity Catalog** enabled workspace
- Works on both **serverless** and **classic** compute
- No external libraries needed — everything uses built-in PySpark + Delta

## Repo Structure

```
pyspark-interview-scenarios/
├── README.md              ← You are here
├── LICENSE
├── .gitignore
└── scenarios/
    └── 04-scd-type-2/
        ├── README.md       ← Interview question, approach, pitfalls
        └── scd_type_2_delta_merge.py  ← Databricks notebook
```

As more scenarios ship, each gets its own folder under `scenarios/`.

## Who Is This For?

- **Interview candidates** preparing for Senior/Staff Data Engineer roles
- **Working DEs** who want tested patterns they can drop into production
- **Content creators** looking for accurate code to reference


## About

Built by **Chandra Shekhar Som** (CS) — Lead Data Engineer with 6+ years in Azure data engineering across Databricks, Delta Lake, ADF, Fabric, and Power BI.

This repo is the companion code for the **PySpark Interview Scenarios** carousel series on LinkedIn and Instagram.

[![Instagram](https://img.shields.io/badge/@sudo.cs__-E4405F?logo=instagram&logoColor=white)](https://instagram.com/sudo.cs_)
[![LinkedIn](https://img.shields.io/badge/c--shekhar1029-0A66C2?logo=linkedin&logoColor=white)](https://linkedin.com/in/c-shekhar1029)
[![GitHub](https://img.shields.io/badge/sudo--cs--dev-181717?logo=github&logoColor=white)](https://github.com/sudo-cs-dev)

---

*If this helped you land an offer, star the repo and let me know — that's the best feedback.* ⭐
