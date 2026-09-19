# Orchestration Patterns

Patterns are reusable answers to problems that come up in almost every data
platform. Recognising the pattern saves you from designing from scratch.

---

## Pattern 1: Medallion Pipeline

The default shape of a Databricks data platform.

```mermaid
flowchart LR
    S[(Source systems)] --> B[Bronze<br/>raw, as-is]
    B --> SI[Silver<br/>cleaned, conformed]
    SI --> G[Gold<br/>business aggregates]
    G --> D[Dashboards / ML]
```

As a job:

```mermaid
flowchart TD
    A[ingest_bronze] --> B[validate_bronze]
    B --> C[build_silver_orders]
    B --> D[build_silver_customers]
    C --> E[build_gold_sales]
    D --> E
    E --> F[refresh_dashboard]
    E --> G[quality_checks]
```

```text
When to use: nearly every batch platform
Why it works: each layer has one job, and failures are isolated to a layer
```

---

## Pattern 2: Ingest, Transform, Publish Separation

Split the medallion job into three jobs when different teams or schedules own
different layers.

```mermaid
flowchart TD
    J1[Job: ingestion<br/>hourly] --> T1[(bronze.*)]
    T1 -- table update trigger --> J2[Job: transformation<br/>runs when bronze lands]
    J2 --> T2[(silver.*)]
    T2 -- table update trigger --> J3[Job: publishing<br/>runs when silver lands]
    J3 --> T3[(gold.*)]
```

```text
Benefit: teams deploy independently, coupled only by the table contract
Cost:    harder to see the end-to-end picture in one screen
```

---

## Pattern 3: Parent and Child Jobs

```mermaid
flowchart TD
    P[Parent: daily_master]
    P --> A[Run Job: ingestion]
    A --> B[Run Job: transformation]
    B --> C[Run Job: publishing]
    C --> D[Run Job: quality_gate]
```

Same layering as pattern 2, but with a single parent job that owns the order.
Use this when you want one place to see the whole run, while keeping each child
job independently runnable.

| | Pattern 2 (triggers) | Pattern 3 (parent job) |
|---|---|---|
| Coupling | Loose | Tight |
| End-to-end visibility | Low | High |
| Ownership | Per team | Central |
| Re-run the whole flow | Awkward | One click |

---

## Pattern 4: Fan-Out Ingestion with For Each

Fifty source tables, one piece of logic.

```mermaid
flowchart TD
    A[get_table_config<br/>reads a control table] --> B[For Each table<br/>concurrency 10]
    B --> C1[ingest orders]
    B --> C2[ingest customers]
    B --> C3[... 48 more]
    C1 --> D[consolidate_and_log]
    C2 --> D
    C3 --> D
```

The control table drives everything:

```sql
CREATE TABLE control.ingestion_config (
    table_name    STRING,
    source_path   STRING,
    load_type     STRING,   -- full | incremental
    watermark_col STRING,
    is_active     BOOLEAN
);
```

```python
# Task: get_table_config
rows = spark.table("control.ingestion_config").filter("is_active").collect()
tables = [r.table_name for r in rows]
dbutils.jobs.taskValues.set(key="tables", value=tables)
```

Adding a 51st table becomes an `INSERT`, not a code change. This is called
**metadata-driven ingestion** and it is how most mature platforms work.

---

## Pattern 5: Quality Gate

Stop bad data before it reaches consumers.

```mermaid
flowchart TD
    A[build_silver] --> B[run_quality_checks<br/>sets passed = true/false]
    B --> C{passed?}
    C -- true --> D[publish_gold]
    C -- false --> E[alert_data_steward]
    E --> F[hold_previous_version]
```

```python
checks = {
    "row_count_positive": df.count() > 0,
    "no_null_keys":       df.filter("order_id IS NULL").count() == 0,
    "amounts_sane":       df.filter("amount < 0").count() == 0,
    "freshness":          df.agg({"order_date": "max"}).collect()[0][0] is not None,
}

passed = all(checks.values())
dbutils.jobs.taskValues.set(key="passed", value=passed)
dbutils.jobs.taskValues.set(key="failed_checks", value=[k for k, v in checks.items() if not v])
```

The key idea: **gold stays stale rather than becoming wrong.** Stale data with a
known timestamp is recoverable; wrong data that people already acted on is not.

---

## Pattern 6: Incremental Load with a Watermark

```mermaid
flowchart LR
    A[Read last watermark<br/>from control table] --> B[Read source<br/>WHERE updated_at > watermark]
    B --> C[MERGE into target]
    C --> D[Write new watermark]
```

```python
last = spark.sql(
    "SELECT max(watermark) w FROM control.load_state WHERE table_name = 'orders'"
).collect()[0].w

new_data = spark.table("bronze.orders").filter(f"updated_at > '{last}'")

new_data.createOrReplaceTempView("updates")
spark.sql("""
    MERGE INTO silver.orders t
    USING updates s ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

new_watermark = new_data.agg({"updated_at": "max"}).collect()[0][0]
```

Write the new watermark **only after** the merge succeeds, so a failure means the
next run reprocesses the same window rather than skipping it.

```text
Fail before watermark update → reprocess (safe, because MERGE is idempotent)
Fail after watermark update  → data loss (never do this)
```

---

## Pattern 7: Backfill Job

Keep the backfill separate from the daily job.

```mermaid
flowchart TD
    A[backfill_job<br/>parameters: start_date, end_date] --> B[generate_date_list]
    B --> C[For Each date<br/>concurrency 5]
    C --> D[run same transformation logic]
```

```text
✔ Same notebooks as production, different parameters
✔ Lower concurrency so it does not starve the daily pipeline
✔ Separate job so backfills do not pollute the daily run history
✔ Only possible if the pipeline is date-parameterised and idempotent
```

---

## Pattern 8: Multi-Environment Promotion

```mermaid
flowchart LR
    D[Dev workspace<br/>dev_catalog] --> S[Staging workspace<br/>stg_catalog]
    S --> P[Prod workspace<br/>main catalog]
    G[Git repository] --> D
    G --> S
    G --> P
```

With Databricks Asset Bundles the same definition deploys everywhere:

```yaml
bundle:
  name: sales_platform

targets:
  dev:
    default: true
    variables:
      catalog: dev_catalog
    resources:
      jobs:
        daily_sales_pipeline:
          schedule:
            pause_status: PAUSED      # never auto-run in dev

  prod:
    variables:
      catalog: main
    run_as:
      service_principal_name: "sp-data-platform"
      resources:
        jobs:
          daily_sales_pipeline:
            schedule:
              pause_status: UNPAUSED
```

```bash
databricks bundle deploy --target dev
databricks bundle deploy --target prod
```

Topic 15 covers bundles and CI/CD in depth.

---

## Pattern 9: External Orchestrator Calls Databricks

In many enterprises the master schedule lives in Airflow, Azure Data Factory, or
Control-M, because the flow spans more than Databricks.

```mermaid
flowchart LR
    A[SAP extract] --> O[Airflow / ADF]
    O --> DB[Databricks job run-now]
    DB --> W[Wait for completion]
    W --> S[Salesforce load]
    W --> T[Tableau refresh]
```

```python
# Airflow
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

run_etl = DatabricksRunNowOperator(
    task_id="run_databricks_etl",
    databricks_conn_id="databricks_default",
    job_id=620745,
    job_parameters={"run_date": "{{ ds }}"},
)
```

| Approach | Choose when |
|----------|-------------|
| **Databricks Workflows only** | The pipeline lives entirely in Databricks |
| **External orchestrator** | The flow spans many systems, or the company standardised on one scheduler |
| **Hybrid** | External tool triggers coarse Databricks jobs that own their internal DAG |

The hybrid is usually best: keep Databricks-internal dependencies inside
Databricks, and let the external tool coordinate across systems.

---

## Pattern 10: Streaming plus Batch (Lambda-style)

```mermaid
flowchart TD
    K[(Kafka / Event Hub)] --> S[Continuous job<br/>streaming ingest to bronze]
    S --> B[(bronze.events)]
    B --> D[Daily batch job<br/>rebuild silver + gold]
    D --> G[(gold.daily_metrics)]
    B --> R[Near real-time gold<br/>streaming aggregation]
```

```text
Streaming path → low latency, approximate, rolling metrics
Batch path     → correct, complete, reconciled daily
```

Topics 11, 12, and 13 cover the streaming side in detail.

---

## Anti-Patterns

```text
❌ One notebook, 3000 lines, one task
   → cannot retry a part, cannot parallelise, impossible to debug

❌ Chaining tasks that have no data dependency
   → serial runtime for no reason

❌ Jobs pointing at notebooks in a personal user folder
   → breaks when that person leaves or moves the file

❌ Job owned and run as an individual user
   → use a service principal

❌ Sleep loops to wait for another job
   → use a table update trigger or a Run Job task

❌ Hard-coded dates, paths, and catalogs
   → no backfills, no promotion between environments

❌ Every job scheduled at exactly midnight
   → compute spike, queueing, and noisy failures
```

---

## Choosing a Pattern

```mermaid
flowchart TD
    Q1{How many source tables?}
    Q1 -- Few, distinct logic --> P1[Explicit tasks per table]
    Q1 -- Many, same logic --> P2[For Each + control table]
    P1 --> Q2{Do teams own different layers?}
    P2 --> Q2
    Q2 -- Yes --> P3[Separate jobs + table triggers]
    Q2 -- No --> P4[Single medallion job]
    P3 --> Q3{Does the flow leave Databricks?}
    P4 --> Q3
    Q3 -- Yes --> P5[External orchestrator, hybrid]
    Q3 -- No --> P6[Databricks Workflows only]
```

---

## Quick Revision

```text
Patterns:
1  Medallion pipeline
2  Ingest / transform / publish split via table triggers
3  Parent job with Run Job children
4  Metadata-driven fan-out with For Each
5  Quality gate before publishing
6  Incremental load with a watermark
7  Separate backfill job
8  Multi-environment promotion with bundles
9  External orchestrator, hybrid ownership
10 Streaming plus batch

Principles:
✔ One task = one re-runnable unit
✔ Control tables beat copy-pasted tasks
✔ Stale gold beats wrong gold
✔ Update the watermark only after a successful write
✔ Parameterise everything that differs between runs or environments
```
