# Data Engineering Patterns: Interview Questions

```text
1. Medallion Architecture
2. Bronze Layer
3. Silver Layer
4. Gold Layer
5. Batch and Incremental
6. Scenario-Based
```

---

# 1. Medallion Architecture

### What is the medallion architecture?

A layered design — bronze (raw), silver (cleaned and conformed), gold (business
aggregates) — where each layer has one responsibility and each can be rebuilt
from the one before it.

### Why three layers rather than one transformation?

```text
Reprocessing  → fix a bug and rebuild silver without re-extracting from source
Auditing      → prove what the source actually sent
Separation    → technical correctness (silver) vs business definitions (gold)
Debugging     → isolate where a wrong number was introduced
```

### One-line summary of each layer?

```text
Bronze = what the source said
Silver = what is true
Gold   = what the business asks about
```

### Which layer holds business logic?

Gold. Silver holds technical rules that are true regardless of who asks. Putting
business rules in silver means every new question forces a silver rewrite.

### Should dashboards query silver?

No. Silver is row-level and unaggregated, so queries are slow and each dashboard
re-implements business definitions, producing conflicting numbers.

### Does medallion apply to streaming?

Yes. The layers are identical; only the trigger and write mechanics change.

### When would you add a fourth layer?

A landing zone before bronze for files as delivered, or a certified/platinum
layer for governed executive datasets. Add layers only for a concrete reason —
each one costs storage and latency.

---

# 2. Bronze Layer

### What is the bronze layer?

An append-only Delta table holding source data exactly as received, plus
ingestion metadata, retained long enough to rebuild all downstream layers.

### Why keep raw data if it is messy?

Because source systems purge history you cannot recover, and because every
downstream bug becomes unfixable if the original is gone.

### Which metadata columns belong in bronze?

`_ingested_at`, `_source_file`, `_batch_id`, `_source_system`, and file
modification time. `_batch_id` makes a single bad load surgically deletable.

### Why store bronze columns as strings?

So a source changing a column's format does not break ingestion. Casting belongs
in silver, where failures can be quarantined deliberately.

### Should bronze deduplicate?

No. Duplicates are part of what arrived. Silver decides which version wins.

### What is `_rescued_data`?

An Auto Loader column capturing fields that did not match the expected schema, so
unexpected source changes are preserved rather than silently dropped.

### How is bronze ingestion made idempotent?

Auto Loader checkpoints track processed files exactly once. Custom loaders need a
processed-files control table and an anti-join.

### Who should have access to bronze?

Engineers only. Bronze contains duplicates, untyped columns, and known-bad rows;
an analyst querying it will produce a wrong number.

---

# 3. Silver Layer

### What happens in silver?

Casting, validation, deduplication, conforming values, and reference enrichment —
producing one row per business entity with enforced types.

### Why does a failed cast not raise an error in Spark?

Spark returns null on cast failure. You must compare pre- and post-cast nullness
to detect it, or data is silently lost.

### How do you deduplicate correctly?

`row_number()` over a window partitioned by the business key and ordered by a
meaningful timestamp, keeping row 1. `dropDuplicates` selects arbitrarily and is
only safe for byte-identical rows.

### Why is MERGE preferred over append in silver?

It makes the write idempotent, so retries and reprocessing produce the same
result rather than duplicating rows.

### What does this MERGE guard do?

```sql
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
```

It prevents an out-of-order reprocess from overwriting a newer record with an
older one.

### Quarantine or fail?

Quarantine isolated bad rows so good data flows; fail when the reject rate
crosses a threshold, which signals a systemic problem rather than noise.

### SCD Type 1 vs Type 2?

Type 1 overwrites and keeps only current state. Type 2 versions rows with
`valid_from`, `valid_to`, and `is_current`, preserving point-in-time history so
historical reports do not change retroactively.

### Should silver join fact tables together?

No. Join reference and lookup data in silver; fact-to-fact joins belong in gold,
at the required grain.

---

# 4. Gold Layer

### What is the gold layer?

The consumption layer: business rules applied once, data aggregated and modelled
for dashboards, reports, and ML.

### Star schema or wide aggregate tables?

Star schema for flexible self-service exploration; wide pre-aggregated tables for
known repeated dashboard queries. Mature platforms use both.

### What is grain and why does it matter?

Grain is what one row represents. Every join, aggregation, and metric depends on
it; mismatched grain is the most common cause of double counting.

### How do you refresh gold incrementally?

MERGE recomputed aggregates for a recent window keyed on the grain columns, or
use a materialized view and let Databricks manage refresh.

### Why does the incremental gold query look back several days?

To pick up late-arriving data. Recomputing only today leaves older dates
permanently wrong.

### How do you stop three dashboards reporting three different revenues?

Define the metric once in gold and require all consumers to read that definition
rather than recomputing from silver.

### Why document gold tables?

Gold is consumed by people who did not build it. Comments define the grain and
the business rules, and they feed Genie and catalog search.

---

# 5. Batch and Incremental

### Full load vs incremental load?

Full reloads everything each run — simple, self-healing, expensive. Incremental
processes only changes — cheaper but needs watermarks or CDC plus idempotency.

### When is a full reload the right answer?

Small tables, reference data, and dimensions. A correct full reload beats a
subtly broken incremental load.

### How does a watermark load work?

Read the last processed timestamp from a control table, select source rows newer
than it, MERGE them, then advance the watermark only after the write succeeds.

### Why update the watermark last?

Updating it first means a failure permanently skips that window, silently losing
data. Updating it last means a failure simply reprocesses, which is safe because
MERGE is idempotent.

### Why do watermark loads miss deletes?

A deleted row has no change timestamp to detect. You need soft deletes, CDC, or
periodic full reconciliation.

### What is CDC?

Change Data Capture reads the database transaction log and emits insert, update,
and delete events in commit order, capturing changes a watermark cannot see.

### What is Delta Change Data Feed?

A Delta feature exposing row-level changes to a Delta table with `_change_type`
values, letting downstream tables update incrementally instead of rebuilding.

### Why collapse CDC events before MERGE?

MERGE fails when multiple source rows match one target row, so you keep only the
latest event per key using `row_number()` ordered by commit time.

### How do you size the late-arrival lookback window?

Measure it: compute the p50/p99/max of `ingested_date - event_date` over history
and cover the p99, plus a periodic full recompute for the long tail.

### What is reconciliation and why does it matter?

Comparing source and target counts and sums on a schedule. Without it, an
incremental pipeline can drift and be quietly wrong for months.

---

# 6. Scenario-Based

### Design a pipeline for 200 source tables from one ERP.

```text
Control table: table_name, source_query, load_type, watermark_col, is_active
Bronze: For Each task over the active list, generic ingest notebook, concurrency 10
Silver: generic transform driven by a config table of rules per table
Gold:   hand-written per business mart, because business logic is not generic
Workflow: parent job with Run Job children per layer
Monitoring: per-table row counts logged to control.pipeline_runs, alerts on drift
```

### The nightly pipeline is slow and getting slower. Diagnose.

```text
1. Which layer? Check task durations in the run history trend.
2. Bronze slow  → source extraction, JDBC parallelism, file listing
3. Silver slow  → full reload where incremental would do; small files; skew
4. Gold slow    → rebuilding everything nightly instead of MERGE on a window
5. Check DESCRIBE DETAIL for small file counts on the hot tables
6. Check the Spark UI for shuffle spill and skewed partitions
7. Parallelise independent tasks that were chained out of habit
```

### A business user says last month's revenue changed. Explain and prevent.

```text
Explain:
- DESCRIBE HISTORY on the gold table shows when it changed
- Compare VERSION AS OF n EXCEPT VERSION AS OF n-1
- Likely causes: late-arriving data, an SCD1 dimension overwritten,
  or a business rule change applied retroactively

Prevent:
- Use SCD2 dimensions so historical joins stay point-in-time correct
- Snapshot published gold monthly for immutable reporting
- Document that a rolling window intentionally restates recent days
- Version the business logic and record it in the pipeline run log
```

### Half a file loaded and the job crashed. What is the state?

```text
Bronze with Auto Loader: the checkpoint only commits fully processed files,
so a retry reprocesses the incomplete file exactly once. No duplicates.

Bronze with a custom append: partial rows may have landed. Delete by
_batch_id and re-run, which is exactly why _batch_id exists.

Silver with MERGE: idempotent, simply re-run.

Gold with CREATE OR REPLACE: atomic, either the old or new version exists.
Gold with multiple appends: use a staging table and one final swap instead.
```

### Source added three new columns without telling you. What happens?

```text
Bronze: with schemaEvolutionMode = rescue, the data lands in _rescued_data
        and nothing breaks. An alert on _rescued_data volume tells you.
Silver: unaffected, because it selects explicit columns.
Action: decide deliberately whether the new columns matter, then add them
        to the silver select and backfill from bronze — which is possible
        precisely because bronze kept everything.
```

### You must support both a real-time dashboard and an accurate daily report.

```text
Streaming path: Auto Loader / Kafka → bronze → streaming silver → rolling gold
                low latency, approximate, good enough for operational views

Batch path:     nightly recompute of gold over a lookback window
                reconciled, corrected, the number of record for reporting

Both read the same bronze, so the two paths cannot diverge on source data.
Document clearly which table is authoritative for which purpose.
```

---

## Rapid-Fire Recap

```text
Bronze = what the source said | append-only | string types | metadata columns
Silver = what is true         | cast, validate, dedupe, conform, MERGE
Gold   = what the business asks | aggregates, business rules, documented grain

Incremental:
watermark (no deletes) | CDC (all changes) | Auto Loader (files) | CDF (Delta)

Golden rules:
✔ Never transform into bronze
✔ Technical rules in silver, business rules in gold
✔ Advance the watermark only after a successful load
✔ MERGE with an ordering guard
✔ Collapse CDC events per key before MERGE
✔ Size the late-arrival window from measured data
✔ Reconcile on a schedule, or be quietly wrong
```
