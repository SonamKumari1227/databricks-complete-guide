# Delta Live Tables: Interview Questions

> Remember the current branding: **Lakeflow Declarative Pipelines**. Interviewers
> may use either name.

```text
1. Fundamentals
2. Dataset Types
3. Expectations
4. CDC and APPLY CHANGES
5. Production
6. Scenario-Based
```

---

# 1. Fundamentals

### What is Delta Live Tables?

A declarative framework for building Delta pipelines. You define datasets and the
queries that produce them; DLT infers the dependency graph, manages checkpoints
and state, enforces quality expectations, handles retries, and refreshes tables
incrementally where it can.

### Declarative vs imperative — what actually changes?

```text
Imperative (Workflows + notebooks):
  you write reads, writes, checkpoints, task dependencies, retries, quality checks

Declarative (DLT):
  you write what each table contains; the framework owns the rest
```

### How does DLT determine execution order?

It parses the code and records every `dlt.read` / `dlt.read_stream` /
`LIVE.<table>` reference, building the DAG from those edges. Dependencies are
never declared manually, and a broken reference fails at validation time.

### DLT vs Workflows — when do you use each?

DLT builds Delta tables from Delta tables. Workflows orchestrates anything: ML
training, API calls, exports, dbt, dashboard refreshes. The common production
shape is a Workflow with a `pipeline_task` that runs DLT, surrounded by other
tasks.

### What does DLT give you for free?

Ordering, parallelism, checkpoint and state management, table creation, retries
with backoff, quality metrics, lineage in Unity Catalog, an event log, and
incremental refresh where provable.

### Python or SQL?

Both are fully capable. SQL suits SQL-first teams. Python is required when you
want loops, parameterisation, or metadata-driven generation of many similar
tables.

---

# 2. Dataset Types

### What are the three dataset types?

```text
Streaming table    → persisted, processes each source row exactly once
Materialized view  → persisted, stores a query result, refreshed
View               → not persisted, recomputed wherever referenced
```

### When do you use a streaming table vs a materialized view?

Streaming table for append-only sources where each row should be processed once —
ingestion and append-style silver. Materialized view for aggregations and joins
where recomputing from current state is correct.

### Why would a streaming table fail with "data was updated or deleted"?

Its source is not append-only — an upstream MERGE rewrote files. Fix with
`skipChangeCommits` if those changes can be ignored, or use a materialized view
or Change Data Feed if they matter.

### Why does a materialized view sometimes recompute fully?

Non-deterministic expressions such as `current_timestamp()` in the output, Python
UDFs, or unsupported operations prevent DLT from proving that an incremental
refresh is correct.

### What is a full refresh, and when is it dangerous?

It clears state and reprocesses from the source. It is dangerous for a streaming
table whose source files have been archived or expired, because the history no
longer exists to reprocess.

---

# 3. Expectations

### What are expectations?

Declarative row-level data quality rules attached to a dataset, with automatic
pass and fail counts recorded in the event log.

### What are the three severities?

```text
@dlt.expect          → keep the row, record the violation
@dlt.expect_or_drop  → drop the row, record the violation
@dlt.expect_or_fail  → fail the pipeline update
```

### How do you choose a severity?

Plausibility checks warn; structural problems drop; contract violations fail.
Use `expect_or_fail` sparingly — it halts the whole pipeline, which is right for
regulatory feeds and wrong for a noisy vendor field.

### How do you keep rows that `expect_or_drop` removes?

The quarantine pattern: a second table over the same source with the inverse
filter, plus a boolean flag column per failed rule and a quarantine timestamp.

### Where do expectation metrics live?

The pipeline event log, queryable with `event_log(TABLE(...))`, which lets you
build pass-rate dashboards and alerts.

### Can an expectation check freshness or row counts?

No — expectations are evaluated per row. Build a metrics table of counts and
timestamps, then alert on it with Databricks SQL.

### How do you detect orphan foreign keys declaratively?

Left join the dimension and add an expectation that the joined column is not
null. The failure count is the orphan count, recorded on every run.

---

# 4. CDC and APPLY CHANGES

### What is APPLY CHANGES INTO?

A declarative CDC operation that upserts a change stream into a target table,
handling out-of-order events, collapsing multiple events per key, applying
deletes, and supporting SCD Type 1 and Type 2.

### What problems does it remove compared to a hand-written MERGE?

```text
✔ Deduplicating multiple events per key within a batch
✔ Ordering events correctly (sequence_by)
✔ Distinguishing insert, update, and delete
✔ Maintaining SCD2 validity columns and current flags
✔ Making reprocessing idempotent
```

### What does `sequence_by` do?

Defines the ordering column that decides which version of a key wins when events
arrive out of order or several arrive in the same batch.

### How do you implement SCD Type 2?

`stored_as_scd_type=2`, with optional `track_history_column_list` so only
meaningful changes create a new version. DLT maintains `__START_AT` and
`__END_AT`.

### How do you query an SCD2 table point-in-time?

Join on the key where the event date falls between `__START_AT` and `__END_AT`
(treating a null `__END_AT` as still current).

### Can you do CDC without an external CDC tool?

Yes, if the source is a Delta table: enable Change Data Feed, read it with
`readChangeFeed`, and feed it into `apply_changes` sequenced by `_commit_version`.

### What are the limitations of APPLY CHANGES?

The target cannot also be defined by a normal `@dlt.table`, `sequence_by` must be
a single column, and the target is not append-only so downstream streaming reads
need care.

---

# 5. Production

### What must change between dev and prod?

```text
development: true → false   (retries on, clusters terminate)
catalog: dev_catalog → main
schedule: paused → active
run_as: a person → a service principal
```

### How do you deploy a DLT pipeline?

Defined in Git as an Asset Bundle with per-target variables, deployed with
`databricks bundle deploy --target prod`, never edited in the UI in production.

### What is the event log used for?

Update outcomes, rows written per flow, expectation results, and errors —
queried with `event_log(TABLE(...))` and turned into persistent views for
dashboards and alerts.

### Which alerts should a DLT pipeline have?

Pipeline failure notifications, expectation fail-rate alerts, row-count anomaly
alerts, and a table freshness alert that catches a pipeline paused or never
triggered.

### What drives DLT cost?

Continuous mode when triggered would do, accidental full recomputes of
materialized views, oversized clusters, and development mode left on in
production.

### How do you test a DLT pipeline?

Extract transformations into plain functions and unit test them in CI, run the
full pipeline against sample data in a dev catalog, and rely on expectations as
continuous contract tests in production.

---

# 6. Scenario-Based

### Migrate an existing Workflows pipeline to DLT. What is your plan?

```text
1. Map each notebook task to a dataset (streaming table or materialized view)
2. Replace explicit writes with a returned DataFrame from @dlt.table
3. Replace task dependencies with dlt.read / dlt.read_stream
4. Convert validation code to expectations, add quarantine tables
5. Replace CDC and SCD2 MERGE logic with apply_changes
6. Run both pipelines in parallel against the same source, compare row
   counts and key aggregates
7. Switch consumers, retire the old pipeline

Watch out for: streaming tables needing append-only sources, and full
refresh requiring the source history to still exist.
```

### A gold materialized view recomputes 5 TB every night. Why, and how do you fix it?

```text
Likely causes:
- A current_timestamp() or current_date() column in the output
- A Python UDF in the transformation
- A window function over the whole table
- A non-deterministic join condition

Fix:
- Remove non-deterministic expressions from the output
- Replace the UDF with built-in SQL functions
- If incremental refresh is impossible, scope the query to a recent
  window and MERGE, or restructure as a streaming table
```

### One malformed vendor row halts the pipeline every few days. What went wrong?

```text
An expect_or_fail was applied to a rule that is not a hard contract.

Fix:
- Downgrade it to expect_or_drop and add a quarantine table
- Alert on the quarantine volume instead of failing the update
- Reserve expect_or_fail for cases where publishing nothing is better
  than publishing something — a missing required column, for example
```

### Business asks for both "current customer state" and "customer history". Design it.

```text
One validated CDC view feeding two apply_changes targets:

silver_customers          → stored_as_scd_type=1 (current state)
silver_customers_history  → stored_as_scd_type=2, track_history_column_list

Gold tables read silver_customers for current reporting, and join
silver_customers_history on __START_AT / __END_AT for point-in-time
analysis such as "revenue by the segment the customer was in at order time".
```

### The DLT bill tripled last month. Investigate.

```text
1. system.billing.usage filtered on usage_metadata.dlt_pipeline_id
2. Was the pipeline switched to continuous mode?
3. Was development mode enabled in production (warm clusters, no termination)?
4. Did a materialized view start recomputing fully — check rows written
   per run in the event log for a step change
5. Did the cluster max_workers get raised?
6. Did a full refresh get triggered repeatedly?

Prevention: alert on daily DBUs per pipeline, and review rows-written
trends in the event log dashboard.
```

### Compare DLT expectations with hand-written quality checks.

| | Hand-written | DLT expectations |
|---|-------------|------------------|
| Location | Scattered in code | Declared on the table |
| Metrics | You build them | Automatic in the event log |
| History | You store it | Retained per run |
| Severity control | Custom `if` logic | Three built-in levels |
| Skippable | Easily | Not without removing the decorator |

```text
DLT does not make the rules smarter — it makes them declared, measured,
and impossible to quietly bypass.
```

---

## Rapid-Fire Recap

```text
DLT = declarative pipelines; you define WHAT, DLT handles HOW
DAG inferred from dlt.read / LIVE. references

Datasets:
streaming table (each row once, append-only source)
materialized view (stored result, incremental if provable)
view (not persisted)

Expectations:
expect | expect_or_drop | expect_or_fail  (+ expect_all_* variants)
metrics land in the event log → dashboard + alert
quarantine = second table with the inverse filter

CDC:
apply_changes: keys, sequence_by, apply_as_deletes, stored_as_scd_type 1 or 2
SCD2 gives __START_AT / __END_AT; track_history_column_list limits noise

Production:
development=false | triggered not continuous | serverless | Asset Bundles
service principal | Workflow pipeline_task for everything non-table
event_log(TABLE(...)) for observability
cost = continuous mode + accidental full recomputes
```
