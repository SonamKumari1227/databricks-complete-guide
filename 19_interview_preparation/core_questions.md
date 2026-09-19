# The 40 Most-Asked Questions

Cross-topic questions that come up in almost every Databricks interview, with
answers structured the way an interviewer wants to hear them.

---

# Platform Fundamentals

### 1. What is Databricks?

A unified analytics platform built on Apache Spark, combining data engineering,
analytics, and machine learning on a lakehouse architecture — one copy of data
in cloud storage, governed by Unity Catalog, queried by Spark and SQL.

### 2. What is a lakehouse and why does it exist?

```text
Problem: data lakes were cheap and flexible but unreliable — no transactions,
no schema enforcement, poor query performance. Warehouses were reliable and
fast but expensive, closed, and required copying data out of the lake.

Lakehouse: warehouse reliability and performance ON the lake, via an open
table format (Delta). One copy, one governance model, no ETL lag between systems.
```

### 3. Control plane vs data plane?

The control plane is Databricks-managed: UI, REST API, job scheduler, metadata.
The data plane runs in your cloud account where clusters process data, so your
data never leaves your subscription.

### 4. All-purpose vs job cluster?

```text
All-purpose: always on, shared, most expensive DBU rate, for interactive work
Job cluster:  created per run and terminated, cheapest rate, for production
Serverless:   starts in seconds, no configuration, for short or spiky work

Production on an all-purpose cluster is the most common cost anti-pattern.
```

### 5. What is Photon?

A vectorised C++ execution engine replacing parts of the JVM engine for scans,
joins, aggregations, and Delta writes. Higher DBU rate, usually more than offset
by shorter runtime — but it does not accelerate Python UDFs, which fall back.

---

# Delta Lake

### 6. What is Delta Lake?

Parquet files plus a transaction log (`_delta_log`). The log gives ACID
transactions, snapshot isolation, time travel, schema enforcement, and efficient
updates and deletes on object storage.

### 7. How does time travel work, and what limits it?

Every commit creates a version; readers can query `VERSION AS OF` or
`TIMESTAMP AS OF`. `VACUUM` removes files beyond the retention period, which is
the limit — after a 7-day vacuum, older versions are unreadable.

### 8. Schema enforcement vs schema evolution?

Enforcement rejects writes that do not match the schema, protecting data quality.
Evolution allows controlled additions with `mergeSchema`. Enforcement is the
default; evolution is opt-in.

### 9. Why is MERGE important?

It performs an upsert in one atomic operation, which makes writes **idempotent**.
That is what allows retries, repair runs, and reprocessing to be safe — the
foundation of every reliable pipeline.

### 10. What does OPTIMIZE do and when do you need it?

Compacts small files into larger ones. You need it when `DESCRIBE DETAIL` shows a
small average file size (`sizeInBytes / numFiles`), typically caused by streaming
micro-batches, over-partitioning, or many small appends.

### 11. Liquid clustering vs Z-order vs partitioning?

```text
Partitioning       → physical directories, requires a rewrite to change,
                     disastrous on high-cardinality columns
Z-order            → co-locates data within files via OPTIMIZE, must be re-run
Liquid clustering  → supersedes both, keys changeable with ALTER TABLE,
                     handles skew and small files better — default for new tables
```

### 12. What are deletion vectors?

Deleted rows are marked in a side file instead of rewriting whole data files,
making DELETE, UPDATE, and MERGE dramatically faster on large tables. A later
OPTIMIZE applies them physically.

---

# Unity Catalog

### 13. What is Unity Catalog?

Centralised governance for the lakehouse: a three-level namespace
(`catalog.schema.table`), permissions, lineage, auditing, and discovery shared
across all workspaces attached to a metastore.

### 14. Why does a user with SELECT still get permission denied?

They also need `USE CATALOG` and `USE SCHEMA` — all three levels of the namespace
must be granted. Compute permission is a further, separate layer.

### 15. How do you implement row and column level security?

Row filters and column masks: UDFs attached to the table that evaluate group
membership. They cannot be bypassed, unlike dynamic views, which a user with
SELECT on the base table can query around.

### 16. Why grant permissions to groups rather than users?

Onboarding and offboarding become membership changes, permissions stay auditable,
and access does not fragment into per-user grants nobody can review.

---

# Pipelines and Orchestration

### 17. What is the medallion architecture?

Bronze (raw, as delivered), silver (cleaned, typed, deduplicated), gold (business
aggregates). Each layer has one responsibility and can be rebuilt from the layer
before it — which is what makes reprocessing possible.

### 18. Which layer holds business logic?

Gold. Silver enforces technical truth (types, keys, deduplication). Putting
business definitions in silver means every new question forces a silver rewrite.

### 19. Why keep bronze if it is messy?

Because source systems purge history. When you discover a cleaning bug three
months later, bronze is the only way to reprocess without re-extracting data that
no longer exists.

### 20. What happens to downstream tasks when one fails?

They are skipped with status `UPSTREAM_FAILED`; independent branches keep
running; the job is marked failed. A **repair run** re-runs only the failed and
skipped tasks, preserving expensive upstream work.

### 21. How do retries work, and when should you not use them?

`max_retries` with `min_retry_interval_millis` per task. Do not enable them when
the task is not idempotent — a retry would duplicate data — or when the error is
deterministic, where retrying only delays the alert.

### 22. What makes a write idempotent?

`MERGE` on a business key, `replaceWhere` partition overwrite, delete-then-insert
for a batch, or Structured Streaming checkpoints.

### 23. How do you pass values between tasks?

`dbutils.jobs.taskValues.set` and `.get`, or the reference
`{{tasks.<task>.values.<key>}}`. They are small metadata only — data goes through
a Delta table.

---

# Incremental and CDC

### 24. Full load vs incremental load?

Full reloads everything: simple, self-healing, expensive. Incremental processes
only changes: cheaper but needs a watermark or CDC plus careful idempotency. At
1 GB full reload is correct; at 5 TB it is impossible.

### 25. How does a watermark load work, and what is the critical rule?

Read the last processed timestamp from a control table, extract newer rows, MERGE
them, then advance the watermark **only after** the write succeeds. Advancing
first turns a transient failure into silent permanent data loss.

### 26. Why do watermark loads miss deletes?

A deleted row has no updated timestamp to detect. You need soft deletes, a CDC
feed, or periodic reconciliation.

### 27. What is CDC, and what must you do before MERGE?

Change Data Capture reads the database transaction log and emits inserts,
updates, and deletes in order. Before MERGE you must collapse to one event per
key with `row_number()` ordered by the sequence column, or MERGE fails with
"multiple source rows matched".

### 28. SCD Type 1 vs Type 2?

Type 1 overwrites, keeping current state only. Type 2 versions rows with
`valid_from`, `valid_to`, and `is_current`, so historical reports remain
point-in-time correct when an attribute changes.

---

# Streaming

### 29. What is Structured Streaming?

A stream processing engine treating a stream as an unbounded table, so the same
DataFrame and SQL API works for batch and streaming. Execution is micro-batch.

### 30. What is a checkpoint and how does it give exactly-once?

A durable directory of offsets, commits, and state. Offsets are written before
processing, commits after the sink write; a batch with offsets but no commit is
reprocessed on restart. Combined with a replayable source and a transactional
sink such as Delta, the result is exactly-once.

### 31. Event time vs processing time, and what is a watermark?

Event time is when something happened (a data column); processing time is when
Spark handled it. A watermark is how late an event may arrive and still count —
computed as max observed event time minus a delay. It finalises windows and
bounds state.

### 32. Why does a streaming aggregation need a watermark?

Without one, Spark must retain every window forever in case a late event arrives,
so state grows unbounded — and in append mode it can never decide a window is
final.

### 33. What is `trigger(availableNow=True)` and why does it matter commercially?

It processes all available data then stops, giving streaming semantics with batch
economics. For latency budgets of five minutes or more it is often ten times
cheaper than an always-on stream.

### 34. When would you avoid a stream-stream join?

When the requirement is "update this record when a related event arrives".
`foreachBatch` plus a Delta MERGE uses the table as state, removing watermark
tuning and handling arbitrarily late events.

---

# Performance

### 35. How do you identify data skew?

In the Spark UI, the maximum task duration or shuffle read within a stage is far
above the median. Confirm with a row count per join key.

### 36. How do you fix skew, in order?

```text
1. AQE skew join (enabled by default — tune factor and threshold)
2. Remove null and sentinel keys (-1, 'UNKNOWN') — usually the actual cause
3. Pre-aggregate before joining
4. Salt only the hot keys
5. Change join strategy or raise the broadcast threshold
```

### 37. What does AQE do?

Re-optimises at runtime using real statistics: coalescing small shuffle
partitions, converting joins to broadcast, and splitting skewed partitions.

### 38. Why is `WHERE year(order_date) = 2026` slow?

A function on the column defeats file-level pruning, so every file must be read.
Use a range predicate on the bare column.

---

# Operations

### 39. Which four alerts does every pipeline need?

```text
1. Failure   → it crashed
2. Freshness → it never ran (a paused job produces no failure to alert on)
3. Quality   → it ran but wrote bad data
4. Duration  → it ran correctly but missed the SLA
```

Only the first produces an error message, which is why teams with only failure
alerts still get surprised.

### 40. How do you investigate a wrong number in a gold table?

```text
1. DESCRIBE HISTORY — did the table change, and when?
2. Version diff — exactly which rows changed
3. Job run history — did the pipeline run and succeed?
4. Control table — volume anomaly against the baseline?
5. Quarantine table — were valid rows rejected?
6. Bronze _rescued_data — did the source schema change?
7. Lineage — what else consumed the bad data?
```

---

## Rapid-Fire Recap

```text
Delta      = Parquet + transaction log → ACID, time travel, MERGE
UC         = catalog.schema.table + grants + lineage + masks/filters
Medallion  = bronze (raw) → silver (true) → gold (business)
Idempotent = MERGE | replaceWhere | checkpoints
Incremental= watermark (advance AFTER success) | CDC (sees deletes)
SCD2       = valid_from / valid_to / is_current → point-in-time correctness
Streaming  = unbounded table, checkpoints, event time, watermarks
availableNow = streaming semantics, batch cost
Skew       = task max >> median → nulls/sentinels → AQE → salting
Layout     = liquid clustering > Z-order > partitioning; OPTIMIZE
Alerts     = failure + freshness + quality + duration
Production = job clusters, service principals, bundles, CI/CD
```
