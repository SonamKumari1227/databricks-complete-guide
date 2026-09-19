# Monitoring and Troubleshooting: Interview Questions

```text
1. Observability Fundamentals
2. System Tables
3. Spark UI and Logs
4. Errors
5. Incident Response
6. Scenario-Based
```

---

# 1. Observability Fundamentals

### What should you monitor in a data platform?

Availability, freshness, quality, performance against SLA, cost, and access — not
just whether jobs succeeded.

### Name the four failure modes of a pipeline.

```text
1. It crashed              → job failure alert
2. It never ran            → freshness alert
3. It ran but wrote bad data → quality alert
4. It ran correctly but too slowly → duration alert
```

Only the first produces an error message.

### Why is a freshness alert essential when you already alert on failure?

A job that never runs — paused schedule, deleted trigger, disabled service
principal, broken notification queue — produces nothing to fail, so no failure
alert can fire.

### What is the value of a control table of pipeline runs?

History and trends that built-in telemetry does not give: rows in and out per
table per day, quarantine counts, duration trends — the evidence needed for
anomaly alerts, SLO reporting, and post-incident analysis.

### How do you avoid alert fatigue?

Severity-based routing, retrigger intervals, an owner and runbook per alert,
messages that state what is wrong and what to do, and deleting alerts nobody
acts on.

### SLA vs SLO?

The SLA is the promise to the business ("fresh by 08:00"); the SLO is a stricter
internal target that protects it ("complete by 07:15 on 99% of days"), with an
error budget for the remainder.

### How do you use lineage operationally?

To find the blast radius of a bad table, trace a wrong number to its source, and
identify unused tables that can be deprecated.

---

# 2. System Tables

### What are system tables?

Databricks-managed Delta tables in the `system` catalog exposing platform
telemetry — job runs, billing, query history, audit, lineage — queryable with
ordinary SQL.

### Which schemas matter and what do they contain?

```text
system.lakeflow → jobs, tasks, run timelines
system.billing  → DBU usage and list prices
system.query    → SQL warehouse query history
system.access   → audit logs and table lineage
system.compute  → cluster and warehouse metadata
```

### How do you report on job failures across a workspace?

Join `system.lakeflow.job_run_timeline` to `system.lakeflow.jobs`, aggregate by
job and result state, and build a dashboard plus an alert.

### How do you find which task inside a job is the bottleneck?

`system.lakeflow.job_task_run_timeline`, grouped by `task_key` with average and
maximum durations.

### How do you attribute cost to a team?

`system.billing.usage` grouped by `custom_tags.team`, with tags made mandatory by
cluster policy, or by `usage_metadata.job_id` joined to job names.

### How do you find queries with poor data pruning?

`system.query.history`, comparing `read_rows` to `produced_rows` — a very high
ratio means files are being read that could have been skipped.

### How do you detect jobs that stopped running?

Left join `jobs` to `job_run_timeline` and filter where the maximum run time is
older than the expected interval.

### What are the limitations of system tables?

They must be enabled per metastore, have finite retention, and have some
ingestion latency — so snapshot what you need for long-term reporting.

---

# 3. Spark UI and Logs

### Where do you start when a job is slow?

The Spark UI: find the slowest stage, then read the task summary metrics — the
distribution of duration and shuffle read across tasks.

### What does max task duration far above the median indicate?

Data skew: one partition holds disproportionately more data.

### What does spill in a stage mean?

Tasks needed more memory than available. Fix by reducing partition size, fixing
skew, or giving executors more memory per core — before adding nodes.

### What does the SQL/DataFrame tab show that the Stages tab does not?

The physical plan annotated with real metrics: files pruned, rows per operator,
join strategy, and where Photon fell back.

### What does `BroadcastNestedLoopJoin` in a plan mean?

Usually a missing equality join condition, producing a near-cartesian join.

### Where do cluster logs go after termination?

Nowhere, unless `cluster_log_conf` delivery to a volume or cloud storage was
configured beforehand.

### What is the cluster event log useful for?

Node loss from spot reclamation, driver not responding, init script failures, and
cloud capacity errors — many apparent Spark failures are actually cluster events.

### How do you monitor a running stream?

`lastProgress`: input versus processed rate, batch duration, state row count,
watermark, and rows dropped by watermark — persisted via a
`StreamingQueryListener`.

### What do `DESCRIBE DETAIL` and `DESCRIBE HISTORY` tell you?

`DESCRIBE DETAIL` gives physical health — file count, size, clustering — which
reveals small-file problems. `DESCRIBE HISTORY` gives the change log: which
operation changed the table, when, and how many rows.

---

# 4. Errors

### What causes driver OOM?

Pulling too much to the driver: `collect()`, `toPandas()`, a huge result display,
or an oversized broadcast.

### What causes executor OOM, and in what order do you address it?

Oversized partitions, skew, large broadcasts, or excessive caching. Address skew
first, then increase shuffle partitions, then reduce the broadcast threshold,
then unpersist caches, and only then change node types.

### How do you resolve `ConcurrentAppendException`?

Make writes provably disjoint with `replaceWhere` or a partition predicate,
cluster so writers touch different ranges, or serialise them with
`max_concurrent_runs = 1`.

### What causes "multiple source rows matched a target row" in MERGE?

Duplicate keys in the source. Deduplicate with `row_number()` ordered by a
sequence column before merging.

### A user granted SELECT still gets permission denied. Why?

They also need `USE CATALOG` and `USE SCHEMA` — all three levels of the namespace
must be granted.

### Why did a cast produce nulls rather than failing?

Spark returns null on cast failure by design. Detect it by comparing pre- and
post-cast non-null counts, or route failures to quarantine.

### Why does a streaming query fail after a code change?

The change altered a stateful operator, so the existing checkpoint is
incompatible. A new checkpoint directory and a reprocessing plan are needed.

### A long-running job failed after an employee left. Why?

It ran under that person's identity. Production jobs must run as a service
principal.

### Auto Loader is not picking up new files. What do you check?

`pathGlobFilter` exclusions, whether the files are already recorded in the
checkpoint, and — in notification mode — lost events, which is why
`cloudFiles.backfillInterval` should always be set.

---

# 5. Incident Response

### Walk through your incident process.

Detect, assess impact via lineage and query history, contain by pausing the
pipeline and restoring the table, diagnose whether code, data, platform, or
schedule changed, fix and backfill, verify against an independent source,
communicate throughout, and hold a review that produces an alert, a test, and a
guardrail.

### Why contain before diagnosing?

Bad data spreads — dashboards are read, exports sent, decisions made. Containment
is cheap and reversible; diagnosis takes time.

### How do you roll back bad data?

`DESCRIBE HISTORY` to find the last good version, then `RESTORE TABLE ... TO
VERSION AS OF`, within the VACUUM retention window.

### What makes backfilling straightforward?

Bronze retained the raw data, writes are idempotent, and dates are parameterised —
then a backfill is a loop over run dates rather than a code change.

### What is the most important number in a post-incident review?

Time to detect. A fast fix after two days of undetected wrongness means the
monitoring failed.

### What should an incident communication say?

What is wrong, who is affected, what not to use, and when the next update will
arrive — issued before the root cause is known.

### What belongs in a runbook?

Symptoms, owner, impact, triage steps, common causes with fixes, containment
actions, recovery procedure, and escalation — written before the incident and
linked from every alert.

---

# 6. Scenario-Based

### An executive says last month's revenue looks wrong. What do you do?

```text
1. Contain: annotate or hide the dashboard; do not let more decisions be made
2. DESCRIBE HISTORY on the gold table → when did it last change?
3. Version diff → exactly which rows changed
4. control.pipeline_runs → volume anomalies in that window
5. Quarantine table → were valid rows rejected?
6. Bronze _rescued_data → did the source change?
7. Lineage → what else consumed this table?
8. Fix, reprocess from bronze, verify against silver aggregates
9. Communicate cause, fix, and verification
10. Add the alert that would have caught it on day one
```

### The pipeline has succeeded every day for a week but the dashboard is stale. Explain.

```text
Possibilities:
- The job runs but processes zero rows (upstream stopped delivering)
- Auto Loader notification queue broken; backfillInterval not set
- The gold task is present but the dashboard refresh task was removed
- The dashboard reads a different table than the pipeline writes
- A schedule was paused for the dashboard refresh only

Diagnosis: freshness query on each table in the chain, to find the
first stale link. Success is not the same as progress.
```

### You are paged at 03:00 for a failed job. Walk through your first ten minutes.

```text
1. Open the run; find the FIRST failed task (later ones are consequences)
2. Read the last Python frame of the traceback
3. Check the cluster event log — node loss, OOM, init script, capacity
4. Check the runbook linked in the alert
5. Assess: does this block a morning SLA? If not, it can wait for daylight
6. If it does: contain (has bad data been written? restore if so)
7. Apply the known fix or a repair run if it looks transient
8. If unresolved in 30 minutes, escalate — do not debug alone at 03:00
9. Note the timeline for the post-incident review
```

### How would you design monitoring for a new platform from scratch?

```text
Per pipeline:
✔ Failure alert to a monitored channel
✔ Freshness alert on each output table
✔ Quality checks with a gate before publishing
✔ Duration warning below the SLA, timeout above it
✔ Row counts logged to a control table

Platform-wide:
✔ Ops dashboard from system.lakeflow (failures, runtimes, jobs not running)
✔ Cost dashboard from system.billing by job and team tag
✔ Query performance from system.query.history
✔ Access and permission-change auditing from system.access.audit
✔ Cluster log delivery enabled everywhere by policy

Process:
✔ A runbook per critical pipeline, linked from each alert
✔ Post-incident reviews that always produce an alert, a test, and a guardrail
✔ Quarterly alert review — delete what nobody acts on
```

### Costs jumped 40% this month with no new pipelines. Investigate.

```text
1. system.billing.usage daily trend → when did it step up?
2. Group by SKU → all-purpose vs jobs vs SQL vs DLT
3. Group by job_id and team tag → which workload grew?
4. Check for: a DLT pipeline switched to continuous, development mode
   left on, autotermination disabled, a warehouse with min_clusters raised,
   an always-on stream started for a test and never stopped
5. Cross-check runtime trends — a job taking 3× longer costs 3× more
6. Add a daily DBU alert so the next step change is caught in a day
```

### A downstream team says your table broke their pipeline. How do you respond?

```text
1. Check DESCRIBE HISTORY — what changed and when?
2. Check whether a schema change was additive or breaking
3. Check lineage — who else consumes it and are they affected?
4. Short term: restore the previous version or re-add the removed column
5. Long term: schema contract tests in CI, a deprecation window before
   removing anything, and a consumer notification process
6. Post-incident action: add the contract test that would have blocked it
```

---

## Rapid-Fire Recap

```text
Four alerts: failure | freshness | quality | duration
Freshness catches what failure alerts cannot

System tables:
lakeflow (runs) | billing (cost) | query (SQL) | access (audit + lineage)

Spark UI: slowest stage → task min/median/max → skew, spill, shuffle
Plans: explain("formatted") → joins, exchanges, pushed filters
Logs: configure cluster_log_conf in advance or lose them
Cluster event log before debugging code

Delta diagnostics:
DESCRIBE DETAIL (files) | DESCRIBE HISTORY (what changed) | VERSION AS OF diff

Errors:
driver OOM = collect | executor OOM = skew first
ConcurrentAppend = replaceWhere | MERGE dup = deduplicate source
permission = USE CATALOG + USE SCHEMA + SELECT
stateful stream change = new checkpoint

Incident: detect → assess → CONTAIN → diagnose → fix → verify →
          communicate → prevent
Key metric: time to detect
Every review produces: an alert, a test, a guardrail
```
