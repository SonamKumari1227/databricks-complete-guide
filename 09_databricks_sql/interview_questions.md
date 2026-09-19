# Databricks SQL: Interview Questions

```text
1. Fundamentals
2. SQL Warehouses
3. Queries and Objects
4. Dashboards and BI
5. Alerts and Automation
6. Performance and Cost
7. Scenario-Based
```

---

# 1. Fundamentals

### What is Databricks SQL?

The analytics interface of the Lakehouse: SQL warehouses as compute, a SQL
editor, saved queries, dashboards, alerts, and JDBC/ODBC endpoints for BI tools,
all governed by Unity Catalog.

### How does the Lakehouse remove the need for a separate data warehouse?

BI tools query Delta tables directly through SQL warehouses, so there is one copy
of the data, one governance model, and no ETL lag between lake and warehouse.

### Who uses Databricks SQL versus notebooks?

Analysts and BI consumers use Databricks SQL; engineers building pipelines use
notebooks and jobs. Both read the same Delta tables under the same Unity Catalog
permissions.

---

# 2. SQL Warehouses

### What is a SQL warehouse?

SQL-optimised compute sized by T-shirt size, with Photon enabled, which you do
not configure as a Spark cluster.

### Serverless vs Pro vs Classic?

Serverless runs in the Databricks account and starts in seconds. Pro and Classic
run in your cloud account and take minutes to start; Pro includes features such
as Predictive I/O that Classic lacks. Serverless is the default for interactive
analytics.

### Difference between warehouse size and scaling?

```text
Size    → power applied to a single query   → fixes SLOW queries
Scaling → number of clusters for concurrency → fixes QUEUED queries
```

### What is auto stop?

The idle timeout after which the warehouse shuts down and billing stops. It is
the single largest cost control in Databricks SQL.

### How is a warehouse billed?

By DBUs consumed per hour while running, not per query or per byte scanned.

### How do BI tools connect?

Via the warehouse JDBC/ODBC endpoint (server hostname plus HTTP path),
authenticating with OAuth or a token, with all access enforced by Unity Catalog.

### A user has CAN USE on the warehouse but gets permission denied. Why?

Warehouse permission and data permission are separate. They also need
`USE CATALOG`, `USE SCHEMA`, and `SELECT` on the object in Unity Catalog.

---

# 3. Queries and Objects

### What is the three-level namespace?

`catalog.schema.table` in Unity Catalog, replacing the two-level
`database.table` of the Hive metastore.

### Saved query vs view vs materialized view?

| | Stored in | Computed | Visible to BI |
|---|---|---|---|
| Saved query | Databricks SQL | Every run | No |
| View | Unity Catalog | Every run | Yes |
| Materialized view | Unity Catalog | Precomputed, scheduled refresh | Yes |

### How do you parameterise a query?

Named parameters such as `:start_date`, exposed as widgets (text, number, date
range, dropdown, query-based dropdown), which dashboards surface as filters.

### Why fully qualify table names in saved queries?

Because `USE CATALOG` / `USE SCHEMA` context belongs to the editor session. An
unqualified saved query breaks for anyone with different context.

### How do you query a table as of last week?

```sql
SELECT * FROM t TIMESTAMP AS OF '2026-09-11';
SELECT * FROM t VERSION AS OF 42;
```

### How do you find the schema and size of a table?

`DESCRIBE TABLE`, `DESCRIBE DETAIL` (size, file count, location), and
`DESCRIBE HISTORY` (Delta versions).

---

# 4. Dashboards and BI

### AI/BI dashboards vs legacy dashboards?

AI/BI dashboards use shared datasets, a free-form canvas, cross-filtering, and
Genie integration. Legacy dashboards attach one saved query per widget. New work
should use AI/BI dashboards.

### Why use one dataset for many charts?

One query execution instead of many, and all tiles are guaranteed to show
mutually consistent numbers.

### Embedded credentials vs viewer credentials when publishing?

Embedded runs all queries as the publisher, so every viewer sees identical data.
Viewer credentials run as each viewer, respecting their grants and row filters.
Publishing with embedded credentials over row-filtered data bypasses that
security.

### How do you keep a dashboard consistent with the pipeline?

Refresh it as the final task of the job that builds the gold tables, rather than
on an independent schedule.

### What is Genie?

A natural language interface over a curated set of tables that generates SQL from
plain-language questions. Its accuracy depends on clean gold tables, table and
column comments, instructions, and example query pairs.

### How do you make a dashboard fast?

Read small pre-aggregated gold tables or materialized views, push filters into
SQL parameters so Delta prunes files, share datasets across tiles, and use a
serverless warehouse to avoid cold starts.

---

# 5. Alerts and Automation

### What is a SQL alert?

A scheduled query plus a condition on a result column, which notifies configured
destinations when the condition is met.

### How do you detect a pipeline that silently stopped running?

A freshness alert on `max(ingested_at)` or `max(order_date)`. A job failure
notification cannot fire for a job that never ran.

### Name the alerts a mature platform has.

```text
1. Job failure          → Workflow on_failure
2. Job never ran        → SQL freshness alert
3. Data quality broken  → SQL quality alert
4. Business anomaly     → SQL deviation alert
5. Job too slow         → Workflow duration health rule
6. Cost spike           → SQL alert on system.billing.usage
```

### How do you avoid alert fatigue?

Retrigger intervals, severity-based routing, an owner and runbook per alert, and
deleting alerts nobody has acted on.

### What makes a good alert message?

It states what is wrong, how bad it is, and what to do — with a runbook link and
the triggering value embedded.

---

# 6. Performance and Cost

### What is data skipping?

Delta keeps min/max statistics per file in the transaction log, so queries can
eliminate files without reading them.

### Liquid clustering vs Z-ordering vs partitioning?

Partitioning creates fixed directories and needs a rewrite to change. Z-ordering
co-locates data within files via `OPTIMIZE`. Liquid clustering replaces both,
lets you change keys with `ALTER TABLE`, and handles skew and small files better.
Use liquid clustering for new tables.

### Why is `WHERE year(order_date) = 2026` slow?

The function on the column prevents file pruning. Use a range predicate on the
bare column.

### What is the small file problem and how do you fix it?

Many tiny files make metadata and listing dominate the read. Fix with `OPTIMIZE`,
optimized writes, auto compaction, or predictive optimization.

### What does AQE do?

Re-optimises at runtime: coalesces small shuffle partitions, converts joins to
broadcast joins, and splits skewed partitions.

### What is Photon?

A vectorised C++ engine that accelerates scans, joins, aggregations, and Delta
writes. It has a higher DBU rate usually offset by shorter runtime, and it does
not help Python UDFs.

### Which caches exist?

Result cache (identical query, unchanged data), disk cache (Parquet on cluster
SSD), and materialized views (explicitly precomputed).

### How do you investigate a slow query?

Open the query profile: bytes scanned versus returned, time per operator, shuffle
size, disk spill, Photon coverage, and queued time.

---

# 7. Scenario-Based

### An executive dashboard takes 3 minutes to load. Fix it.

```text
1. Query profile: is it scanning silver tables at query time?
   → pre-aggregate into a gold table or materialized view.
2. Is the warehouse cold starting? → switch to serverless.
3. Are there 20 tiles each running its own query? → consolidate datasets.
4. Are filters applied in the browser rather than in SQL?
   → push date and country filters into parameters.
5. Small files on the gold table? → OPTIMIZE and enable auto compaction.
```

### Monthly SQL spend doubled. Find out why.

```text
1. system.billing.usage grouped by warehouse_id → which warehouse grew?
2. Check auto_stop_mins — was it disabled or raised?
3. Check size and max_num_clusters — did someone scale it up?
4. system.query.history → a new heavy recurring query or a runaway BI extract?
5. Check min_num_clusters > 1 keeping clusters warm 24/7.
6. Add a daily DBU alert so the next spike is caught in a day, not a month.
```

### Analysts complain queries are "queueing". What do you change?

Increase `max_num_clusters` (scaling), not the warehouse size. Size fixes slow
single queries; scaling fixes concurrency. Consider a separate warehouse for
heavy dbt or job workloads so they do not compete with interactive users.

### Two dashboards show different revenue for the same day. Why?

```text
Likely causes:
- Each tile has its own near-duplicate query with a slightly different filter
- One reads silver, the other reads gold
- One includes cancelled orders, the other does not
- Different refresh times, so one is stale

Fix: define the metric once — a shared dataset, a view, or a metric view —
and have every tile and dashboard read that single definition.
```

### The business says a number is wrong. How do you investigate?

```text
1. DESCRIBE HISTORY on the gold table → when did it last change?
2. Compare versions: VERSION AS OF n EXCEPT VERSION AS OF n-1
3. Check the quarantine table for rejected rows that should have been included
4. Check system.lakeflow.job_run_timeline → did the pipeline run and succeed?
5. Trace lineage in Unity Catalog to the upstream source
6. If the pipeline wrote bad data: RESTORE TABLE ... TO VERSION AS OF n
```

### How would you give a business team self-service analytics safely?

```text
✔ Curated gold tables only, with comments on every table and column
✔ Views and metric definitions so "revenue" means one thing
✔ Unity Catalog grants on the gold schema, with row filters and column masks
✔ A serverless warehouse with auto stop and a size cap, CAN USE not CAN MANAGE
✔ A Genie space scoped to those gold tables with example questions
✔ Cost alerts and query history monitoring
```

---

## Rapid-Fire Recap

```text
Warehouse types: Serverless | Pro | Classic
Size = speed per query, Scaling = concurrency
Auto stop = biggest cost lever

Objects: saved query | view | materialized view
Namespace: catalog.schema.table, always fully qualified

Dashboards: shared datasets, filters pushed into SQL, refreshed by the pipeline
Publishing: embedded credentials vs viewer credentials (security implication)

Alerts: freshness catches what failure alerts cannot

Performance: read fewer bytes
liquid clustering → OPTIMIZE → caches → Photon → AQE
Diagnose with the query profile, always

Monitoring: system.query.history + system.billing.usage
```
