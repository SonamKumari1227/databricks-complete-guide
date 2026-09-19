# Workflows: Interview Questions

Grouped by theme, phrased the way interviewers actually ask them.

```text
1. Fundamentals
2. Tasks and Compute
3. Dependencies and Control Flow
4. Scheduling and Triggers
5. Parameters and Task Values
6. Failure Handling
7. Monitoring and Cost
8. Scenario-Based
```

---

# 1. Fundamentals

### What are Databricks Workflows?

The built-in orchestration service of Databricks. A **job** defines a pipeline of
**tasks**, their dependencies, the compute they run on, when they trigger, and
what happens on failure. Each execution is a **run**.

### Difference between a job and a run?

A job is the definition; a run is one execution of it. A job can have thousands
of runs, each with its own run id, parameters, and status.

### Why not just schedule notebooks with cron?

Cron can start something, but it cannot express task dependencies, retry only the
failed step, pass values between steps, provision and terminate compute, or keep
run history.

### What is the difference between Workflows and Delta Live Tables?

| Workflows | DLT |
|-----------|-----|
| Orchestrates *any* task type | Declarative data pipeline framework |
| You define the DAG explicitly | DLT infers the DAG from table definitions |
| You handle data quality yourself | Built-in expectations |
| General purpose | Specialised for ETL into Delta tables |

They compose: a Workflow task can trigger a DLT pipeline.

---

# 2. Tasks and Compute

### What task types exist?

Notebook, Python script, Python wheel, SQL, DLT pipeline, Run Job, If/else
condition, For Each, JAR, Spark Submit, dbt.

### Job cluster vs all-purpose cluster for a job?

| Job cluster | All-purpose cluster |
|-------------|--------------------|
| Created per run, terminated after | Always on, shared |
| Cheapest DBU rate | Most expensive DBU rate |
| Isolated per run | Shared, someone can restart it |
| Correct for production | Development only |

### What is a shared job cluster?

One cluster defined at job level and reused by several tasks in the same run, so
you pay one startup instead of one per task.

### When would you use serverless compute for a job?

For short tasks, frequent runs, or when the 3 to 7 minute cluster startup
dominates the actual work. You trade a higher per-DBU rate for near-zero startup
and no cluster configuration.

### How should a job authenticate in production?

Run it as a **service principal**, not as a named user, so the job survives that
person leaving and gets permissions granted to an identity rather than a human.

---

# 3. Dependencies and Control Flow

### What is a DAG?

Directed Acyclic Graph: tasks connected by one-way `depends_on` arrows with no
cycles.

### If a task fails, what happens downstream?

Downstream tasks are **skipped** with status `UPSTREAM_FAILED`. Independent
branches continue to run. The job as a whole is marked failed.

### How do you make a task run even when an upstream task fails?

Set `run_if`. `ALL_DONE` runs it regardless of outcome;
`AT_LEAST_ONE_FAILED` runs it only on failure.

### List the `run_if` values.

`ALL_SUCCESS` (default), `AT_LEAST_ONE_SUCCESS`, `NONE_FAILED`, `ALL_DONE`,
`AT_LEAST_ONE_FAILED`, `ALL_FAILED`.

### How do you branch a pipeline conditionally?

Use an **If/else condition** task comparing two operands, usually a task value or
job parameter, and attach downstream tasks to the `true` and `false` outcomes.

### How do you run the same logic over 50 tables?

A **For Each** task with the table list as input and a nested task receiving
`{{input}}`, typically driven by a control table rather than a hard-coded list.

### How do you make one job depend on another?

Either a **Run Job** task inside a parent job (tight coupling, full visibility),
or a **table update trigger** on the downstream job (loose coupling, teams stay
independent).

---

# 4. Scheduling and Triggers

### Which cron syntax does Databricks use?

Quartz cron with 6 or 7 fields including seconds, not the 5-field Linux cron.

### Write a cron for every weekday at 7:30 AM.

`0 30 7 ? * MON-FRI`

### What does `?` mean in Quartz cron?

"No specific value", required in either day-of-month or day-of-week since
specifying both would conflict.

### Why schedule in UTC?

To avoid daylight saving transitions, which can make a schedule run twice or not
at all on switch days.

### What is a file arrival trigger?

A trigger that starts the job when new files appear in a Unity Catalog external
location or volume, with settings to debounce bursts and wait for uploads to
finish.

### What is a table update trigger?

A trigger that fires when one or more Delta tables receive new commits. It is the
cleanest way to chain pipelines owned by different teams.

### What is a continuous job?

A job that immediately starts a new run whenever the previous one ends. Suited to
always-on streaming, requires `max_concurrent_runs = 1`, and uses exponential
backoff on repeated failures.

### What happens if a scheduled run fires while a previous run is still going?

With `max_concurrent_runs = 1`, the new run is queued (if queueing is enabled) or
skipped. With a higher value, runs overlap.

---

# 5. Parameters and Task Values

### How do you pass parameters into a notebook task?

Define job or task parameters, and read them inside the notebook with
`dbutils.widgets.get("name")`.

### How do you pass a value from one task to another?

`dbutils.jobs.taskValues.set(key=..., value=...)` upstream, and
`dbutils.jobs.taskValues.get(taskKey=..., key=..., debugValue=...)` downstream,
or the reference `{{tasks.<task_key>.values.<key>}}`.

### Name useful dynamic value references.

`{{job.id}}`, `{{job.run_id}}`, `{{job.start_time.[iso_date]}}`,
`{{job.trigger.type}}`, `{{job.parameters.<name>}}`, `{{task.name}}`,
`{{tasks.<key>.values.<key>}}`.

### Why must every widget have a default?

So the notebook still runs interactively during development, when the job is not
supplying parameter values.

### What type do widget values return?

Always strings. Cast explicitly: `int(dbutils.widgets.get("limit"))`.

### `dbutils.notebook.run` vs a job task?

`notebook.run` calls a notebook from inside another notebook on the same cluster:
no separate DAG node, no per-task retry, no repair. A job task is a first-class
node with its own status, retries, and repair support. Prefer job tasks in
production.

### Can you pass a DataFrame through task values?

No. Task values are small JSON-serialisable metadata. Pass data through a Delta
table and share only the table name or a batch id.

---

# 6. Failure Handling

### How do retries work?

`max_retries` sets extra attempts per task, spaced by
`min_retry_interval_millis`. Failure notifications fire only after all retries
are exhausted.

### When should you not enable retries?

When the task is not idempotent (a retry would duplicate data), or when the error
is deterministic, such as a syntax error or a permission denial.

### What is idempotency and how do you achieve it in Spark?

Running twice gives the same result as running once. Achieved with `MERGE`, with
`replaceWhere` partition overwrites, with delete-then-insert for a batch, or with
Structured Streaming checkpoints.

### What is a repair run?

Re-running only the failed and skipped tasks of an existing run. It keeps the
same run id, the original parameters, and the results of tasks that already
succeeded, so you avoid re-running expensive upstream work.

### How do you recover a table that a bad run corrupted?

Delta time travel: find the good version with `DESCRIBE HISTORY`, then
`RESTORE TABLE <t> TO VERSION AS OF <n>`.

### Why set a task timeout?

To stop a hung task from consuming compute indefinitely, blocking the next
scheduled run, and delaying the alert. A common rule is 2 to 3 times normal
runtime.

### Should bad data fail the pipeline or be quarantined?

Depends on the contract. Fail fast when downstream consumers must never see
partial data. Quarantine when the good records are still useful and a steward can
review the rejects. DLT expectations formalise both options.

---

# 7. Monitoring and Cost

### Which notification events are available?

`on_start`, `on_success`, `on_failure`,
`on_duration_warning_threshold_exceeded`, `on_streaming_backlog_exceeded`.

### How do you alert a Slack channel or PagerDuty?

Create a notification destination at workspace level and reference it as a
webhook notification in the job.

### How do you catch a job that runs but is too slow?

A health rule on `RUN_DURATION_SECONDS` with a duration warning notification,
plus a hard `timeout_seconds` as the backstop.

### How do you report on all job failures across a workspace?

Query `system.lakeflow.job_run_timeline` (and `job_task_run_timeline` for task
detail) and build a Databricks SQL dashboard on top.

### How do you attribute compute cost to a pipeline?

`system.billing.usage` filtered on `usage_metadata.job_id`, combined with
consistent cluster tagging.

### Where do job cluster logs go after the cluster terminates?

They are lost unless **cluster log delivery** is configured to a volume, DBFS, or
cloud storage path.

### How do you reduce job cost?

```text
✔ Use job clusters or serverless instead of all-purpose
✔ Share one job cluster across tasks
✔ Right-size workers and enable autoscaling sensibly
✔ Use spot / preemptible workers for fault-tolerant work
✔ Enable Photon for heavy SQL and Delta workloads
✔ Avoid over-frequent schedules where a trigger would do
```

---

# 8. Scenario-Based

### A job takes 3 hours and must finish in 1. What do you do?

```text
1. Look at the DAG: are tasks chained that have no real dependency?
   Parallelising independent branches often gives the biggest win.
2. Check the Spark UI for skew, shuffle spill, and small files.
3. Right-size the cluster, enable Photon, enable AQE.
4. Convert full reloads to incremental loads with a watermark.
5. Use a shared job cluster to remove repeated startup time.
6. Split into critical-path and non-critical jobs so the SLA path is short.
```

### A job silently produced wrong numbers for three days. How do you prevent a repeat?

```text
1. Add a quality gate task: gold publishes only if checks pass.
2. Add freshness and row-count assertions, not just failure alerts.
3. Log every run to a control table and dashboard the trend.
4. Use Delta time travel to identify when the numbers diverged.
5. Add anomaly alerting: row count deviating more than N% from the 7-day mean.
```

### Two runs of the same job overlapped and duplicated data. Fix?

```text
1. Set max_concurrent_runs = 1 immediately.
2. Make the write idempotent: MERGE or replaceWhere by batch id.
3. Add a batch_id column so a bad batch can be deleted cleanly.
4. Review the schedule interval against actual runtime and add a timeout.
```

### Your pipeline depends on a vendor file that arrives between 2 AM and 9 AM. Design it.

```text
Use a file arrival trigger on the landing location, with
wait_after_last_change_seconds so partial uploads are not picked up,
and min_time_between_triggers_seconds to debounce multi-file drops.

Add a fallback scheduled run at 09:30 that alerts if no file arrived,
so a missing delivery is detected rather than silently ignored.
```

### You must ingest 200 tables with identical logic. Design it.

```text
Control table: table_name, source_path, load_type, watermark_col, is_active
Task 1: read the control table, emit the active list as a task value
Task 2: For Each over the list, concurrency 10 to 20
        nested task = generic ingest notebook parameterised by {{input}}
Task 3: consolidate results, write a run log, alert on any failed iteration

Adding a table becomes an INSERT, not a deployment.
```

### How would you promote a pipeline from dev to prod?

```text
Code in Git → Databricks Asset Bundles with dev / staging / prod targets
Per target: catalog variable, schedule pause_status, run_as service principal
CI: databricks bundle validate + unit tests on PR
CD: databricks bundle deploy --target prod on merge to main
Prod jobs never edited in the UI; the bundle is the source of truth.
```

### A downstream team keeps reading half-written gold tables. Fix?

```text
1. Write to a staging table, then swap or MERGE in one final atomic step.
2. Publish only after the quality gate passes.
3. Let consumers trigger off the table update trigger rather than the clock.
4. Delta gives snapshot isolation, so readers see a consistent version —
   ensure the write is one transaction, not many appends.
```

---

## Rapid-Fire Recap

```text
Job = definition, Run = execution
Task types: notebook, python, wheel, SQL, DLT, run job, condition, for each, JAR, dbt
Cron = Quartz, 6-7 fields, daily 6 AM = 0 0 6 * * ?
run_if = ALL_SUCCESS | ALL_DONE | NONE_FAILED | AT_LEAST_ONE_SUCCESS | AT_LEAST_ONE_FAILED | ALL_FAILED
Task values = dbutils.jobs.taskValues.set / get
Repair run = re-run only failed + skipped tasks
Idempotency = MERGE | replaceWhere | checkpoints
Monitoring = system.lakeflow.job_run_timeline + system.billing.usage
Production = job cluster + service principal + bundles + alerts
```
