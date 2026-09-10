# Unity Catalog: System Tables and Monitoring

## Overview

Unity Catalog exposes a lot of operational data as **queryable tables**, called **System Tables**.

Instead of digging through logs or external dashboards, admins and engineers can simply run **SQL queries** to monitor:

```text
Billing and usage
Audit logs
Job runs
Cluster activity
Lineage
Query history
Storage usage
```

---

# What Are System Tables?

System Tables are Delta tables, **managed automatically by Databricks**, that live inside a special system catalog.

```text
system
  |
  ------------------------------
  |            |               |
  v            v               v
access      billing          compute
  |            |               |
  v            v               v
audit logs   usage records  cluster/warehouse info
```

They can be queried like any normal table:

```sql
SELECT * FROM system.access.audit
LIMIT 10;
```

---

# Why System Tables Are Important

```text
No need for external log aggregation tools
Centralized, SQL-queryable operational data
Enables custom dashboards and alerts
Historical data retained for auditing and trend analysis
Works across the entire account, not just one workspace
```

Mental Model:

```text
System Tables = Databricks' own operational data, exposed as SQL tables
```

---

# Common System Table Schemas

## 1. `system.access`

Tracks access-related activity.

```text
system.access.audit          -> full audit log of actions
system.access.table_lineage  -> table-level lineage records
system.access.column_lineage -> column-level lineage records
```

---

## 2. `system.billing`

Tracks usage and cost.

```text
system.billing.usage          -> compute/DBU usage records
system.billing.list_prices    -> pricing information
```

---

## 3. `system.compute`

Tracks cluster and warehouse activity.

```text
system.compute.clusters       -> cluster configuration/history
system.compute.warehouses     -> SQL warehouse configuration/history
system.compute.node_types     -> available node type info
```

---

## 4. `system.lakeflow` (Jobs)

Tracks job and pipeline execution.

```text
system.lakeflow.jobs           -> job definitions
system.lakeflow.job_run_timeline -> job run history and status
```

---

## 5. `system.query`

Tracks query-level activity on SQL warehouses.

```text
system.query.history -> detailed query execution history
```

---

# System Tables Architecture Diagram

```text
                     Databricks Platform Activity
                                |
                                v
                         System Tables
                                |
        --------------------------------------------
        |             |             |               |
        v             v             v               v
     access        billing       compute          query
        |             |             |               |
        v             v             v               v
   audit/lineage   usage/cost   clusters/warehouses  query history
```

---

# Example: Auditing Table Access

```sql
SELECT
    event_time,
    user_identity.email AS user_email,
    action_name,
    request_params.full_name_arg AS table_name
FROM system.access.audit
WHERE action_name = 'getTable'
  AND request_params.full_name_arg = 'sales.gold.customer_summary'
ORDER BY event_time DESC;
```

This shows exactly who accessed a specific table and when.

---

# Example: Tracking Cost by Workspace

```sql
SELECT
    workspace_id,
    SUM(usage_quantity) AS total_dbus,
    sku_name
FROM system.billing.usage
GROUP BY workspace_id, sku_name
ORDER BY total_dbus DESC;
```

This helps identify which workspace or workload is consuming the most compute.

---

# Example: Monitoring Failed Job Runs

```sql
SELECT
    job_id,
    run_id,
    period_start_time,
    period_end_time,
    result_state
FROM system.lakeflow.job_run_timeline
WHERE result_state = 'FAILED'
ORDER BY period_start_time DESC;
```

This helps operations teams quickly spot failing pipelines.

---

# Example: Table-Level Lineage via System Tables

```sql
SELECT
    source_table_full_name,
    target_table_full_name,
    event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'sales.gold.monthly_sales'
ORDER BY event_time DESC;
```

This programmatically answers: *"What feeds this table?"* — the same question the Lineage UI answers visually.

---

# Building Monitoring Dashboards

Since system tables are just Delta tables, they can power:

```text
Databricks SQL Dashboards
Alerts (e.g. notify if job failure rate > threshold)
Cost tracking dashboards by team/workspace
Security dashboards (sensitive table access monitoring)
```

```text
System Tables
      |
      v
SQL Queries / Views
      |
      v
Dashboards + Alerts
      |
      v
Proactive Monitoring & Governance
```

---

# Real-World Example

A platform team wants a single dashboard to monitor:

```text
1. Daily DBU cost per workspace
2. Number of failed job runs per day
3. Access count on sensitive finance tables
```

Solution using system tables:

```sql
-- 1. Daily cost per workspace
SELECT workspace_id, DATE(usage_start_time) AS usage_date,
       SUM(usage_quantity) AS dbus
FROM system.billing.usage
GROUP BY workspace_id, DATE(usage_start_time);

-- 2. Failed jobs per day
SELECT DATE(period_start_time) AS run_date,
       COUNT(*) AS failed_runs
FROM system.lakeflow.job_run_timeline
WHERE result_state = 'FAILED'
GROUP BY DATE(period_start_time);

-- 3. Access on sensitive finance tables
SELECT event_time, user_identity.email, request_params.full_name_arg
FROM system.access.audit
WHERE request_params.full_name_arg LIKE 'finance.%'
  AND action_name = 'getTable';
```

All three queries feed into a single Databricks SQL Dashboard, refreshed on a schedule.

---

# System Tables vs Manual Audit Log Export

| Aspect | Manual Log Export | System Tables |
|--------|-------------------|----------------|
| Setup | Requires external pipeline/storage config | Enabled at account level, built-in |
| Query Method | External tools/log parsing | Standard SQL |
| Historical Data | Depends on retention config | Retained automatically |
| Cross-Workspace View | Difficult | Native, account-wide |
| Dashboarding | Requires extra tooling | Native Databricks SQL Dashboards |

---

# Important Interview Questions

## What are System Tables in Databricks Unity Catalog?

System Tables are Delta tables automatically managed by Databricks that expose operational data — such as audit logs, billing usage, job runs, and lineage — as queryable SQL tables inside a special `system` catalog.

---

## Name a few important system table schemas.

`system.access` (audit logs, lineage), `system.billing` (usage and cost), `system.compute` (clusters and warehouses), `system.lakeflow` (jobs), and `system.query` (query history).

---

## How can system tables be used for auditing?

By querying `system.access.audit`, admins can see who performed an action, what action was performed, on which object, and when — all using standard SQL.

---

## How can system tables help with cost monitoring?

`system.billing.usage` provides usage and DBU consumption records that can be grouped by workspace, SKU, or time period to track and optimize cost.

---

## How do system tables relate to lineage shown in the Unity Catalog UI?

The lineage shown in the UI is backed by the same data available in `system.access.table_lineage` and `system.access.column_lineage`, which can be queried directly for custom reporting.

---

# Best Practices

## 1. Build Dashboards on Top of System Tables

Use Databricks SQL Dashboards to visualize cost, job failures, and access patterns.

---

## 2. Set Up Alerts for Anomalies

Create alerts for spikes in cost, repeated job failures, or unusual access to sensitive tables.

---

## 3. Restrict Access to System Tables

Since system tables contain sensitive operational data, apply proper permissions to control who can query them.

---

## 4. Use System Tables for Chargeback/Showback

Leverage `system.billing.usage` to allocate costs accurately across teams or business units.

---

## 5. Combine With Row/Column Security

Apply governance controls on system tables just like any other sensitive dataset, if needed.

---

## 6. Query Regularly, Not Just During Incidents

Build monitoring into routine operations rather than only checking system tables reactively during an incident.

---

# Complete Mental Model

```text
                DATABRICKS PLATFORM ACTIVITY
                            |
                            v
                     SYSTEM TABLES (SQL)
                            |
       -----------------------------------------
       |            |             |            |
       v            v             v            v
    access       billing       compute       query
       |            |             |            |
       v            v             v            v
  audit/lineage   cost/usage   clusters/wh   query history
                            |
                            v
              Dashboards, Alerts, Governance Reporting
```

---

# Key Takeaways

- System Tables expose Databricks operational data as queryable Delta tables under the `system` catalog.
- Key schemas include access (audit/lineage), billing (usage/cost), compute (clusters/warehouses), lakeflow (jobs), and query (query history).
- They allow monitoring, auditing, and cost tracking using plain SQL — no external tools required.
- System tables provide account-wide, historical, and centralized visibility.
- They can power Databricks SQL Dashboards and alerts for proactive governance.
- Access to system tables should itself be governed, since they contain sensitive operational data.
- They are the SQL-queryable foundation behind features like the Lineage UI and Audit Logs.
