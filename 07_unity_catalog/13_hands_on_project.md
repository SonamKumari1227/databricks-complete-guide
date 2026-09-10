# Unity Catalog: Hands-On Project

## Overview

This project ties together everything learned across all previous topics:

```text
Fundamentals & Architecture
Permissions & Access Control
Storage Credentials / External Locations / Volumes
Lineage, Auditing, Discovery
Delta Sharing
Row/Column-Level Security
Service Principals & OAuth
Workspace-Catalog Binding
Metastore/Workspace Architecture
Hive Metastore Migration
System Tables & Monitoring
Enterprise Best Practices
```

You will design and implement a realistic **end-to-end governed Lakehouse** for a fictional retail company.

---

# Project Scenario

**Company:** RetailNova Inc.
**Goal:** Build a governed sales analytics platform using Unity Catalog.

Requirements:

```text
1. Separate environments for Dev and Production.
2. Sales data flows through Bronze -> Silver -> Gold layers.
3. Regional managers should only see their own region's data.
4. Customer PII (email, phone) must be masked for most users.
5. An automated nightly pipeline must load data without using personal credentials.
6. External partners need read-only access to a curated Gold table.
7. Leadership wants a monitoring dashboard for cost and pipeline health.
```

---

# Step 1: Design the Catalog Structure

```text
                     Metastore (retail_metastore)
                                |
                  ------------------------------
                  |                            |
                  v                            v
             dev_catalog                  prod_catalog
                  |                            |
        --------------------          --------------------
        |         |         |         |         |         |
        v         v         v         v         v         v
     bronze     silver     gold     bronze     silver     gold
```

```sql
CREATE CATALOG dev_catalog;
CREATE CATALOG prod_catalog;

CREATE SCHEMA prod_catalog.bronze;
CREATE SCHEMA prod_catalog.silver;
CREATE SCHEMA prod_catalog.gold;

CREATE SCHEMA dev_catalog.bronze;
CREATE SCHEMA dev_catalog.silver;
CREATE SCHEMA dev_catalog.gold;
```

---

# Step 2: Set Up Storage Credentials & External Locations

```sql
CREATE STORAGE CREDENTIAL retail_storage_cred
WITH (AZURE_MANAGED_IDENTITY | AWS_IAM_ROLE ...);

CREATE EXTERNAL LOCATION retail_bronze_loc
URL 's3://retailnova-data/bronze/'
WITH (STORAGE CREDENTIAL retail_storage_cred);
```

```text
Storage Credential
       |
       v
External Location
       |
       v
Tables reference this location for managed/external data
```

---

# Step 3: Apply Workspace-Catalog Binding

```text
dev_catalog  -> bound to ws_dev only
prod_catalog -> bound to ws_prod only
```

This prevents developers from touching production data, even if they are accidentally granted access.

---

# Step 4: Build the Bronze -> Silver -> Gold Pipeline

```sql
-- Bronze: raw ingestion
CREATE TABLE prod_catalog.bronze.orders
AS SELECT * FROM raw_ingestion_source;

-- Silver: cleaned data
CREATE TABLE prod_catalog.silver.orders AS
SELECT
    order_id,
    customer_id,
    region,
    order_date,
    amount,
    email,
    phone
FROM prod_catalog.bronze.orders
WHERE amount IS NOT NULL;

-- Gold: aggregated reporting table
CREATE TABLE prod_catalog.gold.monthly_sales AS
SELECT
    region,
    DATE_TRUNC('month', order_date) AS month,
    SUM(amount) AS total_sales
FROM prod_catalog.silver.orders
GROUP BY region, DATE_TRUNC('month', order_date);
```

```text
bronze.orders
      |
      v
silver.orders
      |
      v
gold.monthly_sales
```

---

# Step 5: Apply Row-Level Security (Regional Isolation)

```sql
CREATE FUNCTION prod_catalog.gold.region_filter(region STRING)
RETURN
  is_account_group_member(CONCAT(region, '_managers'))
  OR is_account_group_member('exec_team');

ALTER TABLE prod_catalog.gold.monthly_sales
SET ROW FILTER prod_catalog.gold.region_filter ON (region);
```

Result:

```text
india_managers -> see only India rows
us_managers    -> see only US rows
exec_team      -> see all rows
```

---

# Step 6: Apply Column Masking (Protect Customer PII)

```sql
CREATE FUNCTION prod_catalog.silver.mask_email(email STRING)
RETURN
  CASE
    WHEN is_account_group_member('data_engineering') THEN email
    ELSE CONCAT('***@', SPLIT(email, '@')[1])
  END;

ALTER TABLE prod_catalog.silver.orders
ALTER COLUMN email
SET MASK prod_catalog.silver.mask_email;
```

```text
data_engineering -> sees full email
everyone else    -> sees masked email
```

---

# Step 7: Set Up a Service Principal for the Nightly Pipeline

```sql
GRANT SELECT ON TABLE prod_catalog.bronze.orders TO `svc-etl-nightly`;
GRANT MODIFY ON TABLE prod_catalog.silver.orders TO `svc-etl-nightly`;
GRANT MODIFY ON TABLE prod_catalog.gold.monthly_sales TO `svc-etl-nightly`;
```

```text
Orchestration Tool (e.g. Airflow)
            |
            v
   Service Principal (svc-etl-nightly)
            |
            v
     OAuth Token Authentication
            |
            v
   Unity Catalog authorizes based on GRANTs
            |
            v
      Pipeline runs bronze -> silver -> gold
```

No personal credentials are used anywhere in the pipeline.

---

# Step 8: Share the Gold Table With an External Partner (Delta Sharing)

```sql
CREATE SHARE retail_partner_share;

ALTER SHARE retail_partner_share
ADD TABLE prod_catalog.gold.monthly_sales;

CREATE RECIPIENT partner_company
USING ID 'partner_company_sharing_identifier';

GRANT SELECT ON SHARE retail_partner_share TO RECIPIENT partner_company;
```

```text
prod_catalog.gold.monthly_sales
            |
            v
        Delta Share
            |
            v
   External Partner (read-only, no Databricks account required)
```

---

# Step 9: Add Metadata, Tags, and Ownership

```sql
COMMENT ON TABLE prod_catalog.gold.monthly_sales
IS 'Monthly aggregated sales figures by region, used for reporting';

ALTER TABLE prod_catalog.gold.monthly_sales
SET TAGS ('domain' = 'sales', 'classification' = 'internal', 'owner' = 'data-engineering');

COMMENT ON COLUMN prod_catalog.silver.orders.email
IS 'Customer email address, masked for non-data-engineering roles';
```

This makes the datasets discoverable and trustworthy for analysts across the company.

---

# Step 10: Build a Monitoring Dashboard Using System Tables

```sql
-- Pipeline health
SELECT job_id, run_id, result_state, period_start_time
FROM system.lakeflow.job_run_timeline
WHERE result_state = 'FAILED'
ORDER BY period_start_time DESC;

-- Cost by workspace
SELECT workspace_id, SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
GROUP BY workspace_id;

-- Access to sensitive gold tables
SELECT event_time, user_identity.email, request_params.full_name_arg
FROM system.access.audit
WHERE request_params.full_name_arg LIKE 'prod_catalog.gold.%'
  AND action_name = 'getTable';
```

These queries feed a Databricks SQL Dashboard for leadership visibility into pipeline health, cost, and sensitive data access.

---

# Full End-to-End Architecture Diagram

```text
                      Metastore (retail_metastore)
                                  |
                  ---------------------------------
                  |                                |
                  v                                v
            dev_catalog                       prod_catalog
          (bound to ws_dev)                (bound to ws_prod)
                                                    |
                                     -------------------------------
                                     |             |               |
                                     v             v               v
                                  bronze        silver           gold
                                     |             |               |
                                     v             v               v
                               raw orders   cleaned orders   monthly_sales
                                                   |               |
                                        column mask (email)   row filter (region)
                                                                    |
                                                                    v
                                                          Delta Share -> Partner
                                                                    |
                                                                    v
                                                       System Tables -> Dashboard
                                                       (cost, jobs, audit)
```

---

# Project Checklist

```text
[x] Catalog structure designed (env + medallion layers)
[x] Storage credentials and external locations configured
[x] Workspace-Catalog Binding applied for environment isolation
[x] Bronze -> Silver -> Gold pipeline built
[x] Row-level security applied for regional isolation
[x] Column masking applied for customer PII
[x] Service Principal + OAuth used for automated pipeline
[x] Delta Sharing configured for external partner access
[x] Metadata, tags, ownership, and descriptions added
[x] Monitoring dashboard built using System Tables
```

---

# Reflection Questions (Self-Check)

```text
1. Why was Workspace-Catalog Binding used instead of relying only on GRANTs?
2. Why is column masking preferred over fully hiding the email column?
3. Why does the nightly pipeline use a Service Principal instead of a real user account?
4. How would you trace a data quality issue in gold.monthly_sales back to its source?
5. How would System Tables help you detect unauthorized access to gold.monthly_sales?
```

---

# Key Takeaways

- This project combines catalog structure, storage setup, pipeline design, security, sharing, and monitoring into one governed Lakehouse.
- Workspace-Catalog Binding enforces environment isolation on top of GRANT-based permissions.
- Row-level security and column masking together protect regional data and PII without blocking usability.
- Service Principals and OAuth remove the need for personal credentials in automated pipelines.
- Delta Sharing enables secure external collaboration without duplicating data.
- Metadata, tags, and ownership make datasets discoverable and trustworthy.
- System Tables provide the SQL-queryable foundation for cost, pipeline health, and security monitoring dashboards.
- Together, these pieces demonstrate the complete Unity Catalog governance model in a realistic, end-to-end scenario.
