# Unity Catalog: Metastore and Workspace Architecture

## Overview

To truly understand Unity Catalog, it's important to understand how it fits with:

```text
Databricks Account
Metastore
Workspaces
Catalogs
```

This topic explains the **overall architecture** — how everything connects together.

---

# 1. Databricks Account

The Databricks Account is the top-level entity.

```text
Databricks Account
        |
        v
  One or more Workspaces
        |
        v
  One Unity Catalog Metastore (per region, typically)
```

An account can have many workspaces across different regions and cloud providers.

---

# 2. Metastore

## What Is a Metastore?

A metastore is the top-level container for Unity Catalog metadata.

```text
Metastore stores information about:

Catalogs
Schemas
Tables
Views
Volumes
Functions
Models
Permissions
```

```text
1 Metastore  ->  Typically 1 per region
1 Metastore  ->  Can be attached to MULTIPLE workspaces
```

---

## Metastore Architecture Diagram

```text
                     Metastore (Region: US-East)
                              |
              --------------------------------
              |               |               |
              v               v               v
        Workspace A     Workspace B     Workspace C
        (Marketing)     (Finance)       (Data Eng)
```

All three workspaces share the same metastore, meaning they can share the same catalogs, permissions, and governance rules (subject to workspace-catalog binding).

---

# 3. Workspaces

## What Is a Workspace?

A workspace is the environment where users:

```text
Write notebooks
Run jobs
Create clusters
Query data
Build dashboards
```

```text
Workspace = Where the WORK happens

Metastore = Where the GOVERNANCE/METADATA lives
```

---

## One Metastore, Many Workspaces

```text
                       Metastore
                           |
        -----------------------------------------
        |                |                |
        v                v                v
   ws_dev            ws_staging         ws_prod
```

This allows:

```text
Centralized governance
Shared catalogs (if not restricted by binding)
Consistent permissions across environments
```

---

# 4. Catalogs, Schemas, and Tables (Recap)

Inside a metastore, data is organized using a **three-level namespace**:

```text
catalog.schema.table
```

Example:

```text
sales.gold.monthly_sales
```

```text
Metastore
   |
   v
Catalog (sales)
   |
   v
Schema (gold)
   |
   v
Table (monthly_sales)
```

---

# Full Architecture Diagram

```text
                     Databricks Account
                              |
                              v
                          Metastore
                              |
              --------------------------------
              |               |               |
              v               v               v
        Workspace A     Workspace B     Workspace C
              |               |               |
              v               v               v
          Catalogs        Catalogs         Catalogs
              |               |               |
              v               v               v
          Schemas          Schemas          Schemas
              |               |               |
              v               v               v
           Tables            Tables           Tables
```

---

# Metastore Assignment Rules

```text
Each workspace can be attached to only ONE metastore at a time.

A metastore can be attached to MULTIPLE workspaces.

Metastores are typically created per region 
(to keep data close to compute and comply with data residency rules).
```

---

## Example: Multi-Region Setup

```text
Metastore (US Region)
        |
        -------------------------
        |                       |
        v                       v
   ws_us_dev               ws_us_prod

Metastore (EU Region)
        |
        -------------------------
        |                       |
        v                       v
   ws_eu_dev               ws_eu_prod
```

This setup keeps US and EU data governance completely separate, which is often required for compliance (e.g., GDPR).

---

# Storage Layer Connection

The metastore itself doesn't store the actual data — it stores **metadata** and references to storage locations.

```text
Metastore
    |
    v
Storage Credential
    |
    v
External Location (cloud storage path)
    |
    v
Actual Data Files (Delta Lake / Parquet etc.)
```

```text
Metastore   = "Map" of what data exists and who can access it

Storage     = Where the actual bytes live (S3 / ADLS / GCS)
```

---

# Identity Federation Across Workspaces

Because multiple workspaces share one metastore:

```text
A user's identity and permissions are consistent
across every workspace attached to that metastore.
```

Example:

```text
sonam@company.com granted SELECT on sales.gold.monthly_sales

-> This permission applies in ws_dev, ws_staging, and ws_prod
   (unless restricted via workspace-catalog binding)
```

This is a major improvement over the old Hive Metastore model, where each workspace had its own separate, disconnected metastore.

---

# Real-World Example

A company operates in two regions and has three environments:

```text
Regions: US, EU
Environments: Dev, Prod
```

Architecture:

```text
Metastore_US
     |
     -----------------------
     |                     |
     v                     v
  ws_us_dev             ws_us_prod

Metastore_EU
     |
     -----------------------
     |                     |
     v                     v
  ws_eu_dev             ws_eu_prod
```

Catalogs:

```text
us_sales_catalog    -> lives in Metastore_US
eu_sales_catalog    -> lives in Metastore_EU
```

Governance benefit:

```text
EU data never crosses into the US metastore,
satisfying data residency requirements automatically,
simply due to architecture — not manual policy enforcement.
```

---

# Metastore vs Workspace vs Catalog

| Concept | Role |
|---------|------|
| Databricks Account | Top-level organizational entity |
| Metastore | Stores metadata, permissions, governance rules (usually per region) |
| Workspace | Environment where users run notebooks, jobs, queries |
| Catalog | Top-level data container inside a metastore |
| Schema | Grouping of tables/views within a catalog |
| Table/View | Actual queryable data object |

---

# Important Interview Questions

## What is a Unity Catalog metastore?

A metastore is the top-level container in Unity Catalog that stores metadata about catalogs, schemas, tables, permissions, and other governed objects.

---

## Can one metastore be attached to multiple workspaces?

Yes. A single metastore can be attached to multiple workspaces, allowing them to share catalogs, permissions, and governance rules.

---

## Can a workspace be attached to multiple metastores?

No. A workspace can be attached to only one metastore at a time.

---

## Why are metastores typically created per region?

To keep metadata and governance close to where the compute and storage reside, and to help meet data residency and compliance requirements.

---

## How does Unity Catalog improve on the old Hive Metastore model?

Unlike Hive Metastore, which was isolated per workspace, Unity Catalog's metastore is centralized and can be shared across multiple workspaces, providing consistent identity, permissions, and governance everywhere.

---

## Does the metastore store the actual data?

No. The metastore stores metadata and references; the actual data files are stored in cloud storage (e.g., S3, ADLS, GCS) accessed via storage credentials and external locations.

---

# Best Practices

## 1. Align Metastores With Regions

Create one metastore per region to satisfy data residency and reduce latency.

---

## 2. Use Workspace-Catalog Binding for Isolation

Even though workspaces share a metastore, use binding to isolate sensitive catalogs to specific workspaces.

---

## 3. Plan Metastore Structure Early

Metastore-to-workspace mapping is a foundational decision — plan it carefully before onboarding many teams.

---

## 4. Centralize Identity Management

Use account-level identity (SSO, SCIM) so permissions remain consistent across all workspaces attached to a metastore.

---

## 5. Document the Architecture

Maintain a clear diagram of which workspaces attach to which metastore, especially in multi-region setups.

---

# Complete Mental Model

```text
                  DATABRICKS ACCOUNT
                          |
                          v
                     METASTORE
                (per region, shared)
                          |
        ---------------------------------
        |                |               |
        v                v               v
   Workspace 1      Workspace 2     Workspace 3
        |                |               |
        v                v               v
     Catalogs         Catalogs        Catalogs
```

---

# Key Takeaways

- The metastore is the top-level container for Unity Catalog metadata and governance.
- One metastore can be attached to many workspaces; one workspace can attach to only one metastore.
- Metastores are typically created per region for compliance and performance reasons.
- Workspaces are where work happens; metastores are where governance and metadata live.
- Identity and permissions are consistent across all workspaces sharing a metastore.
- The metastore stores metadata only — actual data lives in cloud storage.
- This architecture is a major upgrade over the old, per-workspace Hive Metastore model.
