# Unity Catalog: Hive Metastore Migration

## Overview

Before Unity Catalog, Databricks used the **Hive Metastore (HMS)** to store table metadata.

Many organizations still have data registered in Hive Metastore and need to migrate to Unity Catalog to get centralized governance, lineage, auditing, and fine-grained security.

This topic covers:

```text
Why migrate
Differences between Hive Metastore and Unity Catalog
Migration strategies
Step-by-step migration approach
```

---

# 1. What Is the Hive Metastore?

The Hive Metastore is the legacy metadata store used by Databricks (and other big data tools) before Unity Catalog.

```text
Hive Metastore
      |
      v
Stores table/schema metadata
      |
      v
Scoped PER WORKSPACE (not shared)
```

---

## Old Architecture (Before Unity Catalog)

```text
Workspace A -> Hive Metastore A -> Tables (isolated)
Workspace B -> Hive Metastore B -> Tables (isolated)
Workspace C -> Hive Metastore C -> Tables (isolated)
```

Each workspace had its own disconnected metastore.

Problems:

```text
No centralized governance
No cross-workspace permissions
No built-in lineage
No fine-grained access control (row/column security)
Difficult auditing across workspaces
```

---

# 2. Why Migrate to Unity Catalog?

```text
Centralized governance across all workspaces
Consistent permission model (GRANT/REVOKE)
Built-in lineage tracking
Built-in auditing
Row-level and column-level security
Data discovery across the whole organization
Support for Delta Sharing
```

Mental Model:

```text
Hive Metastore = Isolated, workspace-local metadata

Unity Catalog  = Centralized, account-wide governance
```

---

# 3. Two-Level vs Three-Level Namespace

## Hive Metastore (Two-Level Namespace)

```text
schema.table
```

Example:

```text
sales.orders
```

## Unity Catalog (Three-Level Namespace)

```text
catalog.schema.table
```

Example:

```text
main.sales.orders
```

This is one of the biggest structural changes during migration — every reference to a table needs an extra "catalog" level.

---

# 4. Migration Architecture Diagram

```text
        Hive Metastore (Legacy)
                  |
                  v
        hive_metastore.schema.table
                  |
          Migration Process
                  |
                  v
     unity_catalog.schema.table
                  |
                  v
     Centralized Governance Applied
     (Permissions, Lineage, Auditing, Discovery)
```

---

# 5. Migration Strategies

## Strategy 1: In-Place Migration (Metadata Only)

Used when data files don't need to move — only metadata is registered into Unity Catalog.

```text
Hive Table (path: s3://bucket/sales/orders)
        |
        v
CREATE TABLE in Unity Catalog pointing to same location
        |
        v
Table now governed by Unity Catalog
```

Best for:

```text
External tables that already sit on well-structured cloud storage
```

---

## Strategy 2: CTAS-Based Migration (Copy Data)

Used when data needs to be physically copied into a Unity Catalog-managed table.

```sql
CREATE TABLE main.sales.orders
AS SELECT * FROM hive_metastore.sales.orders;
```

Best for:

```text
Managed tables
Tables that need cleanup or restructuring during migration
```

---

## Strategy 3: Using Databricks Upgrade Wizard / SYNC Command

Databricks provides a `SYNC` command and UI-based "Upgrade to Unity Catalog" wizard to automate migration for many tables at once.

```sql
SYNC TABLE main.sales.orders
FROM hive_metastore.sales.orders;
```

```text
SYNC keeps the destination Unity Catalog table
updated with changes from the Hive Metastore source table
(useful during a phased migration).
```

---

# 6. Migration Diagram: Full Process

```text
                 Step 1: Assess
        Inventory all Hive Metastore tables
                      |
                      v
                 Step 2: Plan
      Decide catalog/schema structure in Unity Catalog
                      |
                      v
              Step 3: Set Up Unity Catalog
     Create Metastore, Catalogs, Schemas, Storage Credentials
                      |
                      v
              Step 4: Migrate Tables
     Use SYNC / CTAS / In-place registration
                      |
                      v
              Step 5: Recreate Permissions
       Reapply GRANTs using Unity Catalog's model
                      |
                      v
              Step 6: Update Downstream References
   Update notebooks, jobs, dashboards to use new 3-level names
                      |
                      v
              Step 7: Validate
     Confirm data correctness, lineage, and permissions
                      |
                      v
              Step 8: Decommission Hive Metastore Tables
         (once fully validated and no longer needed)
```

---

# 7. Handling Permissions During Migration

Hive Metastore permissions do **not** automatically transfer to Unity Catalog — they must be **recreated** using Unity Catalog's GRANT model.

```text
Hive Metastore Permissions (Legacy ACLs)
                |
                v
        Manual Review Required
                |
                v
     Recreate using Unity Catalog GRANT statements
```

Example:

```sql
GRANT SELECT ON TABLE main.sales.orders TO `analysts_group`;
GRANT MODIFY ON TABLE main.sales.orders TO `data_engineering_group`;
```

---

# 8. Common Migration Challenges

```text
Two-level to three-level namespace changes break existing code
Permissions need to be manually redefined
Some workloads may reference hardcoded table paths
Large volumes of tables require a phased, prioritized approach
Testing is required to ensure lineage and access work as expected
```

---

# Real-World Example

A company has 500 tables in Hive Metastore across 3 workspaces, with no centralized governance.

Migration approach:

```text
1. Inventory all 500 tables and classify by importance
   (critical, medium, low priority).

2. Create a Unity Catalog structure:
      prod_catalog.finance
      prod_catalog.sales
      prod_catalog.marketing

3. Migrate critical tables first using SYNC 
   (keeps them updated during transition).

4. Migrate remaining tables using CTAS where data cleanup is needed.

5. Recreate permissions using GRANT statements 
   based on existing team structures.

6. Update notebooks/jobs to reference the new 
   catalog.schema.table format.

7. Run both systems in parallel temporarily, 
   then decommission Hive Metastore tables 
   once validation is complete.
```

---

# Hive Metastore vs Unity Catalog

| Feature | Hive Metastore | Unity Catalog |
|---------|---------------|----------------|
| Namespace | schema.table (2-level) | catalog.schema.table (3-level) |
| Scope | Per workspace | Shared across workspaces |
| Governance | Limited | Centralized |
| Lineage | Not built-in | Built-in |
| Auditing | Limited | Built-in |
| Row/Column Security | Not supported | Supported |
| Data Discovery | Limited | Built-in |
| Delta Sharing | Not integrated | Fully integrated |

---

# Important Interview Questions

## Why do organizations migrate from Hive Metastore to Unity Catalog?

To gain centralized governance, cross-workspace permissions, built-in lineage and auditing, fine-grained row/column security, and better data discovery — none of which the isolated, per-workspace Hive Metastore provides.

---

## What is the key structural difference between Hive Metastore and Unity Catalog?

Hive Metastore uses a two-level namespace (`schema.table`), while Unity Catalog uses a three-level namespace (`catalog.schema.table`).

---

## What is the SYNC command used for in migration?

`SYNC` is used to migrate and keep a Unity Catalog table synchronized with its corresponding Hive Metastore source table during a phased migration.

---

## Do Hive Metastore permissions carry over automatically to Unity Catalog?

No. Permissions must be manually recreated using Unity Catalog's GRANT/REVOKE model after migration.

---

## What are the main migration strategies available?

In-place metadata registration (for external tables), CTAS-based copying (for managed tables or restructuring), and using the SYNC command or upgrade wizard for automated, phased migration.

---

# Best Practices

## 1. Inventory Before Migrating

Catalog all existing Hive Metastore tables and classify them by priority before starting.

---

## 2. Migrate in Phases

Start with a pilot group of tables, validate the approach, then scale to the rest of the organization.

---

## 3. Recreate Permissions Deliberately

Don't assume old ACLs map directly — review and redesign permissions using groups where possible.

---

## 4. Update Code References Early

Identify and update all notebooks, jobs, and dashboards referencing the old two-level namespace.

---

## 5. Run in Parallel During Transition

Keep both Hive Metastore and Unity Catalog tables available temporarily to validate correctness before cutover.

---

## 6. Decommission Only After Validation

Remove old Hive Metastore tables only after confirming data accuracy, permissions, and downstream compatibility.

---

# Complete Mental Model

```text
        Hive Metastore (Legacy, Per-Workspace)
                       |
                       v
              Migration (SYNC / CTAS / In-place)
                       |
                       v
        Unity Catalog (Centralized, Account-Wide)
                       |
                       v
   Governance: Permissions + Lineage + Auditing + Discovery
```

---

# Key Takeaways

- Hive Metastore is the legacy, per-workspace metadata store used before Unity Catalog.
- Unity Catalog uses a three-level namespace (catalog.schema.table) vs Hive's two-level namespace.
- Migration strategies include in-place registration, CTAS-based copying, and the SYNC command.
- Permissions do not migrate automatically — they must be recreated in Unity Catalog.
- Migration should be phased: assess, plan, set up, migrate, reapply permissions, validate, decommission.
- Downstream code (notebooks, jobs, dashboards) must be updated to the new namespace.
- Unity Catalog provides governance capabilities Hive Metastore never had: lineage, auditing, fine-grained security, and discovery.
