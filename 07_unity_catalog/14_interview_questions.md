# Unity Catalog: Complete Interview Questions

## Overview

This is a consolidated set of interview questions covering **all** Unity Catalog topics, organized by theme, for quick review before an interview.

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

---

# 1. Fundamentals & Architecture

## What is Unity Catalog?

Unity Catalog is Databricks' centralized data governance solution that manages permissions, metadata, lineage, auditing, and discovery across all workspaces attached to a metastore.

## What is the three-level namespace in Unity Catalog?

`catalog.schema.table` — catalog is the top-level container, schema groups related tables, and table/view holds the actual queryable data.

## How is Unity Catalog different from the legacy Hive Metastore?

Unity Catalog is centralized and shared across workspaces with built-in lineage, auditing, and fine-grained security, while Hive Metastore is isolated per workspace with a two-level namespace and no built-in governance features.

---

# 2. Permissions & Access Control

## How does Unity Catalog manage permissions?

Through a GRANT/REVOKE model applied at the catalog, schema, table, view, or function level, typically assigned to groups.

## What is the difference between USE CATALOG and SELECT privileges?

`USE CATALOG` allows a user to reference/traverse into a catalog, while `SELECT` allows them to actually read data from a table within it — both are typically required together.

## Why are groups preferred over individual users for GRANTs?

Groups scale better, simplify audits, and reduce the operational overhead of managing access as team membership changes.

---

# 3. Storage Credentials, External Locations, Volumes

## What is a Storage Credential?

A Storage Credential is an object that stores the authentication mechanism (e.g., IAM role, managed identity) Unity Catalog uses to access cloud storage.

## What is an External Location?

An External Location combines a Storage Credential with a specific cloud storage path, defining exactly where and how Unity Catalog can read/write data.

## What is a Volume in Unity Catalog?

A Volume is a governed object for managing non-tabular files (e.g., images, PDFs, raw files) within Unity Catalog's governance model.

---

# 4. Lineage, Auditing, Discovery

## What is Data Lineage?

Data lineage tracks the flow of data between source, transformation, and destination assets, showing where data comes from and where it goes.

## What is Data Auditing?

Data auditing tracks activities such as data access, queries, and permission-related operations, answering who did what and when.

## What is the difference between Lineage and Auditing?

Lineage answers "where did the data come from/go?" (data flow), while auditing answers "who accessed or changed something and when?" (user activity).

## What is Data Discovery?

Data discovery is the process of finding and understanding available data assets using metadata, search, descriptions, tags, ownership, and lineage.

## Does Unity Catalog support column-level lineage?

Yes, for supported workloads, Unity Catalog can track lineage down to individual columns, not just entire tables.

---

# 5. Delta Sharing

## What is Delta Sharing?

Delta Sharing is an open protocol that lets organizations securely share live data with other organizations without copying it or requiring the recipient to use Databricks.

## What is a Share and a Recipient in Delta Sharing?

A Share is a collection of tables/data objects being shared; a Recipient is the identity (organization or user) granted access to that Share.

## Does the recipient need to be on Databricks to use Delta Sharing?

No — Delta Sharing is an open protocol, so recipients can access shared data using various clients, not only Databricks.

---

# 6. Row-Level and Column-Level Security

## What is Row-Level Security in Unity Catalog?

Row-Level Security restricts which rows of a table a user can see, based on rules defined in a SQL function tied to the user's identity or group membership.

## What is Column-Level Security (Column Masking)?

Column masking controls what value a user sees for a specific column — the real value, a masked value, or NULL — depending on who is querying.

## How are row filters and column masks applied?

Using `ALTER TABLE ... SET ROW FILTER` for row filters and `ALTER TABLE ... ALTER COLUMN ... SET MASK` for column masks, both backed by SQL functions.

## Can row filters and column masks be combined on one table?

Yes, both can be active on the same table simultaneously for fine-grained governance.

---

# 7. Service Principals & OAuth

## What is a Service Principal?

A Service Principal is a non-human identity used by applications, jobs, or automated processes to authenticate and interact with Databricks and Unity Catalog.

## Why use Service Principals instead of personal accounts for automation?

To avoid tying automation to an individual's credentials, which creates risk if that person leaves, and to provide a dedicated, auditable identity for pipelines.

## What is OAuth and why is it preferred?

OAuth is a token-based authentication standard that issues short-lived, scoped tokens, offering better security than long-lived static passwords or tokens.

---

# 8. Workspace-Catalog Binding

## What is Workspace-Catalog Binding?

It restricts which workspaces attached to a metastore are allowed to access a given catalog, adding an environment-level boundary beyond normal permissions.

## Is a catalog accessible from all workspaces by default?

Yes, unless it is explicitly bound to specific workspaces.

## Does binding replace GRANT permissions?

No — both checks (binding AND permissions) must pass for access to be granted.

---

# 9. Metastore / Workspace Architecture

## What is a Metastore?

The top-level container in Unity Catalog that stores metadata about catalogs, schemas, tables, and permissions — typically one per region.

## Can one metastore serve multiple workspaces?

Yes, a single metastore can be attached to multiple workspaces, but each workspace can be attached to only one metastore at a time.

## Why are metastores usually created per region?

To keep governance close to compute/storage and to help meet data residency and compliance requirements.

---

# 10. Hive Metastore Migration

## Why migrate from Hive Metastore to Unity Catalog?

To gain centralized governance, cross-workspace permissions, built-in lineage/auditing, row/column security, and better discovery — none of which the isolated Hive Metastore provides.

## What is the SYNC command used for?

`SYNC` migrates and keeps a Unity Catalog table synchronized with its corresponding Hive Metastore source table during a phased migration.

## Do Hive Metastore permissions carry over automatically?

No — permissions must be manually recreated using Unity Catalog's GRANT/REVOKE model.

---

# 11. System Tables & Monitoring

## What are System Tables?

Delta tables automatically managed by Databricks that expose operational data — audit logs, billing usage, job runs, lineage — as queryable SQL tables under the `system` catalog.

## Name key system table schemas.

`system.access` (audit/lineage), `system.billing` (usage/cost), `system.compute` (clusters/warehouses), `system.lakeflow` (jobs), `system.query` (query history).

## How can system tables support cost governance?

By querying `system.billing.usage` grouped by workspace or SKU to build cost dashboards and identify high-cost workloads.

---

# 12. Enterprise Best Practices

## What catalog structure is recommended at enterprise scale?

A combined approach: environment-based top-level catalogs (dev/staging/prod) with domain-based schemas inside each, reinforced with Workspace-Catalog Binding.

## What should be checked before a breaking schema change?

Lineage, to understand downstream dependencies, followed by testing in dev/staging before applying to production.

## What layered security model does Unity Catalog support?

Catalog/schema/table permissions (broad access), row-level security (row visibility), column-level security (value masking), and workspace-catalog binding (environment isolation) — used together for defense in depth.

---

# Rapid-Fire Round (Short Answers)

```text
Q: What does GRANT SELECT allow?
A: Read access to a table.

Q: What connects Unity Catalog to cloud storage?
A: Storage Credentials + External Locations.

Q: What answers "who accessed this table"?
A: Auditing (system.access.audit).

Q: What answers "where did this data come from"?
A: Lineage.

Q: What helps users find the right dataset?
A: Data Discovery (metadata, tags, ownership, descriptions).

Q: What identity type is used for automated pipelines?
A: Service Principal.

Q: What secures automated authentication?
A: OAuth.

Q: What restricts a catalog to specific workspaces?
A: Workspace-Catalog Binding.

Q: What restricts visible rows for a user?
A: Row-Level Security (row filters).

Q: What hides/masks column values?
A: Column-Level Security (column masks).

Q: What is the top-level metadata container in Unity Catalog?
A: The Metastore.

Q: What is the legacy system Unity Catalog replaces?
A: Hive Metastore.

Q: What exposes operational data as SQL tables?
A: System Tables.

Q: What enables secure data sharing outside Databricks?
A: Delta Sharing.
```

---

# Mental Model Recap (All Topics)

```text
                          UNITY CATALOG
                               |
      -----------------------------------------------------------
      |          |          |          |          |             |
      v          v          v          v          v             v
 Security    Lineage    Auditing   Discovery   Sharing     Governance Ops
      |          |          |          |          |             |
      v          v          v          v          v             v
Permissions  Data Flow   Who did    Find Data  External     Metastore/
Row/Column   Tracking    What/When  (Tags,     Data Access  Workspace
Security                            Owners)    (Delta       Architecture,
Binding                                        Sharing)     Migration,
Service                                                     System Tables,
Principals                                                  Best Practices
/OAuth
```

---

# Key Takeaways

- Unity Catalog unifies security, lineage, auditing, discovery, and sharing under one governance layer.
- Permissions use GRANT/REVOKE, ideally assigned to groups, following least privilege.
- Row-level and column-level security add fine-grained control beyond table-level permissions.
- Service Principals and OAuth enable secure automation without personal credentials.
- Workspace-Catalog Binding and multi-workspace metastore architecture enable environment isolation at scale.
- Migrating from Hive Metastore requires namespace changes and manual permission recreation.
- System Tables turn operational monitoring (cost, jobs, audit) into plain SQL queries.
- Enterprise best practices tie all of this together with structure, naming, tagging, and layered security.
