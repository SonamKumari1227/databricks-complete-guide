# Unity Catalog: Enterprise Best Practices

## Overview

Setting up Unity Catalog correctly at the **enterprise scale** requires more planning than a small team setup.

This topic brings together best practices across:

```text
Catalog Structure
Naming Conventions
Access Control Strategy
Environment Isolation
Governance Operations
Cost & Performance
Change Management
```

---

# 1. Catalog Structure Strategy

## Recommended Pattern: Environment-Based Catalogs

```text
dev_catalog
staging_catalog
prod_catalog
```

Alternative Pattern: Domain-Based Catalogs (within environment)

```text
prod_catalog
   |
   -----------------------------
   |            |               |
   v            v               v
 sales        finance        marketing
```

## Combined Pattern (Common at Large Enterprises)

```text
                     Metastore
                          |
        ------------------------------------
        |                |                 |
        v                v                 v
   dev_catalog     staging_catalog     prod_catalog
        |                |                 |
   ---------        ---------         ---------
   |   |   |        |   |   |         |   |   |
   v   v   v        v   v   v         v   v   v
 sales fin mktg    sales fin mktg    sales fin mktg
```

This keeps environment separation **and** domain separation clear and consistent.

---

# 2. Naming Conventions

Consistency avoids confusion at scale.

```text
Catalog:  <environment>            e.g. prod
Schema:   <domain>_<layer>         e.g. sales_gold
Table:    <descriptive_name>       e.g. monthly_sales_summary
```

Example:

```text
prod.sales_gold.monthly_sales_summary
```

Avoid:

```text
customers_final
customers_final_v2
customers_final_new
```

Prefer:

```text
prod.sales_gold.customer_summary   (single source of truth)
```

---

# 3. Access Control Strategy

## Use Groups, Not Individual Users

```text
Bad:
GRANT SELECT ON TABLE ... TO `sonam@company.com`;

Good:
GRANT SELECT ON TABLE ... TO `sales_analysts_group`;
```

## Apply Least Privilege

```text
Grant only what is needed:
    SELECT for analysts
    MODIFY for engineers
    ALL PRIVILEGES only for admins/owners
```

## Layer Security Properly

```text
Catalog/Schema/Table Permissions  -> broad access control
Row-Level Security                -> restrict visible rows
Column-Level Security             -> mask sensitive columns
Workspace-Catalog Binding         -> restrict by environment
```

---

# 4. Environment Isolation

```text
                Enterprise Governance Boundary
                            |
          -----------------------------------
          |                |                 |
          v                v                 v
     Dev Catalog      Staging Catalog     Prod Catalog
          |                |                 |
          v                v                 v
    bound to Dev      bound to Staging   bound to Prod
      Workspace          Workspace         Workspace
```

Never allow production catalogs to be accessible from development workspaces, even accidentally — use Workspace-Catalog Binding to enforce this.

---

# 5. Governance Operations Checklist

```text
[ ] Every important table has an OWNER
[ ] Every important table has a DESCRIPTION
[ ] Sensitive columns have documented CLASSIFICATION tags
[ ] Consistent TAGGING standard is enforced org-wide
[ ] Row/Column security applied where required
[ ] Audit logs reviewed periodically for sensitive tables
[ ] Lineage checked before making breaking schema changes
```

---

# 6. Tagging Standards

Define a standard, enforced set of tags across the organization:

```text
domain          = sales | finance | marketing | hr
environment     = dev | staging | prod
classification  = public | internal | confidential | restricted
owner           = team-name
```

```sql
ALTER TABLE prod.sales_gold.customer_summary
SET TAGS ('domain' = 'sales', 'classification' = 'internal');
```

---

# 7. Change Management for Schema Changes

Before modifying or dropping a column/table:

```text
Step 1: Check Lineage
        -> Which tables/dashboards depend on this?

Step 2: Check Permissions
        -> Who currently has access?

Step 3: Notify Owners of Downstream Assets

Step 4: Apply Change in Dev/Staging First

Step 5: Validate

Step 6: Apply to Production
```

```text
        Proposed Change
              |
              v
       Check Lineage (Impact Analysis)
              |
              v
       Test in Dev/Staging
              |
              v
       Apply to Production
              |
              v
       Monitor Audit Logs Post-Change
```

---

# 8. Cost and Performance Best Practices

```text
Use appropriately sized SQL Warehouses for workloads
Monitor usage via system.billing.usage
Avoid unnecessary full-table scans; leverage partitioning/clustering
Archive or delete unused/duplicate tables (e.g. "_v2", "_final" clutter)
Regularly review unused permissions and stale service principals
```

---

# 9. Security Hardening Checklist

```text
[ ] Service Principals used for all automation (no personal tokens)
[ ] OAuth used instead of long-lived static tokens where possible
[ ] Sensitive columns masked (SSN, salary, card numbers, etc.)
[ ] Row-level security applied for multi-tenant / regional data
[ ] Workspace-Catalog Binding applied for environment isolation
[ ] Regular audit log reviews for sensitive data access
[ ] Least privilege enforced across all groups
```

---

# 10. Governance Operating Model (RACI-style)

```text
Role                  Responsibility
--------------------  --------------------------------------
Platform/Admin Team   Metastore setup, storage credentials,
                       workspace-catalog binding, top-level catalogs

Data Engineering      Schema design, table creation, lineage
                       health, pipeline ownership

Data Governance Team  Tagging standards, classification policy,
                       audit log review, compliance reporting

Data Owners           Table-level descriptions, ownership,
                       approving access requests

Analysts/Consumers    Discover and use approved, governed datasets
```

---

# Real-World Example: Enterprise Rollout Plan

A large enterprise onboarding Unity Catalog org-wide might follow:

```text
Phase 1: Foundation
   - Set up Metastore(s) per region
   - Define catalog structure (env + domain)
   - Set up storage credentials & external locations

Phase 2: Migration
   - Migrate critical tables from Hive Metastore
   - Recreate permissions using groups

Phase 3: Governance Rollout
   - Apply tagging standards
   - Add descriptions & ownership metadata
   - Apply row/column security on sensitive tables

Phase 4: Operationalize
   - Set up dashboards using System Tables
   - Set up alerts for cost, failures, and sensitive access
   - Train teams on Data Discovery tools

Phase 5: Continuous Governance
   - Periodic audit log reviews
   - Regular access reviews
   - Ongoing lineage-based impact analysis before changes
```

---

# Enterprise Governance Mental Model

```text
                    ENTERPRISE UNITY CATALOG
                             |
        -------------------------------------------
        |            |            |               |
        v            v            v               v
   Structure     Security      Operations      Culture
        |            |            |               |
        v            v            v               v
  Catalog/Schema  Permissions   Monitoring     Documentation
  Naming Conv.    Row/Column    Cost Mgmt      Ownership
  Env Isolation   Binding       Change Mgmt    Discoverability
```

---

# Important Interview Questions

## What catalog structure strategy is commonly recommended at enterprise scale?

A combined approach: top-level catalogs by environment (dev/staging/prod), with domain-based schemas (sales, finance, marketing) inside each, often reinforced with Workspace-Catalog Binding.

---

## Why should groups be used instead of individual user grants?

Groups scale better as teams grow and change, reduce administrative overhead, and make audits and access reviews far simpler than managing individual user-level grants.

---

## What should be checked before making a breaking schema change?

Lineage should be checked first to understand downstream dependencies (tables, dashboards, pipelines), followed by testing the change in dev/staging before applying it to production.

---

## What is a common governance operations checklist for enterprise Unity Catalog?

Ensuring every important table has an owner, description, and classification tags; enforcing consistent tagging; applying row/column security where needed; and periodically reviewing audit logs.

---

## How can system tables support enterprise cost governance?

`system.billing.usage` can be queried and grouped by workspace or team to build chargeback/showback dashboards and identify high-cost workloads.

---

# Best Practices Summary

## 1. Design Catalog Structure Deliberately

Combine environment and domain separation from the start; retrofitting later is costly.

---

## 2. Standardize Naming and Tagging

Define and enforce naming conventions and tag taxonomies org-wide.

---

## 3. Always Use Groups for Permissions

Never grant directly to individual users for production data.

---

## 4. Layer All Security Controls

Combine catalog/schema/table permissions, row/column security, and workspace-catalog binding for defense in depth.

---

## 5. Operationalize With System Tables

Build dashboards and alerts on system tables rather than relying on manual checks.

---

## 6. Treat Governance as Ongoing, Not One-Time

Schedule regular audit reviews, access reviews, and lineage-based impact checks.

---

# Key Takeaways

- Enterprise Unity Catalog rollout requires deliberate catalog structure, naming, and tagging standards.
- Combine environment-based and domain-based catalog structuring for clarity at scale.
- Always grant permissions via groups, following least privilege principles.
- Layer permissions, row/column security, and workspace-catalog binding for defense in depth.
- Use lineage for impact analysis before any schema change.
- Use System Tables to operationalize cost monitoring, job health, and access auditing.
- Treat governance as a continuous operating model, not a one-time setup — with clear ownership across platform, engineering, governance, and data owner roles.
