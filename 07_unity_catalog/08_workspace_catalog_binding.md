# Unity Catalog: Workspace-Catalog Binding

## Overview

A single Unity Catalog metastore can be attached to **multiple workspaces**.

By default, once a catalog exists in the metastore, it is visible to **every workspace** connected to that metastore.

But organizations often need more control:

```text
"Production catalog should only be usable from the Production workspace"

"Development catalog should not be accessible from Production"
```

This is where **Workspace-Catalog Binding** comes in.

---

# What Is Workspace-Catalog Binding?

Workspace-Catalog Binding restricts which **workspaces** are allowed to access a given **catalog**.

```text
Metastore
   |
   v
Catalog
   |
   v
Bound to specific Workspace(s) only
```

Without binding:

```text
Catalog is accessible from ANY workspace attached to the metastore
```

With binding:

```text
Catalog is accessible ONLY from the workspace(s) it is explicitly bound to
```

---

# Why Workspace-Catalog Binding Is Needed

## 1. Environment Isolation

```text
dev_catalog     -> bound to Dev Workspace only
staging_catalog -> bound to Staging Workspace only
prod_catalog    -> bound to Production Workspace only
```

This prevents developers from accidentally querying production data from a dev workspace.

---

## 2. Regulatory / Compliance Boundaries

```text
eu_customer_data -> bound to EU Workspace only
us_customer_data -> bound to US Workspace only
```

Useful when data residency or regional compliance rules apply.

---

## 3. Business Unit Separation

```text
finance_catalog -> bound to Finance Workspace
marketing_catalog -> bound to Marketing Workspace
```

Keeps departments working within their own boundaries even though they share one metastore.

---

# Architecture Diagram

```text
                         Metastore
                             |
             ---------------------------------
             |                |               |
             v                v               v
        dev_catalog     staging_catalog   prod_catalog
             |                |               |
             v                v               v
       Dev Workspace    Staging Workspace  Prod Workspace
```

Each catalog is only usable in its bound workspace, even though all workspaces share the same metastore.

---

# How Binding Works

By default:

```text
A catalog created in the metastore is available to ALL workspaces
attached to that metastore.
```

To restrict it:

```text
Bind the catalog to one or more specific workspaces.
```

Once bound:

```text
Only users in the bound workspace(s) can use the catalog,
even if they technically have permissions granted on it.
```

---

## Example Scenario

Suppose a metastore is attached to three workspaces:

```text
ws_dev
ws_staging
ws_prod
```

And there is a catalog:

```text
finance_prod
```

Without binding:

```text
finance_prod is visible/usable from ws_dev, ws_staging, and ws_prod
```

With binding applied:

```text
finance_prod -> bound only to ws_prod
```

Result:

```text
Even a user with SELECT permission on finance_prod
CANNOT query it from ws_dev or ws_staging.
They can only query it from ws_prod.
```

---

# Setting Workspace-Catalog Binding (Conceptual Steps)

```text
1. Open the Catalog settings in the Databricks Account Console
   or the Data Explorer.
2. Navigate to the "Workspaces" tab for the catalog.
3. Restrict the catalog to specific workspace(s) instead of "All Workspaces".
4. Save the binding.
```

This can also be managed via the Databricks CLI / API / Terraform for automation:

```text
databricks catalogs update <catalog_name> --workspaces <workspace_id_list>
```

---

# Binding + Permissions Work Together

Binding does **not replace** permissions — it adds an extra boundary.

```text
Permissions -> WHO can access the catalog (users/groups)

Binding     -> WHERE (which workspace) the catalog can be accessed from
```

```text
                     Catalog Access Check
                             |
             --------------------------------
             |                              |
             v                              v
     Is the workspace bound?        Does the user have
     to this catalog?               permission (GRANT)?
             |                              |
             v                              v
           YES/NO   ------ AND ------->   YES/NO
                             |
                             v
                    Access Allowed only if BOTH are true
```

---

# Real-World Example

A company has:

```text
Metastore: central_metastore
Workspaces: ws_dev, ws_prod
Catalogs: dev_catalog, prod_catalog
```

Governance goal:

```text
Developers should experiment freely in Dev,
but must never touch Production data by mistake.
```

Solution:

```text
dev_catalog  -> bound to ws_dev only
prod_catalog -> bound to ws_prod only
```

Even if a developer is accidentally granted SELECT on `prod_catalog`,
they still cannot query it while working inside `ws_dev`,
because the catalog is not bound to that workspace.

---

# Workspace-Catalog Binding vs Permissions vs Row/Column Security

| Layer | Controls |
|-------|----------|
| Workspace-Catalog Binding | Which workspace can even see/use the catalog |
| Permissions (GRANT) | Which users/groups can access objects |
| Row-Level Security | Which rows a user can see |
| Column-Level Security | What value a user sees in a column |

Mental Model:

```text
Binding     = Which building can you enter?

Permissions = Which rooms can you enter?

Row/Column  = What can you see once you're inside the room?
```

---

# Important Interview Questions

## What is Workspace-Catalog Binding in Unity Catalog?

Workspace-Catalog Binding restricts which workspaces attached to a metastore are allowed to access a given catalog, adding an environment-level boundary on top of normal permissions.

---

## Why is Workspace-Catalog Binding useful?

It prevents accidental cross-environment access — for example, stopping a development workspace from accessing a production catalog — even when a user technically has permission on that catalog.

---

## Is a catalog accessible from all workspaces by default?

Yes, by default a catalog in a metastore is accessible from every workspace attached to that metastore, unless it is explicitly bound to specific workspaces.

---

## Does Workspace-Catalog Binding replace GRANT permissions?

No. Binding controls which workspace can access a catalog, while GRANT permissions control which users or groups can access objects within it. Both checks must pass for access to be allowed.

---

## Give a real-world use case for Workspace-Catalog Binding.

Isolating production data by binding a production catalog only to the production workspace, so developers working in a dev workspace cannot query production data even accidentally.

---

# Best Practices

## 1. Separate Catalogs by Environment

Maintain distinct catalogs for dev, staging, and production, and bind each to its respective workspace.

---

## 2. Bind Sensitive Catalogs Explicitly

Don't rely only on permissions for highly sensitive catalogs — add workspace binding as an extra safeguard.

---

## 3. Review Bindings Periodically

As new workspaces are added to a metastore, review and update catalog bindings to avoid unintended exposure.

---

## 4. Combine With Row/Column Security

Use binding for environment isolation, and row/column security for fine-grained access within an environment.

---

## 5. Automate Binding via Terraform/CLI

For large organizations, manage bindings as code to keep them consistent and auditable.

---

# Complete Mental Model

```text
                     METASTORE
                         |
        ---------------------------------
        |                |               |
        v                v               v
   dev_catalog     staging_catalog   prod_catalog
        |                |               |
        v                v               v
   ws_dev only      ws_staging only   ws_prod only
```

---

# Key Takeaways

- Workspace-Catalog Binding restricts which workspaces can access a catalog.
- By default, catalogs are visible to all workspaces attached to a metastore.
- Binding adds an environment-level safeguard on top of normal GRANT permissions.
- It is commonly used to isolate dev, staging, and production environments.
- Access requires BOTH a valid workspace binding AND proper permissions.
- Binding does not replace permissions, row-level security, or column-level security — it works alongside them.
- Best used for sensitive catalogs and clear environment separation.
