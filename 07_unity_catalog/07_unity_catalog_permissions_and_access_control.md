# Unity Catalog Permissions and Access Control

## Overview

One of the most important responsibilities of Unity Catalog is controlling who can access data.

In enterprise environments:

- Not everyone should see all data
- Some users can read data
- Some users can modify data
- Some users can create tables
- Some users should only access reporting tables

Unity Catalog implements this through a Role-Based Access Control (RBAC) model.

---

# Access Control Components

Permissions are granted to:

```text
User
Group
Service Principal
```

These are collectively called:

```text
Principals
```

---

# Access Control Architecture

### Suggested Diagram

```text
          Principal
(User / Group / Service Principal)
                    |
                    v
              Privileges
                    |
                    v
          Unity Catalog Object
```

---

# Types of Principals

## User

Represents an individual person.

Example:

```text
sonam@company.com
john@company.com
```

Granting directly:

```sql
GRANT SELECT
ON TABLE sales.gold.customers
TO `sonam@company.com`;
```

---

## Group

Represents multiple users.

Examples:

```text
data-engineers
data-scientists
analysts
admins
```

Recommended approach:

```text
Grant permissions to groups,
not individual users.
```

Example:

```sql
GRANT SELECT
ON TABLE sales.gold.customers
TO `analysts`;
```

---

## Service Principal

Represents applications or automated systems.

Used by:

- ETL Pipelines
- Databricks Jobs
- Azure Data Factory
- CI/CD Pipelines
- External Applications

Example:

```text
sales-data-pipeline-sp
```

---

# Permission Hierarchy

A user cannot directly query a table.

The user must have access at every level.

```text
Catalog
   |
Schema
   |
Table
```

Required permissions:

```text
USE CATALOG
       +
USE SCHEMA
       +
SELECT
```

---

# Example

Table:

```text
sales.gold.customer_summary
```

Required grants:

```sql
GRANT USE CATALOG
ON CATALOG sales
TO analysts;
```

```sql
GRANT USE SCHEMA
ON SCHEMA sales.gold
TO analysts;
```

```sql
GRANT SELECT
ON TABLE sales.gold.customer_summary
TO analysts;
```

Without any one of these permissions:

```text
Access Denied
```

---

# Common Privileges

## USE CATALOG

Allows users to access a catalog.

```sql
GRANT USE CATALOG
ON CATALOG sales
TO analysts;
```

Without it:

```sql
USE CATALOG sales;
```

Fails.

---

## USE SCHEMA

Allows users to access a schema.

```sql
GRANT USE SCHEMA
ON SCHEMA sales.gold
TO analysts;
```

---

## SELECT

Allows reading data.

```sql
GRANT SELECT
ON TABLE sales.gold.customer_summary
TO analysts;
```

Allows:

```sql
SELECT * FROM sales.gold.customer_summary;
```

---

## MODIFY

Allows:

- INSERT
- UPDATE
- DELETE
- MERGE

Example:

```sql
GRANT MODIFY
ON TABLE sales.gold.customer_summary
TO data-engineers;
```

---

## CREATE

Allows creation of objects.

Examples:

```text
Tables
Views
Functions
Volumes
Schemas
```

Example:

```sql
GRANT CREATE
ON SCHEMA sales.bronze
TO data-engineers;
```

---

## ALL PRIVILEGES

Grants all permissions.

```sql
GRANT ALL PRIVILEGES
ON TABLE sales.gold.customer_summary
TO admins;
```

Usually restricted to administrators.

---

# Viewing Permissions

Show grants on a table:

```sql
SHOW GRANTS
ON TABLE sales.gold.customer_summary;
```

Example output:

```text
Principal         Privilege
--------------------------------
analysts          SELECT
data-engineers    MODIFY
admins            ALL PRIVILEGES
```

---

# Revoking Permissions

Remove permissions.

Example:

```sql
REVOKE SELECT
ON TABLE sales.gold.customer_summary
FROM analysts;
```

After revoking:

```sql
SELECT * FROM sales.gold.customer_summary;
```

Returns:

```text
Permission Denied
```

---

# Ownership

Every Unity Catalog object has an owner.

Examples:

```text
Catalog Owner
Schema Owner
Table Owner
Volume Owner
```

Owner can:

- Grant permissions
- Revoke permissions
- Change ownership

---

# Transfer Ownership

Example:

```sql
ALTER TABLE sales.gold.customer_summary
OWNER TO `data-engineers`;
```

---

# Least Privilege Principle

Enterprise security follows:

```text
Give only the permissions
required to do the job.
```

Bad Practice:

```text
Everyone gets ALL PRIVILEGES
```

Good Practice:

```text
Analysts -> SELECT

Data Engineers -> MODIFY

Admins -> ALL PRIVILEGES
```

---

# Example Enterprise Setup

## Data Engineers

Permissions:

```text
USE CATALOG
USE SCHEMA
CREATE
SELECT
MODIFY
```

---

## Data Scientists

Permissions:

```text
USE CATALOG
USE SCHEMA
SELECT
```

Mostly access Silver and Gold tables.

---

## Analysts

Permissions:

```text
USE CATALOG
USE SCHEMA
SELECT
```

Usually Gold layer only.

---

## Executives

Permissions:

```text
SELECT
```

Only on reporting tables.

---

# Table-Level Security

Permissions granted directly on a table.

Example:

```sql
GRANT SELECT
ON TABLE sales.gold.customer_summary
TO analysts;
```

---

# Schema-Level Security

Permissions apply to all objects within a schema.

Example:

```sql
GRANT USE SCHEMA
ON SCHEMA sales.gold
TO analysts;
```

---

# Catalog-Level Security

Permissions apply at catalog level.

Example:

```sql
GRANT USE CATALOG
ON CATALOG sales
TO analysts;
```

---

# Row-Level Security

Controls which rows users can see.

Example:

Customer Table

```text
Country
-------
India
USA
UK
```

Requirements:

```text
India Team -> India Rows

US Team -> US Rows
```

Suggested Diagram:

```text
Customer Table
      |
      v
 Row Filter
      |
      v
 User Specific Data
```

Benefits:

- Regulatory compliance
- Data isolation
- Multi-region deployments

---

# Column-Level Security

Controls which columns users can access.

Example:

```text
Employee Table

Name
Department
Salary
SSN
```

Analysts can see:

```text
Name
Department
```

But cannot see:

```text
Salary
SSN
```

Suggested Diagram:

```text
Employee Table
      |
      v
 Column Masking
      |
      v
 Hidden Sensitive Fields
```

---

# Service Principal Example

A common interview topic.

Scenario:

ADF Pipeline loads customer data.

Create Service Principal:

```text
customer-etl-sp
```

Grant only required access:

```sql
GRANT USE CATALOG
ON CATALOG sales
TO `customer-etl-sp`;
```

```sql
GRANT USE SCHEMA
ON SCHEMA sales.bronze
TO `customer-etl-sp`;
```

```sql
GRANT SELECT
ON TABLE sales.bronze.customers
TO `customer-etl-sp`;
```

This is called:

```text
Least Privilege Access
```

---

# Auditing Access

Unity Catalog records:

```text
Who accessed data
What table was accessed
What query was executed
When access occurred
```

Useful for:

- Compliance
- Governance
- Security Monitoring

---

# Common Permission Errors

## Missing Catalog Permission

Error:

```text
Permission denied: Catalog
```

Fix:

```sql
GRANT USE CATALOG
ON CATALOG sales
TO analysts;
```

---

## Missing Schema Permission

Error:

```text
Permission denied: Schema
```

Fix:

```sql
GRANT USE SCHEMA
ON SCHEMA sales.gold
TO analysts;
```

---

## Missing Table Permission

Error:

```text
Permission denied: Table
```

Fix:

```sql
GRANT SELECT
ON TABLE sales.gold.customer_summary
TO analysts;
```

---

# Real Interview Scenario

Question:

A user has SELECT permission on a table but still cannot query it. Why?

Answer:

Because the user also requires:

```text
USE CATALOG
USE SCHEMA
SELECT
```

All three permissions must exist.

---

# Best Practices

## Use Groups Instead of Users

Good:

```sql
GRANT SELECT
ON TABLE sales.gold.customers
TO analysts;
```

Bad:

```sql
GRANT SELECT
ON TABLE sales.gold.customers
TO user1;
GRANT SELECT
ON TABLE sales.gold.customers
TO user2;
GRANT SELECT
ON TABLE sales.gold.customers
TO user3;
```

---

## Follow Least Privilege

Only grant necessary permissions.

---

## Separate Admins and Users

Admins:

```text
ALL PRIVILEGES
```

Users:

```text
SELECT
```

Only if needed.

---

## Regular Permission Audits

Run:

```sql
SHOW GRANTS
ON TABLE sales.gold.customer_summary;
```

Review permissions regularly.

---

# Key Takeaways

- Unity Catalog uses Role-Based Access Control (RBAC).
- Permissions are granted to Users, Groups, and Service Principals.
- To query a table, users need:
  
  ```text
  USE CATALOG
  + USE SCHEMA
  + SELECT
  ```

- Common privileges include:
  
  - SELECT
  - MODIFY
  - CREATE
  - USE CATALOG
  - USE SCHEMA

- Service Principals are used for automation and ETL pipelines.
- Row-Level and Column-Level Security provide fine-grained access control.
- Always follow the Principle of Least Privilege.
- Prefer Groups over individual Users when assigning permissions.