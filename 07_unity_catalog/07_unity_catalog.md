# Unity Catalog

## What is Unity Catalog?

Unity Catalog (UC) is Databricks' centralized governance solution that manages:

- Metadata
- Security
- Access Control
- Data Lineage
- Auditing
- Data Discovery

It provides a single place to govern all data assets across Databricks workspaces.

---

## Why Unity Catalog?

Before Unity Catalog, Databricks used Hive Metastore where governance was limited to individual workspaces.

Problems:

- Difficult permission management
- No centralized governance
- Limited auditing
- No built-in lineage
- Hard to manage multiple workspaces

Unity Catalog solves these problems by providing centralized governance across the entire Lakehouse.

---

## Unity Catalog Architecture

### Suggested Diagram

```
Unity Catalog Architecture
```

```text
                    +------------------+
                    | Unity Catalog    |
                    | Metastore        |
                    +------------------+
                             |
      ---------------------------------------------
      |                   |                     |
      v                   v                     v
   Catalog A          Catalog B           Catalog C
      |                   |                     |
      v                   v                     v
   Schemas            Schemas              Schemas
      |                   |                     |
      v                   v                     v
 Tables/Views       Tables/Views        Tables/Views
```

---

# Core Hierarchy

Unity Catalog follows a three-level namespace.

```text
Metastore
    |
Catalog
    |
Schema
    |
Table/View/Function/Volume
```

Example:

```text
sales
   |
bronze
   |
customers
```

Fully Qualified Name:

```sql
sales.bronze.customers
```

---

# Core Components

## 1. Metastore

Top-level governance container.

Stores:

- Metadata
- Permissions
- Audit Information
- Object Definitions

Usually:

```text
1 Metastore per Region
```

Example:

```text
aws-us-east-1-metastore
```

---

## 2. Catalog

Highest logical container.

Examples:

```text
sales
finance
marketing
hr
```

Create Catalog:

```sql
CREATE CATALOG sales;
```

Think of Catalog as a business domain.

---

## 3. Schema

Schemas organize related objects within a catalog.

Examples:

```text
bronze
silver
gold
analytics
```

Create Schema:

```sql
CREATE SCHEMA sales.bronze;
```

---

## 4. Tables

Store actual data.

Example:

```sql
CREATE TABLE sales.bronze.customers
(
    customer_id INT,
    customer_name STRING
);
```

---

## 5. Views

Virtual tables built from SQL queries.

Example:

```sql
CREATE VIEW sales.gold.active_customers AS
SELECT *
FROM sales.silver.customers
WHERE active = TRUE;
```

Benefits:

- Simplifies reporting
- Provides abstraction
- Adds security layer

---

## 6. Functions

Reusable SQL logic.

Example:

```sql
CREATE FUNCTION sales.gold.get_year(d DATE)
RETURNS INT
RETURN YEAR(d);
```

---

## 7. Volumes

Used for non-tabular files.

Examples:

- CSV
- JSON
- Excel
- PDFs
- Images
- Machine Learning Models

Create Volume:

```sql
CREATE VOLUME sales.bronze.raw_files;
```

Access Path:

```text
/dbfs/Volumes/sales/bronze/raw_files/
```

---

# Unity Catalog Storage Architecture

### Suggested Diagram

```
Unity Catalog + Cloud Storage
```

```text
Unity Catalog
      |
      v
Storage Credential
      |
      v
External Location
      |
      v
ADLS / S3 / GCS
      |
      v
Delta Tables
```

---

# Managed vs External Tables

## Managed Table

Databricks manages:

- Metadata
- Storage

```sql
CREATE TABLE sales.bronze.customers
(
 id INT
);
```

When dropped:

```sql
DROP TABLE customers;
```

Metadata and data are removed.

---

## External Table

Storage exists outside Databricks.

```sql
CREATE TABLE sales.bronze.customers
USING DELTA
LOCATION 'abfss://container@storageaccount.dfs.core.windows.net/customers';
```

When dropped:

```sql
DROP TABLE customers;
```

Only metadata is removed.

Data remains in storage.

---

# Storage Credentials

Storage Credential defines how Databricks authenticates to cloud storage.

Examples:

- Azure Managed Identity
- AWS IAM Role
- GCP Service Account

Example:

```sql
CREATE STORAGE CREDENTIAL my_credential
WITH AZURE_MANAGED_IDENTITY;
```

---

# External Locations

External Location maps cloud storage into Unity Catalog.

Example:

```sql
CREATE EXTERNAL LOCATION raw_data
URL 'abfss://raw@storageaccount.dfs.core.windows.net/'
WITH STORAGE CREDENTIAL my_credential;
```

Purpose:

- Secure cloud storage access
- Reusable storage definition

---

# Access Control Model

### Suggested Diagram

```text
User / Group / Service Principal
               |
               v
          Privileges
               |
               v
         Catalog Object
```

---

# Principals

Permissions can be assigned to:

## User

```text
sonam@company.com
```

---

## Group

```text
data-engineers
analysts
admins
```

Recommended for production environments.

---

## Service Principal

Used by:

- Applications
- ETL Pipelines
- CI/CD
- Automation Scripts

Example:

```text
data-pipeline-sp
```

---

# Common Privileges

| Privilege | Purpose |
|------------|----------|
| SELECT | Read Data |
| MODIFY | Insert/Update/Delete |
| CREATE | Create Objects |
| USAGE | Access Object |
| ALL PRIVILEGES | Full Access |

---

# Granting Permissions

## Grant Catalog Access

```sql
GRANT USE CATALOG
ON CATALOG sales
TO `data-engineers`;
```

---

## Grant Schema Access

```sql
GRANT USE SCHEMA
ON SCHEMA sales.bronze
TO `data-engineers`;
```

---

## Grant Table Access

```sql
GRANT SELECT
ON TABLE sales.bronze.customers
TO `data-engineers`;
```

---

# Permission Hierarchy

To query a table, a user needs:

```text
USE CATALOG
      +
USE SCHEMA
      +
SELECT TABLE
```

Example:

```sql
GRANT USE CATALOG ON CATALOG sales TO analysts;

GRANT USE SCHEMA ON SCHEMA sales.gold TO analysts;

GRANT SELECT ON TABLE sales.gold.customer_summary TO analysts;
```

---

# Row-Level Security

Restricts rows visible to users.

Example:

```text
India Team -> India Data

US Team -> US Data
```

Concept:

```sql
ROW FILTER
```

Use Cases:

- Regional Data Access
- Compliance Requirements

---

# Column-Level Security

Restricts sensitive columns.

Example:

```text
Visible:
---------
Name
Department

Hidden:
---------
Salary
SSN
PAN
```

Concept:

```sql
COLUMN MASK
```

---

# Data Lineage

Tracks data movement and transformations.

### Suggested Diagram

```text
bronze.customers
        |
        v
silver.customers
        |
        v
gold.customer_summary
```

Benefits:

- Impact Analysis
- Troubleshooting
- Compliance
- Auditability

---

# Data Discovery

Allows users to search assets by:

- Table Name
- Column Name
- Tags
- Owner
- Description

Example:

```sql
COMMENT ON TABLE sales.gold.customer_summary
IS 'Customer KPI reporting table';
```

---

# Audit Logs

Unity Catalog records:

- Who accessed data
- What query was run
- When it was accessed
- Which object was accessed

Benefits:

- Security Monitoring
- Compliance
- Governance

---

# Unity Catalog and Delta Lake

Many beginners confuse these concepts.

## Delta Lake

Responsible for:

- Data Storage
- ACID Transactions
- Time Travel
- Schema Enforcement

## Unity Catalog

Responsible for:

- Governance
- Security
- Permissions
- Lineage
- Auditing

Mental Model:

```text
Delta Lake
    =
Storage Layer

Unity Catalog
    =
Governance Layer
```

---

# Unity Catalog vs Hive Metastore

| Feature | Hive Metastore | Unity Catalog |
|----------|---------------|---------------|
| Governance | Limited | Centralized |
| Lineage | No | Yes |
| Auditing | Limited | Yes |
| Multi Workspace Support | No | Yes |
| Fine-Grained Security | Limited | Yes |
| Volumes | No | Yes |
| Data Sharing | Limited | Yes |

---

# Common Commands

## Show Catalogs

```sql
SHOW CATALOGS;
```

---

## Create Catalog

```sql
CREATE CATALOG sales;
```

---

## Show Schemas

```sql
SHOW SCHEMAS IN sales;
```

---

## Create Schema

```sql
CREATE SCHEMA sales.bronze;
```

---

## Show Tables

```sql
SHOW TABLES IN sales.bronze;
```

---

## Use Catalog

```sql
USE CATALOG sales;
```

---

## Use Schema

```sql
USE SCHEMA bronze;
```

---

## Describe Table

```sql
DESCRIBE TABLE sales.bronze.customers;
```

---

# Real-World Enterprise Setup

```text
Metastore
    |
    +-- Sales Catalog
    |      |
    |      +-- Bronze
    |      +-- Silver
    |      +-- Gold
    |
    +-- Finance Catalog
    |
    +-- Marketing Catalog
    |
    +-- HR Catalog
```

Typical Permissions:

```text
Data Engineers
    -> Full Access

Data Scientists
    -> Silver + Gold

Analysts
    -> Gold Only

Executives
    -> Reporting Tables Only
```

---

# Unity Catalog Interview Questions

### What is Unity Catalog?

A centralized governance solution in Databricks that manages metadata, security, permissions, auditing, lineage, and data discovery across Lakehouse assets.

---

### What is the Unity Catalog hierarchy?

```text
Metastore
   |
Catalog
   |
Schema
   |
Table/View/Function/Volume
```

---

### Difference between Catalog and Schema?

Catalog:

- Top-level container
- Usually business domain

Schema:

- Inside catalog
- Organizes related objects

---

### Difference between Managed and External Tables?

Managed Table:

- Databricks manages storage and metadata

External Table:

- Databricks manages metadata only

---

### Why is Unity Catalog important?

Because it provides:

- Centralized Governance
- Security
- Lineage
- Auditing
- Data Discovery
- Cross-Workspace Management

---

# Key Takeaways

- Unity Catalog is the governance layer of Databricks.
- It sits above Delta Lake.
- It controls access to data assets.
- It provides lineage, auditing, and discovery.
- It supports catalogs, schemas, tables, views, functions, and volumes.
- It enables centralized governance across multiple workspaces.
- Modern Databricks deployments should use Unity Catalog instead of Hive Metastore.