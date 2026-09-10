# Unity Catalog: Data Lineage, Auditing, and Data Discovery

## Overview

Unity Catalog does more than control permissions.

It also helps organizations understand:

- Where data comes from
- How data is transformed
- Where data is being used
- Who accessed the data
- When the data was accessed
- Which users own data assets
- How users can discover trusted datasets

The three important governance capabilities are:

```text
Data Lineage
Data Auditing
Data Discovery
```

---

# 1. Data Lineage

## What is Data Lineage?

Data lineage shows the journey of data from its source to its final destination.

Example:

```text
Raw Data
   |
   v
Bronze Table
   |
   v
Silver Table
   |
   v
Gold Table
   |
   v
Power BI Dashboard
```

It answers questions such as:

```text
Where did this data come from?

What transformations were applied?

Which tables depend on this table?

Which dashboards or downstream assets use this table?
```

---

# Lineage Example

Suppose we have:

```text
sales.bronze.orders
```

A Data Engineer cleans the data:

```text
sales.bronze.orders
        |
        v
sales.silver.orders
```

Then creates an aggregation:

```text
sales.silver.orders
        |
        v
sales.gold.monthly_sales
```

The lineage looks like:

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

# Suggested Diagram

```text
Unity Catalog Data Lineage
```

```text
             Source
               |
               v
       bronze.orders
               |
        Transformation
               |
               v
       silver.orders
               |
        Aggregation
               |
               v
     gold.monthly_sales
               |
               v
       BI Dashboard
```

---

# Why Data Lineage Is Important

## 1. Impact Analysis

Suppose a column is going to be removed:

```text
customer_email
```

Before removing it, we can determine:

```text
Which tables use it?

Which reports use it?

Which downstream pipelines depend on it?
```

---

## 2. Troubleshooting

If a dashboard contains incorrect data:

```text
Dashboard
    |
    v
Gold Table
    |
    v
Silver Table
    |
    v
Bronze Table
```

We can trace the data backward and identify where the problem started.

---

## 3. Compliance

Organizations may need to know:

```text
Where sensitive data comes from

Where sensitive data is stored

Where sensitive data is being used
```

Lineage helps answer these questions.

---

# Column-Level Lineage

Unity Catalog can also provide lineage at the column level for supported workloads.

Example:

```text
bronze.customers.email
            |
            v
silver.customers.email
            |
            v
gold.customer_report.email
```

This is more detailed than table-level lineage.

---

# How Lineage Is Captured

Databricks can automatically capture lineage for supported workloads executed on Databricks.

Example:

```sql
CREATE TABLE sales.gold.monthly_sales AS
SELECT
    month,
    SUM(amount) AS total_sales
FROM sales.silver.orders
GROUP BY month;
```

Unity Catalog can understand the relationship between:

```text
sales.silver.orders
```

and:

```text
sales.gold.monthly_sales
```

---

# 2. Data Auditing

## What Is Data Auditing?

Auditing answers:

```text
Who did something?

What did they do?

When did they do it?

What object did they access?
```

Example:

```text
User:
sonam@company.com

Action:
SELECT

Object:
sales.gold.customers

Time:
10:30 AM
```

---

# Audit Logs

Databricks provides audit logs that can be used to track activities occurring within the platform.

Examples:

```text
Login
Query execution
Table access
Permission changes
Object creation
Object deletion
```

Audit information is useful for:

- Security
- Compliance
- Monitoring
- Investigations
- Governance

---

# Audit Architecture

### Suggested Diagram

```text
Databricks Activity
        |
        v
   Audit Logs
        |
        v
Monitoring / Security
        |
        v
Compliance / Investigation
```

---

# Example Audit Scenario

Suppose a sensitive table exists:

```text
finance.gold.employee_salary
```

Security team wants to know:

```text
Who accessed this table?
```

Audit logs can help identify:

```text
User
Timestamp
Action
Object
Workspace
```

---

# Audit vs Lineage

These concepts are different.

## Lineage

Answers:

```text
Where did the data come from?
Where does the data go?
```

## Audit

Answers:

```text
Who accessed or changed something?
When did it happen?
What action occurred?
```

Mental Model:

```text
Lineage = Data Journey

Audit = User Activity
```

---

# 3. Data Discovery

## What Is Data Discovery?

Data discovery means finding and understanding available data assets.

Imagine an organization has thousands of tables.

An analyst needs:

```text
Customer Data
```

Instead of asking the Data Engineering team:

```text
"Which customer table should I use?"
```

They can search the available data assets through the Databricks data discovery experience.

---

# What Can Users Discover?

Depending on permissions and supported features, users can discover:

- Catalogs
- Schemas
- Tables
- Views
- Columns
- Volumes
- Functions
- Models
- Tags
- Descriptions
- Owners

---

# Table Documentation

Good metadata makes data easier to discover.

Example:

```sql
COMMENT ON TABLE sales.gold.customer_summary
IS 'Daily customer-level sales summary used by the reporting team';
```

Now users can understand the purpose of the table.

---

# Column Documentation

Columns can also have descriptions.

Example:

```sql
COMMENT ON COLUMN sales.gold.customer_summary.customer_id
IS 'Unique identifier assigned to each customer';
```

Good documentation should explain:

```text
What the column means
What the data represents
How it should be used
```

---

# Data Ownership

Data assets should have clear ownership.

Example:

```text
Table:
sales.gold.customer_summary

Owner:
data-engineering
```

Ownership helps answer:

```text
Who is responsible for this table?
```

---

# Tags

Tags can be used to classify and organize data assets.

Examples:

```text
environment = production

domain = sales

classification = confidential

data_owner = data-engineering
```

Tags are useful for:

- Data discovery
- Governance
- Classification
- Automation
- Policy management

---

# Sensitive Data Classification

Organizations may classify data as:

```text
Public
Internal
Confidential
Restricted
```

Example:

```text
customer_name
    -> Internal

salary
    -> Confidential

SSN
    -> Restricted
```

Tags and governance policies can help manage these classifications.

---

# Data Discovery Example

Imagine an analyst searches for:

```text
customer sales
```

They may find:

```text
sales.bronze.customers
sales.silver.customers
sales.gold.customer_summary
marketing.gold.customer_segments
```

The analyst can use metadata such as:

```text
Description
Owner
Columns
Tags
Lineage
```

to determine which dataset is appropriate.

---

# Trusted Data

A major goal of data discovery is helping users identify trusted datasets.

Without governance:

```text
customers_final
customers_final_v2
customers_final_new
customers_final_latest
```

Users may not know which table is correct.

With proper governance:

```text
sales.gold.customer_summary
```

can have:

```text
Owner
Description
Tags
Lineage
Quality Information
```

This makes it easier to identify the recommended dataset.

---

# Data Discovery + Governance

These capabilities work together:

```text
             Unity Catalog
                   |
       -------------------------
       |           |           |
       v           v           v
   Discovery    Lineage     Auditing
       |           |           |
       v           v           v
 Find Data    Understand    Track
              Data Flow     Activity
```

---

# Practical SQL Examples

## Add Table Description

```sql
COMMENT ON TABLE sales.gold.customer_summary
IS 'Customer-level daily sales summary';
```

---

## Add Column Description

```sql
COMMENT ON COLUMN sales.gold.customer_summary.customer_id
IS 'Unique customer identifier';
```

---

## View Table Metadata

```sql
DESCRIBE TABLE EXTENDED sales.gold.customer_summary;
```

---

## View Table Details

```sql
DESCRIBE DETAIL sales.gold.customer_summary;
```

---

# SHOW GRANTS

Although primarily related to security, permissions are also important for data discovery.

```sql
SHOW GRANTS
ON TABLE sales.gold.customer_summary;
```

This helps understand:

```text
Who has access?
What privileges do they have?
```

---

# Unity Catalog Governance Model

### Suggested Diagram

```text
                    Unity Catalog
                         |
       ----------------------------------------
       |                |                    |
       v                v                    v
   Security          Lineage             Discovery
       |                |                    |
       v                v                    v
 Permissions       Data Flow          Find Assets
       |
       v
    Auditing
```

---

# Real-World Example

Suppose a company has:

```text
sales.bronze.orders
sales.silver.orders
sales.gold.monthly_sales
```

A business analyst wants monthly sales data.

They discover:

```text
sales.gold.monthly_sales
```

Metadata:

```text
Description:
Monthly aggregated sales data.

Owner:
Data Engineering

Domain:
Sales

Classification:
Internal
```

Lineage:

```text
bronze.orders
      |
      v
silver.orders
      |
      v
gold.monthly_sales
```

Permissions:

```text
Analysts -> SELECT
Data Engineers -> MODIFY
```

Audit:

```text
Analyst accessed the table at 10:30 AM.
```

This demonstrates how Unity Catalog provides end-to-end governance.

---

# Lineage vs Audit vs Discovery

| Feature | Main Question |
|---------|---------------|
| Lineage | Where did the data come from/go? |
| Audit | Who accessed or changed it? |
| Discovery | Where can I find the data? |
| Metadata | What does the data mean? |
| Permissions | Who can access it? |

---

# Important Interview Questions

## What is Data Lineage?

Data lineage tracks the flow of data between source, transformation, and destination assets.

---

## Why is Data Lineage important?

It helps with:

- Impact analysis
- Troubleshooting
- Compliance
- Understanding data dependencies

---

## What is Data Auditing?

Data auditing tracks activities such as data access, queries, and permission-related operations.

---

## Difference Between Lineage and Audit?

```text
Lineage:
Data Flow

Audit:
User Activity
```

---

## What is Data Discovery?

Data discovery is the process of finding and understanding available data assets using metadata, search, descriptions, tags, ownership, and lineage.

---

## Why is Metadata important?

Metadata tells users:

```text
What is this table?

What does this column mean?

Who owns it?

How should it be used?
```

---

## What are Tags?

Tags are metadata labels used to classify and organize data assets.

Example:

```text
domain = finance
classification = confidential
environment = production
```

---

# Best Practices

## 1. Add Descriptions

Every important table should have a meaningful description.

---

## 2. Document Important Columns

Especially:

```text
Business Keys
Foreign Keys
Sensitive Columns
Calculated Fields
```

---

## 3. Assign Ownership

Every production dataset should have a clear owner.

---

## 4. Use Consistent Tags

Define organization-wide tagging standards.

Example:

```text
domain
environment
classification
owner
```

---

## 5. Use Lineage for Impact Analysis

Before modifying a production table:

```text
Check downstream dependencies.
```

---

## 6. Review Audit Logs

Monitor sensitive data access and security-related activity.

---

# Complete Governance Mental Model

Remember Unity Catalog using this model:

```text
                  UNITY CATALOG
                        |
        ---------------------------------
        |               |               |
        v               v               v
     Security        Discovery       Lineage
        |               |               |
        v               v               v
   Who can access?  Find data       Data flow
        |
        v
     Auditing
        |
        v
   Who did what?
```

---

# Final Mental Model

```text
Delta Lake
    |
    |-- Stores and manages data
    |
    v
Unity Catalog
    |
    |-- Security
    |-- Permissions
    |-- Metadata
    |-- Lineage
    |-- Auditing
    |-- Discovery
    |-- Governance
```

The simplest way to remember it:

```text
Delta Lake = Data Management

Unity Catalog = Data Governance
```

---

# Key Takeaways

- Unity Catalog provides centralized data governance.
- Data Lineage shows how data moves and transforms.
- Auditing tracks user and system activity.
- Data Discovery helps users find useful datasets.
- Metadata explains what data means.
- Tags help classify and organize assets.
- Ownership identifies who is responsible for data assets.
- Lineage is useful for impact analysis and troubleshooting.
- Audit logs are important for security and compliance.
- Discovery, metadata, lineage, permissions, and auditing work together to create a governed Lakehouse.