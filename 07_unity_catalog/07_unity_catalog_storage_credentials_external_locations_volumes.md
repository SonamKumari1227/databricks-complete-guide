# Unity Catalog Storage Credentials, External Locations, and Volumes

## Overview

One of the most important concepts in Unity Catalog is understanding how Databricks securely accesses cloud storage.

A common misconception is:

```text
Unity Catalog stores data
```

This is incorrect.

Unity Catalog stores:

- Metadata
- Permissions
- Governance Information

Actual data remains in cloud storage such as:

- Azure Data Lake Storage Gen2 (ADLS)
- Amazon S3
- Google Cloud Storage (GCS)

---

# High-Level Architecture

## Suggested Diagram

```text
Databricks User
       |
       v
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
Delta Tables / Files
```

---

# Why Do We Need Storage Credentials?

Imagine Databricks wants to access data stored in:

```text
abfss://raw@company.dfs.core.windows.net/
```

Questions:

- How will Databricks authenticate?
- Which identity should be used?
- How are permissions managed?

This is where Storage Credentials come in.

---

# Storage Credential

## Definition

A Storage Credential is a Unity Catalog object that contains authentication information required to access cloud storage.

Think of it as:

```text
Secure Login Information
for Cloud Storage
```

---

# Supported Authentication Methods

## Azure

- Managed Identity
- Service Principal

## AWS

- IAM Role

## GCP

- Service Account

---

# Azure Example

```sql
CREATE STORAGE CREDENTIAL uc_storage_credential
WITH AZURE_MANAGED_IDENTITY;
```

---

# What Happens Internally?

```text
Databricks
      |
      v
Managed Identity
      |
      v
ADLS Gen2
```

Databricks uses the Managed Identity to access storage securely.

No usernames or passwords are required.

---

# Why Storage Credentials?

Benefits:

- Centralized Security
- Reusable Authentication
- Better Governance
- No Hardcoded Secrets
- Easier Permission Management

---

# External Location

## Definition

An External Location connects:

```text
Storage Credential
+
Cloud Storage Path
```

into a reusable Unity Catalog object.

---

# Architecture

```text
Storage Credential
         +
Storage Path
         =
External Location
```

---

# Example

Storage Path:

```text
abfss://raw@company.dfs.core.windows.net/
```

Storage Credential:

```text
uc_storage_credential
```

External Location:

```sql
CREATE EXTERNAL LOCATION raw_data
URL 'abfss://raw@company.dfs.core.windows.net/'
WITH STORAGE CREDENTIAL uc_storage_credential;
```

---

# Why External Locations?

Without External Locations:

```text
Every user accesses storage directly.
```

Problems:

- Difficult governance
- Security risks
- No centralized control

With External Locations:

```text
Unity Catalog controls access.
```

---

# Real World Example

Suppose a company has:

```text
ADLS
 |
 +-- raw
 +-- curated
 +-- analytics
```

Create External Locations:

```sql
CREATE EXTERNAL LOCATION raw_data;
```

```sql
CREATE EXTERNAL LOCATION curated_data;
```

```sql
CREATE EXTERNAL LOCATION analytics_data;
```

Now governance can be applied separately.

---

# Granting Access to External Locations

Example:

```sql
GRANT READ FILES
ON EXTERNAL LOCATION raw_data
TO analysts;
```

---

# External Tables

Before Unity Catalog:

```sql
CREATE TABLE customers
USING DELTA
LOCATION 'abfss://raw@storage/...';
```

Storage paths were often referenced directly.

With Unity Catalog:

```text
External Locations manage access.
```

---

# Managed Tables vs External Tables

## Managed Table

Databricks manages:

- Metadata
- Storage

Example:

```sql
CREATE TABLE sales.bronze.customers
(
 id INT
);
```

Storage is automatically managed.

---

## Managed Table Architecture

```text
Unity Catalog
      |
Managed Storage
      |
Delta Table
```

---

## External Table

Data already exists outside Databricks.

Example:

```sql
CREATE TABLE sales.bronze.customers
USING DELTA
LOCATION 'abfss://raw@storage/customers';
```

Databricks manages:

```text
Metadata Only
```

Storage remains external.

---

# Difference Between Managed and External Tables

| Feature | Managed Table | External Table |
|----------|-------------|-------------|
| Metadata Managed | Yes | Yes |
| Storage Managed | Yes | No |
| Data Deleted on DROP | Yes | No |
| Existing Storage Required | No | Yes |

---

# Volumes

## What Are Volumes?

Volumes are Unity Catalog objects used for storing non-tabular files.

Examples:

- CSV Files
- JSON Files
- Excel Files
- PDFs
- Images
- Machine Learning Models
- Log Files

---

# Why Volumes?

Tables work well for structured data.

But what about:

```text
invoice.pdf
image.png
training.csv
model.pkl
```

These are files, not tables.

Volumes solve this problem.

---

# Volume Architecture

```text
Catalog
   |
Schema
   |
Volume
   |
Files
```

---

# Managed Volume

Storage managed by Databricks.

Create:

```sql
CREATE VOLUME sales.bronze.raw_files;
```

---

# Accessing Managed Volume

Path:

```text
/Volumes/sales/bronze/raw_files/
```

Python Example:

```python
dbutils.fs.ls("/Volumes/sales/bronze/raw_files/")
```

---

# External Volume

Uses existing cloud storage.

Example:

```sql
CREATE EXTERNAL VOLUME sales.bronze.raw_files
LOCATION 'abfss://raw@storage/raw_files';
```

---

# Managed Volume vs External Volume

| Feature | Managed Volume | External Volume |
|----------|--------------|--------------|
| Storage Managed by Databricks | Yes | No |
| Existing Storage Required | No | Yes |
| Governance Through UC | Yes | Yes |

---

# Uploading Files to Volumes

Example:

```python
dbutils.fs.cp(
  "file:/tmp/customers.csv",
  "/Volumes/sales/bronze/raw_files/customers.csv"
)
```

---

# Reading Files from Volumes

CSV Example:

```python
df = spark.read.csv(
    "/Volumes/sales/bronze/raw_files/customers.csv",
    header=True
)
```

---

# Writing Files to Volumes

Example:

```python
df.write.mode("overwrite").csv(
    "/Volumes/sales/bronze/raw_files/output"
)
```

---

# Security Flow

## Suggested Diagram

```text
User
  |
  v
Unity Catalog Permission
  |
  v
Volume
  |
  v
Cloud Storage
```

Permissions are checked before storage is accessed.

---

# Common Enterprise Setup

```text
ADLS
 |
 +-- raw
 +-- bronze
 +-- silver
 +-- gold
 +-- ml-models
```

Storage Credentials:

```text
adls_credential
```

External Locations:

```text
raw_location
bronze_location
silver_location
gold_location
```

Volumes:

```text
raw_files
documents
ml_models
```

---

# Interview Questions

## What is a Storage Credential?

A Unity Catalog object that stores authentication information used to access cloud storage.

---

## What is an External Location?

A reusable Unity Catalog object that combines:

```text
Storage Credential
+
Storage Path
```

to securely access cloud storage.

---

## Why do we need External Locations?

They provide centralized governance and secure access to cloud storage.

---

## What is a Volume?

A Unity Catalog object used for storing and governing non-tabular files.

---

## Examples of Files Stored in Volumes?

- CSV
- JSON
- Images
- PDFs
- ML Models
- Log Files

---

## Difference Between Table and Volume?

Table:

```text
Structured Data
Rows and Columns
```

Volume:

```text
Files and Documents
```

---

## Difference Between Managed and External Volume?

Managed Volume:

```text
Databricks manages storage
```

External Volume:

```text
Storage exists outside Databricks
```

---

# Key Takeaways

- Unity Catalog does not store data; it governs data.
- Storage Credentials provide secure authentication to cloud storage.
- External Locations connect Storage Credentials with storage paths.
- Managed Tables are fully managed by Databricks.
- External Tables use existing storage.
- Volumes are used for non-tabular files.
- Both Managed and External Volumes support Unity Catalog governance.
- Storage Credentials + External Locations are fundamental concepts for enterprise Databricks deployments.