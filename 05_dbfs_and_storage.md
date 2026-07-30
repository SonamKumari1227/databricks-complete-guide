# DBFS and Storage

## What is DBFS?

DBFS (Databricks File System) is a distributed file system provided by Databricks that allows users to access files stored in cloud storage through a unified path.

Think of it as:

```text
DBFS = Abstraction Layer over Cloud Storage
```

---

# Why Do We Need DBFS?

Without DBFS:

```text
Notebook
   ↓
Directly Access Azure Blob / ADLS / S3
```

With DBFS:

```text
Notebook
   ↓
DBFS
   ↓
Cloud Storage
```

Benefits:

- Simplified file access
- Consistent path structure
- Easier development
- Supports Spark and Python operations

---

# DBFS Architecture

```text
Databricks Notebook
        │
        ▼
       DBFS
        │
 ┌──────┼──────┐
 ▼      ▼      ▼
ADLS    S3    GCS
```

---

# Common DBFS Paths

### DBFS Path

```text
dbfs:/FileStore/
```

### Driver Path

```text
/fileStore/
```

### Root Path

```text
dbfs:/
```

---

# Accessing Files

### Read CSV

```python
df = spark.read.csv(
    "dbfs:/FileStore/data.csv"
)
```

---

### Write Data

```python
df.write.mode("overwrite").csv(
    "dbfs:/output/"
)
```

---

# DBFS Commands

### List Files

```python
dbutils.fs.ls("dbfs:/")
```

---

### Create Directory

```python
dbutils.fs.mkdirs(
    "dbfs:/demo"
)
```

---

### Copy File

```python
dbutils.fs.cp(
    source,
    destination
)
```

---

### Remove File

```python
dbutils.fs.rm(
    path,
    True
)
```

---

# Mount Points

A mount point connects external cloud storage to DBFS.

Example:

```text
/mnt/raw-data
```

Architecture:

```text
ADLS / S3
    │
    ▼
Mounted to DBFS
    │
    ▼
/mnt/raw-data
```

Example:

```python
spark.read.parquet(
    "/mnt/raw-data/customers"
)
```

---

# Why Mount Storage?

Benefits:

- Shorter paths
- Easier access
- Shared across notebooks

---

# Modern Recommendation

Earlier:

```text
Mount Points
```

Now Preferred:

```text
Unity Catalog Volumes
External Locations
```

Databricks recommends avoiding new mount points for modern architectures.

---

# DBFS Root

Default storage location managed by Databricks.

Example:

```text
dbfs:/
```

Can store:

- Temporary files
- Logs
- Uploaded files

Not recommended for critical production data.

---

# DBFS vs Cloud Storage

| DBFS | Cloud Storage |
|--------|--------|
| Databricks abstraction | Actual storage |
| Easier access | Source of truth |
| Managed by Databricks | Managed by cloud provider |
| Uses dbfs:/ paths | Uses ADLS/S3/GCS paths |

---

# DBFS vs Local Driver Storage

| DBFS | Driver Storage |
|--------|--------|
| Distributed | Local to cluster node |
| Persistent | Temporary |
| Shared | Not shared |
| Production friendly | Not production friendly |

---

# Storage Options in Databricks

## 1. DBFS

```text
dbfs:/data
```

Used for:

- Development
- Testing
- Temporary storage

---

## 2. ADLS Gen2 (Azure)

```text
abfss://container@storage.dfs.core.windows.net/
```

Most common in Azure Databricks.

---

## 3. Amazon S3

```text
s3://bucket-name/
```

Used in AWS Databricks.

---

## 4. Google Cloud Storage

```text
gs://bucket-name/
```

Used in GCP Databricks.

---

# Unity Catalog Storage

Modern Databricks storage architecture:

```text
Unity Catalog
      │
      ▼
External Location
      │
      ▼
Cloud Storage
```

Benefits:

- Better security
- Central governance
- Fine-grained access control

---

# Best Practices

### Store Production Data in Cloud Storage

Good:

```text
ADLS
S3
GCS
```

Avoid:

```text
DBFS Root
```

for important datasets.

---

### Use Unity Catalog

Prefer:

```text
Unity Catalog Volumes
External Locations
```

over mount points.

---

### Keep DBFS for Temporary Files

Examples:

- Testing files
- Logs
- Intermediate outputs

---

# Interview Questions

### What is DBFS?

DBFS (Databricks File System) is a distributed abstraction layer that provides easy access to underlying cloud storage from Databricks.

---

### Is DBFS actual storage?

No.

DBFS is an abstraction layer; the actual data resides in cloud storage such as ADLS, S3, or GCS.

---

### What are Mount Points?

Mount points connect external cloud storage to DBFS paths like:

```text
/mnt/raw-data
```

---

### Are Mount Points still recommended?

Not for new implementations.

Databricks recommends using:

- Unity Catalog
- External Locations
- Volumes

---

### Why avoid DBFS Root for production data?

Because governance, security, and data management are limited compared to cloud storage integrated with Unity Catalog.

---

# Mental Model

```text
Notebook
   │
   ▼
 DBFS
   │
   ▼
Cloud Storage
(ADLS / S3 / GCS)
```

---

# Quick Revision

```text
DBFS = Databricks File System

Key Commands:
✔ dbutils.fs.ls()
✔ dbutils.fs.cp()
✔ dbutils.fs.rm()
✔ dbutils.fs.mkdirs()

Storage Types:
✔ DBFS
✔ ADLS Gen2
✔ S3
✔ GCS

Modern Architecture:
✔ Unity Catalog
✔ External Locations
✔ Volumes

Avoid:
✘ Storing critical production data in DBFS Root
✘ Creating new mount points unless required
```