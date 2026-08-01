# Delta Lake

## What is Delta Lake?

Delta Lake is an open-source storage layer built on top of Data Lakes that brings:

- ACID Transactions
- Time Travel
- Schema Enforcement
- Schema Evolution
- Data Reliability

Think of it as:

```text
Data Lake + Reliability = Delta Lake
```

---

# Why Delta Lake?

Traditional Data Lakes have problems:

```text
❌ No ACID Transactions
❌ Data Corruption Risk
❌ No Version History
❌ Difficult Updates & Deletes
❌ Poor Data Quality Control
```

Delta Lake solves these issues.

---

# Delta Lake Architecture

```text
Data
 │
 ▼
Parquet Files
 │
 ▼
Delta Transaction Log (_delta_log)
 │
 ▼
Delta Table
```

The secret sauce is:

```text
_delta_log
```

which tracks every change made to the table.

---

# What is a Delta Table?

A Delta Table is simply:

```text
Parquet Files
      +
Transaction Log
      =
Delta Table
```

Example:

```python
df.write.format("delta").save(path)
```

---

# Delta Log

Every Delta table contains:

```text
_delta_log/
```

Stores:

- Inserts
- Updates
- Deletes
- Schema Changes
- Transaction History

Example:

```text
_delta_log
├── 000000.json
├── 000001.json
├── 000002.json
```

---

# ACID Transactions

ACID means:

### A - Atomicity

All operations succeed or fail together.

### C - Consistency

Data remains valid after transactions.

### I - Isolation

Multiple users can work simultaneously.

### D - Durability

Committed data is not lost.

---

# Time Travel

Allows viewing historical versions of data.

Example:

```sql
SELECT *
FROM sales VERSION AS OF 5
```

Or:

```sql
SELECT *
FROM sales TIMESTAMP AS OF '2025-01-01'
```

Use Cases:

- Data Recovery
- Auditing
- Debugging

---

# Schema Enforcement

Prevents incorrect data from being written.

Example:

Table Schema:

```text
id INT
name STRING
```

Trying to insert:

```text
id STRING
```

Result:

```text
Error
```

This helps maintain data quality.

---

# Schema Evolution

Allows schema changes when required.

Example:

Old Schema:

```text
id
name
```

New Schema:

```text
id
name
email
```

Enable:

```python
.option("mergeSchema", "true")
```

---

# Upserts using MERGE

One of Delta Lake's most important features.

Example:

```sql
MERGE INTO target t
USING source s
ON t.id = s.id

WHEN MATCHED THEN
UPDATE SET *

WHEN NOT MATCHED THEN
INSERT *
```

Use Cases:

- CDC
- SCD Type 1
- Incremental Loads

---

# Update Records

```sql
UPDATE customers
SET status = 'ACTIVE'
WHERE id = 1
```

---

# Delete Records

```sql
DELETE FROM customers
WHERE id = 1
```

Traditional Parquet does not support this efficiently.

---

# Delta Lake Features

## Data Versioning

Every transaction creates a new version.

```text
Version 0
Version 1
Version 2
Version 3
```

---

## Auditability

See change history:

```sql
DESCRIBE HISTORY sales
```

Useful for troubleshooting.

---

## Reliability

Prevents:

- Partial Writes
- Data Corruption
- Concurrent Write Issues

---

# OPTIMIZE

Improves query performance.

Problem:

```text
10000 Small Files
```

Solution:

```sql
OPTIMIZE sales
```

Result:

```text
Fewer Larger Files
```

---

# Z-Ordering

Improves data skipping.

Example:

```sql
OPTIMIZE sales
ZORDER BY (customer_id)
```

Benefits:

- Faster Reads
- Faster Filtering

---

# VACUUM

Removes old files.

Example:

```sql
VACUUM sales
```

Benefits:

- Save Storage
- Clean Old Data

---

# Delta Lake vs Parquet

| Feature | Parquet | Delta Lake |
|----------|----------|----------|
| ACID Transactions | ❌ | ✅ |
| Time Travel | ❌ | ✅ |
| Updates | ❌ | ✅ |
| Deletes | ❌ | ✅ |
| MERGE | ❌ | ✅ |
| Schema Enforcement | ❌ | ✅ |
| Audit History | ❌ | ✅ |

---

# Delta Lake in Medallion Architecture

```text
Bronze
   ↓
Silver
   ↓
Gold
```

All layers commonly use:

```text
Delta Tables
```

because of reliability and performance.

---

# Common Interview Questions

### What is Delta Lake?

An open-source storage layer that adds ACID transactions, schema enforcement, time travel, and reliable data management to Data Lakes.

---

### What is the Delta Log?

The `_delta_log` directory stores transaction history and metadata for every Delta table operation.

---

### What is Time Travel?

A Delta Lake feature that allows querying historical versions of data.

---

### Difference between Schema Enforcement and Schema Evolution?

| Schema Enforcement | Schema Evolution |
|------------|------------|
| Prevents invalid schema writes | Allows controlled schema changes |
| Improves data quality | Supports evolving datasets |

---

### Why is MERGE important?

MERGE supports efficient UPSERT operations and is heavily used for CDC and SCD implementations.

---

### What does OPTIMIZE do?

Compacts small files into larger files to improve query performance.

---

### What does VACUUM do?

Removes obsolete files that are no longer required by Delta Lake.

---

# Mental Model

```text
Parquet Files
      +
_delta_log
      =
Delta Lake

Delta Lake
      ↓
ACID
Time Travel
MERGE
UPDATE
DELETE
OPTIMIZE
VACUUM
```

---

# Quick Revision

```text
Delta Lake = Parquet + Transaction Log

Core Features:
✔ ACID Transactions
✔ Time Travel
✔ Schema Enforcement
✔ Schema Evolution
✔ MERGE
✔ UPDATE
✔ DELETE

Performance:
✔ OPTIMIZE
✔ ZORDER

Maintenance:
✔ VACUUM

Most Important Folder:
✔ _delta_log
```