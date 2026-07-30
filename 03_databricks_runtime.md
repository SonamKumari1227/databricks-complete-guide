# Databricks Runtime (DBR)

## What is Databricks Runtime?

Databricks Runtime (DBR) is the execution environment used by Databricks clusters.

It is a combination of:

- Apache Spark
- Optimized Spark Engine
- Scala
- Python
- Java Libraries
- Delta Lake
- Databricks-specific optimizations

Think of it as:

Application → Runs on Spark → Spark runs inside Databricks Runtime → Runtime runs on Cluster

---

# Why Do We Need Databricks Runtime?

Without DBR:

- You need to install Spark manually
- Configure dependencies
- Manage compatibility issues

With DBR:

- Pre-configured environment
- Better performance
- Built-in Delta Lake support
- Security updates handled by Databricks

---

# Mental Model

```text
Databricks Workspace
        │
        ▼
     Cluster
        │
        ▼
Databricks Runtime
        │
 ┌──────┼──────┐
 ▼      ▼      ▼
Spark  Delta  Libraries
```

---

# Databricks Runtime Version Format

Example:

```text
13.3 LTS
14.3 LTS
15.4 LTS
16.4 LTS
```

Where:

```text
13  → Major Version
.3  → Maintenance Release
LTS → Long Term Support
```

Example:

```text
13.3 LTS
```

means:

- Runtime Version = 13
- Maintenance Release = 3
- Long-Term Supported Version

---

# How to Check Runtime Version?

Cluster → Configuration

Example:

```text
Databricks Runtime 13.3 LTS
Apache Spark 3.4.1
Scala 2.12
```

Always check:

- DBR Version
- Spark Version
- Scala Version

---

# Databricks Runtime Components

## 1. Apache Spark

Provides:

- Data Processing
- SQL Engine
- Streaming
- Machine Learning

Example:

```python
df = spark.read.csv("file.csv")
```

---

## 2. Delta Lake

Provides:

- ACID Transactions
- Time Travel
- Schema Enforcement
- Schema Evolution

Example:

```python
df.write.format("delta").save(path)
```

---

## 3. Photon Engine

Photon is Databricks' native query engine.

Benefits:

- Faster SQL
- Faster Aggregations
- Faster Joins

Can improve workloads significantly without code changes.

---

## 4. Built-in Libraries

Examples:

- pandas
- numpy
- pyarrow
- delta-spark

No need to install many common packages manually.

---

# Types of Databricks Runtime

## 1. Standard Runtime

Used for:

- Data Engineering
- ETL Pipelines
- General Spark Workloads

Most commonly used runtime.

---

## 2. ML Runtime

Includes:

- TensorFlow
- PyTorch
- Scikit-Learn
- MLflow

Used by Data Scientists.

---

## 3. Photon Runtime

Runtime with Photon enabled.

Best for:

- SQL
- ETL
- Analytics

---

# LTS Runtime

LTS = Long Term Support

Example:

```text
13.3 LTS
15.4 LTS
```

Benefits:

- More stable
- Better compatibility
- Recommended for Production

Interview Tip:

> Production clusters usually use LTS versions.

---

# Runtime Compatibility

Always verify compatibility between:

```text
Databricks Runtime
        +
Spark Version
        +
Scala Version
        +
External Libraries
```

Example:

```text
Mosaic 0.4.2
       ↓
Requires DBR 13.x
```

Wrong runtime version can cause:

```text
ClassNotFoundException
InvalidClassException
Library Conflicts
```

---

# Why Runtime Selection Matters?

Choosing the wrong runtime may cause:

- Job Failures
- Library Issues
- Package Incompatibility
- Performance Problems

Example:

```text
Sedona 1.6.1
Scala 2.12

Works on:
DBR 13.x

May fail on:
DBR 17.x (Scala 2.13)
```

---

# Runtime Upgrade

Reasons to Upgrade:

- New Spark Features
- Security Fixes
- Better Performance
- Bug Fixes

Before upgrading:

✔ Test in lower environment

✔ Validate libraries

✔ Run sample workloads

---

# Interview Questions

### What is Databricks Runtime?

A pre-configured execution environment containing Spark, Delta Lake, libraries, and Databricks optimizations.

---

### Why is DBR important?

It provides compatibility, performance improvements, security updates, and managed Spark infrastructure.

---

### What is LTS Runtime?

Long-Term Support runtime recommended for production because it receives stability and maintenance updates for an extended period.

---

### What is Photon?

A high-performance native query engine developed by Databricks that accelerates SQL and DataFrame workloads.

---

### What should you check before installing a library?

- DBR Version
- Spark Version
- Scala Version
- Library Compatibility

---

# Quick Revision

```text
DBR = Spark + Delta + Libraries + Optimizations

Main Components:
✔ Spark
✔ Delta Lake
✔ Photon
✔ Built-in Libraries

Runtime Types:
✔ Standard
✔ ML
✔ Photon

Production:
✔ Prefer LTS

Always Check:
✔ DBR Version
✔ Spark Version
✔ Scala Version
✔ Library Compatibility
```