# What is Databricks?

## Introduction

Databricks is a unified Data and AI platform that allows organizations to perform Data Engineering, Analytics, Data Science, Machine Learning, Streaming, and Governance from a single environment.

It was founded by the creators of Apache Spark to simplify large-scale data processing and eliminate the complexity of managing multiple disconnected big data tools.

Today, Databricks runs on major cloud providers such as Azure, AWS, and Google Cloud.

---

# The Journey to Databricks

To understand Databricks properly, we first need to understand the evolution of Big Data technologies.

---

# Era 1: Hadoop and MapReduce

Around the early 2000s, organizations started generating massive amounts of data.

Traditional databases were not designed to handle:

- Terabytes of data
- Petabytes of data
- Distributed processing

To solve this problem, Hadoop became the most popular Big Data framework.

A Hadoop ecosystem mainly consisted of:

## HDFS (Storage Layer)

HDFS stands for Hadoop Distributed File System.

Its responsibility was to store large datasets across multiple machines.

Instead of storing data on a single server, HDFS distributed data blocks across many servers.

### Example

Imagine a 1 TB file.

Instead of storing it on one machine:

```text
Server 1 → 256 GB
Server 2 → 256 GB
Server 3 → 256 GB
Server 4 → 256 GB
```

This made storage:

- Distributed
- Fault tolerant
- Scalable

---

## MapReduce (Processing Layer)

MapReduce was Hadoop's processing engine.

Its job was to process data stored in HDFS.

A MapReduce job consisted of:

### Map Phase

Read data and perform transformations.

### Reduce Phase

Aggregate and combine results.

---

### Example

Suppose we want to count how many times each word appears in a file.

Input:

```text
Hello Spark
Hello Hadoop
```

Map Phase:

```text
(Hello,1)
(Spark,1)
(Hello,1)
(Hadoop,1)
```

Reduce Phase:

```text
Hello = 2
Spark = 1
Hadoop = 1
```

---

# Problems with MapReduce

Although Hadoop was revolutionary, it had several limitations.

---

## Problem 1: Excessive Disk I/O

MapReduce continuously read and wrote data to disk.

Workflow:

```text
Read from HDFS
↓
Process
↓
Write to Disk
↓
Read Again
↓
Process Again
↓
Write Again
```

Even intermediate results were stored on disk.

This caused:

- High latency
- Slow execution
- Large disk overhead

---

## Problem 2: Slow Iterative Processing

Machine Learning workloads often require multiple iterations.

For example:

```text
Iteration 1
Iteration 2
Iteration 3
Iteration 4
```

MapReduce wrote results to disk after every iteration.

This made ML workloads extremely slow.

---

## Problem 3: Complex Development

Writing MapReduce programs required a lot of Java code.

Even simple transformations often required hundreds of lines of code.

---

# Era 2: Apache Spark

To solve the limitations of MapReduce, Apache Spark was developed.

Spark introduced a revolutionary concept:

## In-Memory Computation

Instead of repeatedly writing intermediate results to disk, Spark stores data in memory whenever possible.

MapReduce:

```text
Read Disk
Process
Write Disk
Read Disk
Process
Write Disk
```

Spark:

```text
Read Once
Keep in Memory
Process Multiple Times
Write Final Result
```

---

# Why Spark Became Popular

Spark provided:

- Faster execution
- In-memory processing
- Better developer experience
- Support for multiple languages
- Batch processing
- Streaming
- Machine Learning
- Graph Processing

In many workloads, Spark was significantly faster than MapReduce.

---

# New Problem: Too Many Independent Components

Although Spark solved the processing problem, organizations still faced another challenge.

A modern data platform required multiple tools.

### Storage

- HDFS
- S3
- ADLS

### Processing

- Spark

### Scheduling

- Airflow

### Governance

- Ranger
- Atlas

### Machine Learning

- TensorFlow
- Scikit-learn

### Analytics

- Hive
- Tableau
- Power BI

---

A typical architecture looked like:

```text
Storage
   ↓
Spark
   ↓
Airflow
   ↓
Hive
   ↓
ML Platform
   ↓
BI Tools
```

Each tool had:

- Separate configuration
- Separate security
- Separate monitoring
- Separate maintenance

Managing the platform became difficult and expensive.

---

# The Need for Databricks

Organizations wanted:

- One platform for all data workloads
- Easy Spark management
- Built-in governance
- Built-in security
- Integrated machine learning
- Integrated analytics
- Collaboration between teams
- Cloud-native scalability

In short:

> Companies wanted the power of Spark without the operational complexity of managing an entire Big Data ecosystem.

---

# Birth of Databricks

The creators of Apache Spark recognized this challenge.

They built Databricks as a unified platform around Spark.

The idea was simple:

> Instead of stitching together multiple tools, provide everything in a single platform.

---

# What Databricks Provides

Databricks combines multiple services under one roof.

## Storage Layer

- Delta Lake
- Cloud Storage Integration
- Data Lakehouse

---

## Processing Layer

- Apache Spark
- Photon Engine

---

## Data Engineering

- ETL Pipelines
- Batch Processing
- Streaming Pipelines

---

## Analytics

- Databricks SQL
- Dashboards
- Reporting

---

## Machine Learning

- MLflow
- Feature Engineering
- Model Tracking

---

## Governance

- Unity Catalog
- Lineage
- Access Control
- Data Discovery

---

## Orchestration

- Workflows
- Job Scheduling
- Dependency Management

---

# Traditional Architecture vs Databricks

## Traditional Architecture

```text
Storage → Spark → Airflow → Hive → ML Tool → BI Tool
```

Multiple tools.

Multiple teams.

Multiple integrations.

---

## Databricks Architecture

```text
                Databricks

   Storage
       │
   Processing
       │
   Analytics
       │
 Machine Learning
       │
   Governance
       │
   Workflows
```

Everything managed from one platform.

---

# What Makes Databricks Different?

Databricks provides:

- Managed Spark
- Unified Analytics
- Data Lakehouse Architecture
- Integrated Governance
- Auto Scaling Compute
- Collaboration Through Notebooks
- Built-in Machine Learning
- Enterprise Security

---

# Key Takeaway

The evolution can be summarized as:

```text
Hadoop
   ↓
MapReduce Processing
   ↓
Disk-Based Computation Problems
   ↓
Apache Spark
   ↓
In-Memory Processing
   ↓
Multiple Independent Tools
   ↓
Need for Unified Platform
   ↓
Databricks
```

Databricks was created by the founders of Apache Spark to provide a single platform where data engineering, analytics, machine learning, governance, and orchestration can work together seamlessly.