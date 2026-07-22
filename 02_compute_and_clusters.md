# Compute & Clusters

## Introduction

In Databricks, **Compute** refers to the infrastructure used to execute workloads such as notebooks, ETL pipelines, SQL queries, machine learning training, and streaming jobs.

Behind the scenes, Databricks provisions and manages cloud virtual machines (VMs) that run Apache Spark.

A compute resource typically consists of:

```text
Cluster
│
├── Driver Node
│
└── Worker Nodes (Executors)
```

The driver coordinates the execution of Spark applications, while workers perform the actual distributed computations.

---

# Cluster Types

Databricks provides different types of compute depending on the workload.

## 1. All-Purpose Compute

Interactive compute designed for development and collaboration.

### Common Use Cases

- Notebook development
- Data exploration
- Testing Spark code
- Ad-hoc analysis
- Team collaboration

### Characteristics

- Can be shared among multiple users
- Runs until manually terminated
- Supports interactive workloads
- Higher cost if left running

### Business Perspective

Ideal for development teams but should not be used for long-running production pipelines because idle clusters continue to incur costs.

---

## 2. Job Compute

Dedicated compute created specifically for scheduled jobs.

### Common Use Cases

- ETL pipelines
- Production workflows
- Batch processing
- Scheduled data ingestion

### Characteristics

- Starts automatically when a job runs
- Terminates automatically after completion
- Cost-efficient
- Dedicated to a specific workload

### Business Perspective

Recommended for production workloads because organizations only pay when jobs are running.

---

## 3. SQL Warehouse

Compute optimized for SQL workloads.

### Common Use Cases

- BI reporting
- Dashboarding
- Analytics
- Business user queries

### Integration Examples

- Power BI
- Tableau
- Looker
- Databricks SQL Dashboards

---

## 4. Serverless Compute

Databricks manages infrastructure automatically.

### Advantages

- No cluster management
- Faster startup times
- Automatic scaling
- Simplified operations

### Business Perspective

Reduces operational overhead and improves developer productivity.

---

# Cluster Architecture

A Databricks cluster consists of:

```text
Cluster
│
├── Driver Node
│     ├── Spark Driver
│     ├── DAG Scheduler
│     ├── Task Scheduler
│     └── Cluster Coordination
│
└── Worker Nodes
      ├── Executor
      ├── Executor
      ├── Executor
      └── Executor
```

---

## Driver Node

Responsible for:

- Running application code
- Creating execution plans
- Managing Spark jobs
- Scheduling tasks

Only one driver exists per cluster.

---

## Worker Nodes

Responsible for:

- Processing data
- Executing Spark tasks
- Reading and writing data
- Storing cached datasets

A cluster may contain one or many workers.

---

# Cluster Configuration

Cluster configuration determines performance, scalability, and cost.

---

## Runtime Version

Databricks Runtime (DBR) includes:

- Apache Spark
- Delta Lake
- Optimized libraries
- Performance enhancements

Example:

```text
13.3 LTS
14.3 LTS
15.x Runtime
```

---

## Worker Type

Defines VM specifications.

Examples:

```text
Standard_DS3_v2
Standard_DS5_v2
E8ds_v5
```

Impacts:

- CPU
- Memory
- Network throughput

---

## Number of Workers

Determines cluster size.

Example:

```text
Driver = 1
Workers = 4
```

More workers generally increase parallelism.

---

## Autoscaling

Allows clusters to scale automatically.

Example:

```text
Minimum Workers = 2
Maximum Workers = 10
```

Benefits:

- Cost optimization
- Better resource utilization

---

## Auto Termination

Automatically shuts down idle clusters.

Example:

```text
Terminate after 30 minutes of inactivity
```

Business benefit:

Prevents unnecessary cloud costs.

---

# All-Purpose Cluster vs Job Cluster

| Feature | All-Purpose | Job Cluster |
|----------|------------|------------|
| Interactive | Yes | No |
| Shared Users | Yes | No |
| Auto Start | No | Yes |
| Auto Terminate | Optional | Yes |
| Development | Excellent | Limited |
| Production | Not Recommended | Recommended |
| Cost Efficiency | Lower | Higher |

---

## Interview Question

### Which cluster should be used for production ETL jobs?

Job Clusters because they start automatically when required and terminate after completion, reducing operational costs.

---

# Cluster Policies

Cluster Policies provide governance and control over cluster creation.

Organizations use policies to enforce standards and prevent excessive spending.

---

## Examples

Restrict:

- Instance types
- Runtime versions
- Maximum workers
- Auto termination settings

Example Policy:

```text
Max Workers = 10
Runtime = 13.3 LTS
Auto Termination = Mandatory
```

---

## Business Benefits

### Cost Control

Prevents oversized clusters.

### Governance

Ensures approved configurations are used.

### Security

Restricts unauthorized settings.

---

## Interview Question

### Why are Cluster Policies important?

They enforce governance, standardization, security, and cost optimization across the organization.

---

# Pools

Cluster startup can take several minutes because cloud VMs need to be provisioned.

Databricks Pools solve this problem.

---

## What is a Pool?

A pool is a set of pre-created virtual machines kept ready for cluster allocation.

Without Pool:

```text
Create Cluster
      ↓
Provision VM
      ↓
Start Cluster
      ↓
3-5 Minutes
```

With Pool:

```text
Create Cluster
      ↓
Use Existing VM
      ↓
Cluster Ready
      ↓
Seconds
```

---

## Benefits

### Faster Cluster Startup

Reduces waiting time.

### Better Resource Utilization

Reuses virtual machines.

### Cost Optimization

Reduces provisioning overhead.

---

## Business Perspective

Large organizations running hundreds of jobs daily often use pools to improve operational efficiency.

---

## Interview Question

### Does a Pool execute Spark workloads?

No.

A Pool only stores pre-provisioned virtual machines.

Spark workloads execute on clusters that use those pooled resources.

---

# Photon Engine

Photon is Databricks' next-generation query engine.

It is designed to accelerate Spark SQL and DataFrame workloads.

---

## Why Photon Was Built

Traditional Spark execution is JVM-based.

Although Spark is fast, analytical workloads often require additional optimizations.

Photon was developed to:

- Improve query performance
- Reduce execution time
- Lower infrastructure costs

---

## Key Features

### Native Execution Engine

Implemented in C++ for better performance.

### Vectorized Processing

Processes multiple rows simultaneously.

### Optimized CPU Utilization

Improves hardware efficiency.

### Delta Lake Optimization

Works seamlessly with Delta tables.

---

## Workloads That Benefit Most

- SQL queries
- DataFrame transformations
- Aggregations
- Joins
- ETL pipelines
- BI workloads

---

## Business Benefits

### Faster Processing

Queries complete more quickly.

### Lower Cost

Jobs finish faster, reducing compute usage.

### Better User Experience

Improved dashboard and reporting performance.

---

## Interview Question

### Does Photon replace Apache Spark?

No.

Photon is an optimized execution engine that runs underneath Spark and accelerates Spark SQL and DataFrame workloads.

---

# Real-World Architecture

```text
Developer
    │
    ▼
Notebook / Workflow
    │
    ▼
Cluster
    │
    ├── Driver
    │
    └── Workers
           │
           ▼
      Photon Engine
           │
           ▼
        Delta Lake
           │
           ▼
      ADLS / S3 / GCS
```

---

# Key Takeaways

- Compute is the processing layer of Databricks.
- Clusters consist of Driver and Worker nodes.
- All-Purpose Clusters are designed for development and collaboration.
- Job Clusters are recommended for production workloads.
- Cluster Policies enforce governance and cost control.
- Pools reduce cluster startup time.
- Photon accelerates Spark SQL and DataFrame workloads.
- Proper compute selection directly impacts performance, scalability, and cost.