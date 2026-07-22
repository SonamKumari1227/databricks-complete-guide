# Databricks Fundamentals

## What is Databricks?

Databricks is a cloud-based Data and AI platform built on top of Apache Spark. It provides a unified environment for Data Engineering, Data Analytics, Machine Learning, Streaming, and Data Governance.

Instead of managing multiple tools separately, Databricks brings everything together in a single platform.

### Key Features

- Managed Apache Spark
- Delta Lake
- Unity Catalog
- Workflows & Job Scheduling
- Databricks SQL
- Machine Learning Support
- Structured Streaming
- Auto Loader
- Delta Live Tables (DLT)

---

## Databricks Architecture

At a high level, Databricks separates management responsibilities from data processing responsibilities.

```text
+---------------------------------------------------+
|                   Control Plane                   |
|---------------------------------------------------|
| Workspace UI                                      |
| Notebook Management                               |
| Cluster Management                                |
| Job Scheduling                                    |
| Security & Governance                             |
+---------------------------------------------------+
                        |
                        |
                        v
+---------------------------------------------------+
|                    Data Plane                     |
|---------------------------------------------------|
| Spark Driver                                      |
| Spark Executors                                   |
| Delta Lake Processing                             |
| Data Storage Access                               |
| Actual Data Computation                           |
+---------------------------------------------------+
```

### Architecture Components

#### Workspace

The web interface where users interact with Databricks.

#### Compute

Clusters or SQL Warehouses used to process data.

#### Storage

Cloud storage such as:

- Azure Data Lake Storage (ADLS)
- Amazon S3
- Google Cloud Storage (GCS)

#### Governance

Managed through Unity Catalog.

---

## Control Plane vs Data Plane

Understanding this concept is important for Databricks interviews.

### Control Plane

The Control Plane is managed by Databricks.

Its responsibilities include:

- Workspace UI
- Notebook metadata
- Cluster configuration
- Job scheduling
- Authentication
- Access management

Think of it as the "management layer."

---

### Data Plane

The Data Plane is where actual processing happens.

Responsibilities include:

- Running Spark jobs
- Executing notebooks
- Reading data
- Writing data
- Performing transformations

Think of it as the "execution layer."

---

### Simple Analogy

Imagine a restaurant.

#### Control Plane

- Manager
- Billing system
- Order management

#### Data Plane

- Kitchen
- Chefs
- Cooking process

The manager decides what should happen.

The kitchen performs the actual work.

---

## Workspace Overview

A Databricks Workspace is the central environment where users develop and manage workloads.

### Main Workspace Components

#### Notebooks

Interactive development environment for:

- Python
- SQL
- Scala
- R

#### Compute

Provides processing power through clusters and SQL warehouses.

#### Workflows

Used for scheduling and orchestrating jobs.

#### Catalog

Used to manage data assets through Unity Catalog.

#### Repos

Integration with Git repositories such as GitHub, GitLab, and Azure DevOps.

---

### Typical Workflow

```text
Notebook
    ↓
Cluster
    ↓
Read Data
    ↓
Transform Data
    ↓
Write Delta Table
    ↓
Schedule using Workflow
```

---

## Compute Types

Compute provides the resources required to execute workloads.

Databricks offers multiple compute options.

### All-Purpose Compute

Used for:

- Development
- Testing
- Exploration

Characteristics:

- Shared by multiple users
- Interactive
- Suitable for notebooks

---

### Job Compute

Used for:

- Scheduled jobs
- Production pipelines

Characteristics:

- Starts automatically
- Stops automatically
- Cost efficient
- Recommended for production

---

### SQL Warehouse

Used for:

- SQL queries
- BI dashboards
- Reporting

Commonly connected with:

- Power BI
- Tableau
- Databricks SQL Dashboards

---

### Serverless Compute

Databricks manages infrastructure automatically.

Benefits:

- No cluster management
- Fast startup
- Automatic scaling

---

## Key Takeaways

- Databricks is a unified Data and AI platform built on Apache Spark.
- The platform separates management (Control Plane) from execution (Data Plane).
- Workspaces provide a collaborative environment for development and analytics.
- Compute resources execute workloads.
- Common compute types include All-Purpose Compute, Job Compute, SQL Warehouses, and Serverless Compute.