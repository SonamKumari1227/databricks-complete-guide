# Databricks Notebooks

## What is a Databricks Notebook?

A Databricks Notebook is an interactive workspace where you can write and execute code, visualize data, and document your work.

Think of it as:

```text
Notebook = Code + Output + Documentation
```

Used for:

- Data Engineering
- Data Analysis
- Machine Learning
- SQL Queries
- Testing Spark Code

---

# Why Use Notebooks?

Benefits:

- Interactive development
- Easy debugging
- Supports multiple languages
- Built-in visualizations
- Easy collaboration

---

# Supported Languages

Databricks notebooks support:

```text
Python
SQL
Scala
R
```

Default language is selected when the notebook is created.

---

# Language Magic Commands

Switch languages inside the same notebook.

### Python

```python
print("Hello")
```

### SQL

```sql
%sql

SELECT * FROM customers;
```

### Scala

```scala
%scala

println("Hello")
```

### R

```r
%r

print("Hello")
```

---

# Notebook Structure

```text
Notebook
│
├── Cell 1
├── Cell 2
├── Cell 3
└── Cell N
```

Each cell executes independently.

---

# Running Cells

### Run Current Cell

```text
Shift + Enter
```

Runs the current cell and moves to the next.

---

### Run Cell Only

```text
Ctrl + Enter
```

Runs the current cell without moving.

---

# Spark Session

A SparkSession is automatically available.

Example:

```python
df = spark.read.csv("/path/file.csv")
```

No need to create:

```python
SparkSession.builder...
```

Databricks creates it automatically.

---

# Display Data

### Traditional Spark

```python
df.show()
```

### Databricks Way

```python
display(df)
```

Benefits:

- Better UI
- Filtering
- Sorting
- Charts

---

# Widgets

Widgets allow parameterized notebooks.

Example:

```python
dbutils.widgets.text(
    "country",
    "India"
)
```

Read value:

```python
country = dbutils.widgets.get("country")
```

Used heavily in production pipelines.

---

# Notebook Parameters

Common use case:

```text
ADF
 │
 ▼
Notebook
 │
 ▼
Read Parameter
 │
 ▼
Process Data
```

Example:

```python
year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
```

---

# Notebook Variables

Variables created in one cell are available in later cells.

Example:

```python
name = "Sonam"
```

Later:

```python
print(name)
```

---

# %run Command

Used to execute another notebook.

Example:

```python
%run ./common_functions
```

Useful for:

- Reusable functions
- Shared configurations
- Utility code

---

# Notebook Workflows

Notebook A:

```python
dbutils.notebook.run(
    "/ETL/bronze",
    3600
)
```

Used to call another notebook programmatically.

---

# Notebook Outputs

Possible outputs:

- Tables
- Charts
- Logs
- HTML
- Spark DataFrames

Example:

```python
display(df)
```

---

# Notebook Revision History

Databricks automatically tracks:

- Changes
- Previous versions
- Collaborator edits

Useful for troubleshooting.

---

# Notebook Best Practices

### Keep Notebooks Small

Good:

```text
Bronze Notebook
Silver Notebook
Gold Notebook
```

Bad:

```text
One notebook doing everything
```

---

### Move Logic to Functions

Bad:

```python
2000 lines in notebook
```

Good:

```python
def clean_data(df):
    ...
```

---

### Parameterize Notebooks

Avoid hardcoded values.

Bad:

```python
year = 2026
```

Good:

```python
year = dbutils.widgets.get("year")
```

---

### Separate Configurations

Store:

- Paths
- Secrets
- Environment Variables

Outside business logic.

---

# Common dbutils Commands

### List Files

```python
dbutils.fs.ls("/FileStore")
```

---

### Create Widget

```python
dbutils.widgets.text(
    "year",
    "2026"
)
```

---

### Get Widget Value

```python
dbutils.widgets.get("year")
```

---

### Remove Widget

```python
dbutils.widgets.removeAll()
```

---

# Notebook vs Script

| Notebook | Python Script |
|-----------|-----------|
| Interactive | Non-interactive |
| Easy debugging | Better for production code |
| Visual output | Code only |
| Exploration | Reusable modules |

---

# Interview Questions

### What is a Databricks Notebook?

An interactive development environment used to write, execute, and document code in Databricks.

---

### Which languages are supported?

- Python
- SQL
- Scala
- R

---

### What is `display()`?

A Databricks-specific function used to visualize DataFrames with filtering, sorting, and charting capabilities.

---

### What is `%run`?

Used to execute another notebook and import its variables/functions into the current notebook.

Example:

```python
%run ./common_functions
```

---

### What are widgets?

Notebook input parameters used to pass values dynamically at runtime.

---

### Difference between `%run` and `dbutils.notebook.run()`?

| %run | dbutils.notebook.run() |
|--------|--------|
| Imports notebook content | Executes notebook as a separate job |
| Same context | New execution context |
| Returns nothing | Can return values |

---

### Why use widgets?

To make notebooks reusable and avoid hardcoding values.

---

# Mental Model

```text
Notebook
│
├── Cells
├── Spark Code
├── SQL Queries
├── Widgets
├── Visualizations
└── Outputs
```

---
### Passing Parameters from ADF to Databricks Notebook

ADF can pass runtime values to a Databricks notebook using **Base Parameters** in the Notebook Activity.

**ADF → Notebook Activity → Base Parameters → dbutils.widgets → Notebook Logic**

Example:

```python
dbutils.widgets.text("year", "")
year = dbutils.widgets.get("year")
```

ADF passes:

```text
year = 2026
```

Use cases:

- Dynamic file paths
- Incremental loads
- Environment-specific configurations
- Reusable notebooks

**Interview Tip:**  
ADF passes parameters through **Base Parameters**, and Databricks receives them using **`dbutils.widgets`**.

---
# Quick Revision

```text
Notebook = Code + Documentation + Output

Languages:
✔ Python
✔ SQL
✔ Scala
✔ R

Important Commands:
✔ display()
✔ %sql
✔ %run
✔ dbutils.widgets
✔ dbutils.notebook.run()

Common Uses:
✔ ETL Development
✔ Data Exploration
✔ Testing
✔ Production Pipelines
```