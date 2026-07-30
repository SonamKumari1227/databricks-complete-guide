# Databricks Cluster Types - Mental Models & Key Points

## 1. All-Purpose Cluster (Interactive Cluster)

### Mental Model
🏢 Permanent Office

- Office stays open even when employees are idle.
- Same office is reused for multiple tasks.

### Key Points

- Used for development, debugging, testing, ad-hoc analysis.
- Cluster is manually created.
- Notebooks attach to the same cluster.
- Driver and Worker VMs remain running.
- Cost continues even when idle.
- Auto-termination can stop the cluster after inactivity.

### Lifecycle

Start Cluster
→ Run Notebook A
→ Run Notebook B
→ Run Notebook C
→ Idle
→ Auto-Terminate (optional)

### Interview One-Liner

Used for interactive workloads where engineers need a persistent cluster environment.

---

## 2. Job Cluster

### Mental Model
🏨 Hotel Room / Temporary Project Team

- Created only when needed.
- Destroyed immediately after work finishes.

### Key Points

- Used for production ETL jobs.
- Created automatically when job starts.
- Terminated automatically when job completes.
- Fresh cluster environment for every run.
- More cost-efficient than all-purpose clusters.
- Does NOT wait for inactivity timeout.

### Lifecycle

Job Starts
→ Cluster Created
→ Execute Job
→ Job Finishes
→ Cluster Terminated

### Interview One-Liner

Used for automated production workloads and provides better cost efficiency and isolation.

---

## 3. Serverless Compute

### Mental Model
🚕 Uber / Coworking Space

- You use the service.
- Databricks manages the infrastructure.

### Key Points

- No cluster creation required.
- No VM selection.
- No autoscaling configuration.
- Databricks provisions and manages compute.
- Faster startup times.
- Still runs on cloud infrastructure.
- VMs exist but are hidden from users.

### Important Fact

Serverless ≠ No Servers

It means:

You don't manage the servers.

### Interview One-Liner

Infrastructure provisioning and scaling are abstracted away and managed automatically by Databricks.

---

## 4. Single Node Cluster

### Mental Model
👤 One Employee

- No team.
- No distributed processing.

### Key Points

- Only Driver node exists.
- No Worker nodes.
- Suitable for learning and small workloads.
- Spark still works but loses distributed benefits.

### Interview One-Liner

Spark can run on a single machine, but distributed computation is not utilized.

---

# Driver vs Worker

## Driver

### Mental Model
👨‍💼 Manager

Responsibilities:

- Receives Spark code.
- Creates execution plan.
- Schedules tasks.
- Coordinates workers.
- Collects results.

## Worker / Executor

### Mental Model
👷 Employees

Responsibilities:

- Execute Spark tasks.
- Process partitions.
- Return results to Driver.

---

# Cluster Cost Logic

## All-Purpose Cluster

VMs remain running.

Even when:

- No notebook running
- No user activity

Cost continues.

## Job Cluster

VMs exist only during job execution.

Job Ends
→ Cluster Ends
→ Cost Stops

---

# Interview Comparison

| Feature | All-Purpose | Job Cluster | Serverless |
|----------|------------|------------|------------|
| Main Use | Development | Production ETL | Managed Compute |
| Cluster Creation | Manual | Automatic | Hidden |
| Persistent | Yes | No | No |
| Auto Terminates | After inactivity | After job completion | Managed by Databricks |
| Cost | Higher | Lower | Pay-per-use |
| Infrastructure Visibility | Visible | Visible | Hidden |

---

# Ultimate Memory Trick

Databricks Workspace
│
├── 🏢 All-Purpose Cluster
│      Permanent Office
│
├── 🏨 Job Cluster
│      Temporary Project Team
│
├── 🚕 Serverless Compute
│      Uber of Compute
│
└── 👤 Single Node
       One Employee

Driver  = 👨‍💼 Manager
Workers = 👷 Employees
Cluster = 🏢 Office
Data     = 📦 Work