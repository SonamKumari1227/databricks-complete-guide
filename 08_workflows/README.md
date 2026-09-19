# 08 – Databricks Workflows

Workflows are how you take the code you wrote in a notebook and turn it into
something that **runs by itself, on time, every time, without you clicking Run**.

This folder explains Databricks Workflows from first principles: what a job is,
how tasks depend on each other, how scheduling works, what happens when things
fail, and the orchestration patterns used in real data platforms.

---

## Why this topic matters

```text
Notebook  =  you press Run          → good for exploring
Workflow  =  Databricks presses Run → good for production
```

Every production data pipeline eventually becomes a Workflow.

---

## Reading Order

| # | File | What you learn |
|---|------|----------------|
| 1 | [workflow_basics.md](workflow_basics.md) | What orchestration is and why jobs exist |
| 2 | [jobs_and_tasks.md](jobs_and_tasks.md) | Anatomy of a job, task types, compute choices |
| 3 | [task_dependencies.md](task_dependencies.md) | DAGs, branching, conditions, loops |
| 4 | [scheduling_and_triggers.md](scheduling_and_triggers.md) | Cron, file arrival, continuous, table triggers |
| 5 | [parameters_and_task_values.md](parameters_and_task_values.md) | Passing values into and between tasks |
| 6 | [notifications_and_monitoring.md](notifications_and_monitoring.md) | Alerts, run history, SLAs |
| 7 | [retries_and_failure_handling.md](retries_and_failure_handling.md) | Retries, timeouts, repair runs, idempotency |
| 8 | [orchestration_patterns.md](orchestration_patterns.md) | Real-world pipeline designs |
| 9 | [hands_on_project.md](hands_on_project.md) | Build a full medallion workflow |
| 10 | [interview_questions.md](interview_questions.md) | Questions asked in interviews |

---

## The Big Picture

```mermaid
flowchart LR
    A[Code<br/>Notebook / Python / SQL] --> B[Task]
    B --> C[Job<br/>one or more tasks]
    C --> D[Trigger<br/>schedule / file / manual]
    D --> E[Run<br/>one execution]
    E --> F[Result<br/>success / failure / retry]
    F --> G[Notification]
```

---

## Quick Revision

```text
Job        = the pipeline definition
Task       = one unit of work inside the job
Dependency = the arrow between tasks
Trigger    = what starts the job
Run        = one execution of the job
Repair     = re-run only the failed tasks
```
