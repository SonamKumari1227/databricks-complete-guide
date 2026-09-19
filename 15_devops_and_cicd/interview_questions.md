# DevOps and CI/CD: Interview Questions

```text
1. Fundamentals
2. CLI and Tooling
3. Asset Bundles
4. Git and Environments
5. Testing
6. CI/CD Pipelines
7. Terraform
8. Scenario-Based
```

---

# 1. Fundamentals

### Why does a data platform need DevOps practices?

Because without them there is no review, no history, no rollback, and no
reproducibility. A job edited in the UI on Friday cannot be explained on Monday,
and dev and prod drift apart until deployments become guesswork.

### What are the four tools and what does each own?

```text
Databricks CLI  → interactive commands and CI steps
Asset Bundles   → jobs, DLT pipelines, task clusters (workloads)
Terraform       → workspaces, metastore, catalogs, grants, policies (platform)
Git             → the single source of truth for all of the above
```

### What does "everything as code" mean in practice?

Job definitions, pipeline definitions, cluster configuration, permissions, schema
migrations, and quality rules all live in Git and reach Databricks only through
an automated deployment.

---

# 2. CLI and Tooling

### How should CI authenticate to Databricks?

OAuth machine-to-machine credentials for a per-environment service principal,
supplied as CI secrets. Never a personal access token, never a human identity.

### How do you work with several workspaces from one machine?

Named profiles in `~/.databrickscfg` with `--profile`, or environment variables
per shell session.

### CLI or SDK?

CLI for interactive tasks and simple CI steps; the Python SDK when the automation
has real logic, since shell plus `jq` becomes unmaintainable quickly.

### How would you audit all jobs for missing failure alerts?

List jobs with `--output json` and filter with `jq`, or iterate with the SDK
checking `email_notifications` — the same approach finds jobs on all-purpose
clusters or owned by individuals.

---

# 3. Asset Bundles

### What is a Databricks Asset Bundle?

A folder containing code plus YAML describing the Databricks resources it needs —
jobs, pipelines, clusters, permissions — deployable per environment with
`databricks bundle deploy --target <env>`.

### How do bundles handle multiple environments?

Targets, each with its own workspace host, variables, `run_as` identity, and
mode, so one definition produces dev, staging, and prod deployments.

### What does `mode: development` do?

Prefixes resource names with the deploying user's name, pauses schedules, and
runs jobs as that user — so several engineers can deploy to one workspace without
colliding.

### What happens if you delete a job from the bundle YAML?

The next deploy removes it from the workspace, because deploy converges the
workspace to match the bundle.

### Why must bundle-managed jobs not be edited in the UI?

The next deployment overwrites the manual change. Remove UI edit permissions once
a job is bundle-managed.

### How do you avoid hard-coding resource ids?

Substitutions such as `${resources.pipelines.my_pipeline.id}` and
`${var.catalog}`, resolved at deploy time.

### How do you ship packaged Python code with a bundle?

Declare a wheel artifact that the bundle builds and uploads, and reference it
from tasks with `python_wheel_task` plus a `libraries` entry — which also makes
the logic unit testable.

---

# 4. Git and Environments

### How should notebooks be stored in Git?

As source files (`.py` with `# COMMAND ----------` markers, or `.sql`), never
`.ipynb`, so pull request diffs are reviewable rather than full of output and
metadata noise.

### Which branching model suits data platforms?

Trunk-based with short-lived feature branches, promoting through environments via
bundle targets rather than long-lived release branches.

### Separate workspaces or separate catalogs per environment?

Separate workspaces give genuine isolation of compute, permissions, and
networking, which production needs. Separate catalogs alone are cheaper but let a
mistake reach production data or compete for compute.

### How do you develop locally?

Databricks Connect with the VS Code extension: Spark code runs from the IDE
against a remote cluster, giving debugging, autocomplete, and a normal test
runner.

### How should shared code be organised?

Importable modules under `src/common`, packaged as a wheel — not `%run` of
another notebook, which is untestable and invisible to tooling.

### What belongs in a data pipeline code review that would not appear in an application review?

Idempotency of writes, parameterised dates, late-arriving data handling, quality
gate behaviour, schema compatibility for downstream consumers, and cost
implications of cluster size and schedule.

---

# 5. Testing

### How do you make Spark transformations testable?

Write them as pure functions taking a DataFrame and returning a DataFrame, and
keep reads and writes in a thin entry point.

### Describe the testing pyramid for a data platform.

Many fast unit tests of transformation functions, fewer integration tests against
sample data, a few end-to-end runs in staging, plus continuous data quality checks
running in production.

### What is the single most valuable test in a data pipeline?

The idempotency test: apply the write twice and assert the result is unchanged,
because retries and repair runs depend on it.

### Which edge cases should unit tests always cover?

Empty input, null business keys, unparseable values that cast to null,
duplicates, boundary values, and unexpected categorical values.

### How do you test SQL transformations?

Keep SQL in files, create temporary views from fixtures, execute the same file
the pipeline runs, and assert on the output.

### How do you catch breaking schema changes before consumers do?

A schema contract test asserting expected columns and types — additions pass,
removals and type changes fail.

### How do code tests differ from data quality checks?

Code tests verify logic against fixed fixtures in CI. Data quality checks verify
each day's real data in production on every run. You need both.

### Where does test data come from?

Hand-written fixtures for unit tests, a masked and sampled production subset for
integration, synthetic data for volume testing — never unmasked PII in dev.

---

# 6. CI/CD Pipelines

### Describe a complete CI/CD pipeline for Databricks.

```text
PR      → lint, unit tests, bundle validate for EVERY target
merge   → deploy to dev, run integration tests
tag     → deploy to staging, run the full pipeline and quality suite
approval→ deploy to prod, smoke test, notify
```

### Why validate every bundle target on a PR, not just dev?

To catch changes that resolve in dev but break another target — most commonly a
variable that exists in only one target block.

### How do you run integration tests that need a real cluster?

Define them as a job inside the bundle and trigger it with `databricks bundle
run`, which blocks until completion and returns a non-zero exit code on failure.

### What is a blue-green deployment for a table?

Build the new version as a separate table, validate it against the old one, then
repoint the view consumers read — making the switch instant and reversible.

### What is a shadow run and when do you use it?

Running a rewritten pipeline in parallel writing to a separate table, comparing
outputs daily until they match, before switching consumers. Used for risky
rewrites of business-critical pipelines.

### How do you roll back a bad release?

Redeploy the bundle from the previous tag to restore definitions, and
`RESTORE TABLE ... TO VERSION AS OF` to restore data, then reprocess the affected
window.

### How do you manage schema changes?

Versioned, additive, idempotent migration files applied by a job and recorded in
a control table, with a deprecation window before anything is removed.

---

# 7. Terraform

### What does Terraform manage that bundles do not?

Workspaces, metastores, catalogs and schemas, groups and service principals,
Unity Catalog grants, external locations, storage credentials, and cluster
policies.

### Why does the Databricks provider need two provider configurations?

Account-level resources use the accounts endpoint; workspace-level resources use
the workspace URL.

### Why put Unity Catalog grants in Terraform?

Access changes then require a pull request, leaving an audit trail and making
permissions reproducible across environments.

### Why grant to groups rather than users?

Onboarding and offboarding become membership changes, and permissions remain
auditable instead of accumulating unreviewable per-user grants.

### Why separate Terraform state per environment?

So a plan run against dev can never propose destroying production resources, and
so failures are contained.

### How do you adopt Terraform for an existing workspace?

Import the highest-value resources, write matching HCL, confirm `terraform plan`
reports no changes, freeze manual edits for those resources, then expand.

---

# 8. Scenario-Based

### Someone edited a production job in the UI and broke it. Design a prevention.

```text
1. Move the job into an Asset Bundle, deployed from Git
2. Remove CAN_MANAGE from individuals; grant it to the deploy service principal
   and CAN_VIEW to engineers
3. All changes go through a PR with CI validation
4. Rollback becomes redeploying the previous tag
5. Add an alert on job definition changes from system tables if available
```

### A new engineer takes three days to make their first change. Improve onboarding.

```text
Symptoms usually mean: no local dev, no docs, and manual setup.

Fix:
✔ databricks bundle init template with the team's conventions baked in
✔ Databricks Connect + VS Code extension documented in the README
✔ Dev catalog with masked sample data ready to query
✔ mode: development so their deploy cannot collide with anyone else
✔ A make target: setup, test, deploy-dev
✔ A first-task checklist in the repo
```

### Dev and prod have drifted apart. How do you converge them?

```text
1. Export prod job and pipeline definitions with the CLI
2. Convert to bundle YAML, parameterising everything environment-specific
3. Deploy to dev and diff behaviour against prod
4. Deploy to prod from the bundle — this is the moment of truth
5. Remove UI edit permissions so drift cannot restart
6. Terraform-import catalogs, grants, and policies for the same reason
```

### You must deploy a change to a pipeline that runs every 15 minutes with no downtime.

```text
1. Deploy during a gap between runs — bundle deploy is fast and atomic
   per resource, and a running job finishes with its existing definition
2. For a breaking transformation change, use blue-green:
   write to a new table, validate, repoint the consumer view
3. For a streaming pipeline, a stateful change needs a new checkpoint:
   run old and new in parallel, compare, then cut over
4. Always have the previous tag ready to redeploy
```

### Your CI takes 40 minutes and engineers stop running it. Fix it.

```text
Diagnose what is slow:
- Unit tests spinning up a Spark session per test → session-scoped fixture
- Integration tests running on every PR → move to post-merge
- A cluster starting for each CI job → use serverless, or a pool
- Full bundle deploy on PR → validate only, deploy after merge

Target: PR feedback under 10 minutes. Beyond that, people work around CI.
```

### How would you introduce CI/CD to a team that has never used it?

```text
Start with the smallest valuable slice, not the full pyramid:

Week 1: notebooks into Git as .py source, PR review required
Week 2: one job moved to a bundle, deployed to dev from CI
Week 3: unit tests for the transformation functions of that job
Week 4: staging target and a prod deploy with an approval gate
Then:   expand job by job, add integration tests, adopt Terraform for grants

Trying to do all of it at once is why these initiatives stall.
```

---

## Rapid-Fire Recap

```text
Tools:
CLI (commands) | Bundles (workloads) | Terraform (platform) | Git (truth)

Bundles:
databricks.yml + resources/*.yml + targets (dev/staging/prod)
mode: development → prefixed names, paused schedules
deploy is idempotent and deletes removed resources

Git:
notebooks as .py source | trunk-based | promotion via targets
logic in modules, notebooks as thin entry points

Testing:
pure functions → unit tests | integration in dev | quality gates in prod
idempotency test is the most valuable one

CI/CD:
PR: lint + unit + validate ALL targets
merge: deploy dev + integration
tag: staging + e2e → approval → prod + smoke test
auth: OAuth service principal per environment

Terraform:
metastore, catalogs, grants, groups, policies; state per environment
plan posted on the PR; apply on main

Rollback: redeploy previous tag + RESTORE TABLE TO VERSION AS OF
```
