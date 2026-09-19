# Security: Interview Questions

```text
1. Fundamentals
2. Identity and Access
3. Secrets
4. Data Protection
5. Network
6. Compliance
7. Scenario-Based
```

---

# 1. Fundamentals

### What are the layers of Databricks security?

Identity (who you are), access control (what you may do), data protection (what
you see inside the data), network (where you may connect from), and audit (what
you actually did). A gap in any layer undermines the others.

### What is the shared responsibility model?

Databricks secures the control plane, platform patching, and encryption in
transit. You own identities, permissions, secrets, network configuration, data
classification, and reviewing audit logs. Most incidents come from the second
list.

### What is the difference between the control plane and the data plane?

The control plane is Databricks-managed and holds the UI, API, scheduler, and
metadata. The data plane runs in your cloud account where clusters process data.

---

# 2. Identity and Access

### Why grant permissions to groups rather than individuals?

Onboarding and offboarding become membership changes, permissions remain
auditable, and access does not fragment into per-user grants nobody can
reconstruct or review.

### What is a service principal and why must production use one?

A non-human identity for automation. Jobs owned by it survive staff departures,
follow least privilege for a system rather than a person, and separate automated
from human activity in the audit trail.

### Why does a user with SELECT still get permission denied?

They also need `USE CATALOG` and `USE SCHEMA`. Compute permission is a further,
separate layer.

### How does privilege inheritance work?

Grants cascade downward — a grant on a catalog covers its schemas and tables,
including future ones. This is why sensitive data belongs in its own schema.

### Why does cluster access mode matter for security?

No-isolation clusters do not support Unity Catalog, so grants, row filters, and
column masks are not enforced. They must be blocked with a cluster policy.

### What is SCIM and why does it matter most for leavers?

Automated provisioning from the corporate IdP. Its real value is
deprovisioning — a departing employee loses access automatically rather than
depending on someone remembering to raise a ticket.

### Who should own Unity Catalog objects?

A group such as `data-platform-admins`. Owners can always grant, so individual
ownership creates both a risk and an orphan when that person leaves.

### What is catalog binding?

Restricting a catalog to specified workspaces, so production data cannot be
queried from a development workspace even by a principal holding the grant.

---

# 3. Secrets

### How should credentials be handled?

Secret scopes — ideally Key Vault-backed — read with `dbutils.secrets.get`, and
never written in notebooks, job definitions, repositories, or logs.

### Databricks-backed vs Key Vault-backed scopes?

Databricks-backed is simple and universally available. Key Vault-backed keeps
secrets in the enterprise vault with its own rotation, versioning, and audit, and
is read-only from Databricks.

### Does redaction make printing secrets safe?

No. Redaction catches exact values in output, but slicing, encoding, or sending a
secret to an external service defeats it. It is a safety net, not a control.

### Why does scope granularity matter?

READ on a scope grants every secret in it, so one shared scope lets development
identities read production credentials. Use one scope per environment.

### What is better than storing a storage account key?

A Unity Catalog storage credential backed by a managed identity or IAM role —
there is no key to leak or rotate, and access is governed and audited through
grants.

### How do you reference a secret in cluster configuration?

`{{secrets/scope/key}}`, which resolves at cluster start so the value never
appears in the job definition, in Git, or in the UI.

### What do you do when a credential leaks?

Rotate first, then revoke at the source, check audit logs for misuse, purge it
from history, and add scanning so it cannot recur.

---

# 4. Data Protection

### How do you hide sensitive values from most users?

Column masks: a function applied to the column returning the real value only for
members of a privileged group, enforced wherever the table is read.

### How do you restrict which rows a user sees?

Row filters: a function attached to the table returning a boolean per row,
typically combining group membership with a user-to-scope mapping table.

### Why are row filters and column masks better than dynamic views?

They attach to the table, so a user with SELECT on the base table cannot query
around them. A view only protects data accessed through the view.

### Which group-check function should you use?

`is_account_group_member`, because account-level groups are what SCIM provisions
and what Unity Catalog grants use.

### What is pseudonymisation?

Replacing identifiers with a salted hash so joins and analytics still work while
identity is removed. The salt lives in a secret scope so the hash cannot be
reversed from a known ID list.

### What breaks a GDPR erasure request in a lakehouse?

Delta time travel retains deleted rows until `VACUUM`, and bronze retains the raw
record. Erasure must cover both.

### What are customer-managed keys for?

Holding custody of encryption keys for managed storage and services, so the
organisation can rotate or revoke independently — used where regulation demands
it.

### How do you share data with an external partner safely?

Delta Sharing from a curated, share-safe gold schema, with row filters applied,
expiring recipient tokens, IP restrictions where available, and audited access.

---

# 5. Network

### What is secure cluster connectivity?

Clusters have no public IP and open an outbound connection to the control plane,
so no inbound access is needed. It is the production baseline.

### Why deploy into your own VNet or VPC?

To apply your own security groups, egress firewall rules, private endpoints to
storage, on-premises connectivity, and traffic inspection.

### What do IP access lists protect, and what can go wrong?

They restrict UI and API access by source IP. The common failure is omitting
CI/CD runner ranges or a break-glass path, causing outages or lockouts.

### How do you prevent data exfiltration from a cluster?

Egress control: a firewall or NAT with a destination allowlist, plus a private
package mirror so open internet access is unnecessary.

### How does serverless change the network model?

Serverless compute runs in the Databricks account, so your VNet controls do not
apply. Private connectivity is configured through network connectivity
configurations with stable egress IPs.

### What is the recommended workspace isolation strategy?

Separate workspaces per environment at minimum, one metastore per region, and
catalog binding so production catalogs are reachable only from production
workspaces.

---

# 6. Compliance

### Where do you find who accessed a sensitive table?

`system.access.audit` filtered on the Unity Catalog service and the table name.

### How do you keep audit data for seven years?

A daily job appending from `system.access.audit` into a Delta table owned by the
compliance group, with modify permission revoked from engineers.

### How does lineage support compliance?

It proves the provenance of regulated outputs automatically and reveals where PII
has flowed, including into tables it should not have reached.

### How do you demonstrate data accuracy?

Retained quality metrics — DLT expectation pass and fail counts from the event
log, or row and quarantine counts from a control table — covering the audit
period.

### What is segregation of duties here?

Engineers author and review code but cannot change production directly;
deployment runs through CI as a service principal, so no one person both writes
and unilaterally deploys.

### How do you run an efficient access review?

Compare granted permissions on sensitive schemas against actual access in the
audit log, and revoke everything granted but unused over the period.

---

# 7. Scenario-Based

### A data scientist needs customer data for a model but must not see PII. Design it.

```text
1. Classify: tag PII columns (email, phone, national_id)
2. Separate: move PII into main.pii, granted only to pii-readers
3. Provide main.silver.customers without direct identifiers, with a
   salted-hash customer_key for joins
4. Apply column masks on any PII remaining in shared tables
5. Grant the data scientist SELECT on the non-PII schema only
6. Audit: confirm from access logs that they never touch main.pii
7. If raw PII is genuinely required, time-bound access with approval,
   logged and reviewed on expiry
```

### An auditor asks who has accessed the customer table in the last year. How do you answer?

```text
1. system.access.audit filtered on that table for access events
2. If beyond system table retention, query the compliance archive
3. Cross-reference with SHOW GRANTS to show who COULD access it
4. Present both: granted access and exercised access
5. Show the quarterly review records where unused grants were revoked

If you cannot answer this, the corrective action is the audit archive job —
build it before the next audit, not during.
```

### A contractor's laptop is compromised. What is your response?

```text
Immediate:
1. Disable the account in the IdP — SCIM propagates the deactivation
2. Revoke active tokens and OAuth sessions
3. Check audit logs for activity from that identity and unusual source IPs
4. Check for data downloads or exports in the audit log

Assessment:
5. Determine what they had access to (group memberships and grants)
6. Rotate any secrets in scopes they could read
7. Check whether any job ran as their identity — if so, that is a finding

Prevention:
8. Confirm production runs as service principals, not people
9. Confirm IP access lists or private connectivity are enforced
10. Confirm time-bound access for contractors
```

### Your workspace is open to the internet with password authentication. Prioritise the fixes.

```text
1. SSO with MFA through the corporate IdP — largest risk reduction first
2. IP access list for the workspace (include CI runner ranges,
   and keep a break-glass path)
3. Secure cluster connectivity: no public IPs on compute
4. Storage firewall: deny public access, add private endpoints
5. Private Link for the workspace front end
6. Egress control with an allowlist
7. Block no-isolation clusters by policy

Order matters: identity controls first, because they stop the most
likely attack. Network hardening is slower and more disruptive.
```

### Someone discovers a production database password in a notebook in Git. What now?

```text
1. Rotate the credential immediately — assume it is compromised
2. Revoke the old credential at the source database
3. Check database and audit logs for use of the old credential
4. Move it to a secret scope; update the notebook to dbutils.secrets.get
5. Purge from Git history, accepting that clones and forks may retain it
6. Enable pre-commit and repository secret scanning
7. Review other notebooks for the same pattern
8. Record it as an incident with the prevention actions
```

### How would you secure a new workspace from day one?

```text
Identity:  SSO + MFA + SCIM; groups for everything; SPs per environment
Access:    Unity Catalog, grants to groups in Terraform, catalog binding
Compute:   cluster policies — no-isolation blocked, tags mandatory,
           autotermination enforced
Secrets:   Key Vault-backed scopes per environment, ACLs set
Data:      classification tags, PII in its own schema, masks and row filters
Network:   no public IPs, VNet injection, private endpoints, IP access list,
           egress allowlist
Audit:     system tables enabled, audit archive job, alerts on permission
           changes and failed access
Process:   everything in Git, deployment via CI, quarterly access reviews
```

---

## Rapid-Fire Recap

```text
Layers: identity → access → data protection → network → audit

Identity: SCIM, groups always, service principals for all production
Access:   USE CATALOG + USE SCHEMA + SELECT; grants cascade; own with groups
          block no-isolation clusters; catalog binding for environment isolation

Secrets:  scopes (Key Vault-backed), one per environment, ACLs
          UC storage credentials instead of storage keys
          redaction is a safety net, not a control

Data:     classify with tags → column masks + row filters (not dynamic views)
          PII in its own schema | pseudonymisation | erasure = DELETE + VACUUM

Network:  SSO/MFA → IP access list → no public IPs → VNet injection →
          private endpoints → egress allowlist

Audit:    system.access.audit → archive for retention → alerts on permission
          changes and failures → quarterly access reviews

Leak response: rotate → revoke → audit → purge → detect
```
