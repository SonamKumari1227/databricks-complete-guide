# Unity Catalog: Service Principals and OAuth

## Overview

Not every action in Databricks is performed by a human user.

Automated systems also need to access data and resources, such as:

```text
CI/CD pipelines
Scheduled jobs
Applications
External services
```

For this, Databricks and Unity Catalog use:

```text
Service Principals
OAuth Authentication
```

---

# 1. Service Principals

## What Is a Service Principal?

A Service Principal is an identity created for an **application or automated process**, not a human.

```text
Human User        -> logs in with email/password or SSO
Service Principal -> "logs in" using a client ID and secret/token
```

Think of it as a **robot user account**.

---

## Why Service Principals Are Needed

Without service principals:

```text
Automated jobs would need to use a real employee's login
```

Problems this causes:

```text
If the employee leaves, the job breaks
Hard to track what the "job" actually did vs the human
Security risk — personal credentials shared with automation
```

With service principals:

```text
Jobs and pipelines authenticate using their own dedicated identity
```

---

## Service Principal Use Cases

```text
Airflow / orchestration tools triggering Databricks jobs
CI/CD pipelines deploying notebooks or code
External applications querying data via API
Terraform provisioning Databricks/Unity Catalog resources
Scheduled ETL jobs running unattended
```

---

## Service Principal Flow Diagram

```text
        External System / Pipeline
                    |
                    v
           Service Principal
          (Client ID + Secret)
                    |
                    v
             Databricks / Unity Catalog
                    |
                    v
        Permissions granted like a normal user
                    |
                    v
         Access to Catalogs / Schemas / Tables
```

---

## Granting Permissions to a Service Principal

Service principals can be granted the same types of privileges as regular users.

```sql
GRANT SELECT
ON TABLE sales.gold.monthly_sales
TO `service-principal-app-id`;
```

```sql
GRANT USE CATALOG
ON CATALOG sales
TO `service-principal-app-id`;
```

They can also be added to groups:

```text
service_principal -> added to "etl_jobs_group"
etl_jobs_group    -> granted access to required catalogs/schemas
```

---

# 2. OAuth Authentication

## What Is OAuth?

OAuth is a secure, token-based authentication standard used to verify identity without repeatedly sharing raw passwords.

```text
Old Way:  Personal Access Token (PAT) shared and reused
New Way:  OAuth tokens, short-lived, automatically refreshed
```

---

## Why OAuth Is Preferred

```text
Tokens expire automatically       -> reduces long-term credential risk
Tokens can be scoped               -> limited to specific permissions
Easier to revoke access             -> disable at the identity provider level
Works well with service principals -> ideal for automation
```

---

## OAuth Authentication Flow

```text
      Application / Service Principal
                    |
                    v
        Request Token from Identity Provider
                    |
                    v
         Identity Provider issues OAuth Token
                    |
                    v
     Token sent with API request to Databricks
                    |
                    v
       Databricks validates token and authorizes
                    |
                    v
           Access granted (or denied)
```

---

## Example: Using OAuth with a Service Principal (Conceptual)

```text
1. Register a Service Principal in the Databricks account console.
2. Generate an OAuth secret for the Service Principal.
3. Application requests an OAuth token using client_id + client_secret.
4. Application uses the token in API calls (e.g. Bearer token in headers).
5. Databricks Unity Catalog authorizes actions based on the
   Service Principal's granted permissions.
```

```text
Authorization: Bearer <oauth_access_token>
```

---

## Service Principal + OAuth Together

```text
        Service Principal (Identity)
                    |
                    v
          OAuth Token (Authentication)
                    |
                    v
      Unity Catalog Permissions (Authorization)
                    |
                    v
             Secure Automated Access
```

Mental Model:

```text
Service Principal = WHO is acting (identity)

OAuth             = HOW it proves who it is (authentication)

Permissions       = WHAT it is allowed to do (authorization)
```

---

# Real-World Example

Suppose a company runs a nightly ETL pipeline using Airflow.

Requirements:

```text
The pipeline must read from sales.bronze.orders
The pipeline must write to sales.silver.orders
No individual employee's credentials should be used
```

Solution:

```text
1. Create a Service Principal: "svc-etl-nightly"
2. Grant it the following:
     GRANT SELECT ON TABLE sales.bronze.orders TO `svc-etl-nightly`;
     GRANT MODIFY ON TABLE sales.silver.orders TO `svc-etl-nightly`;
3. Configure Airflow to authenticate using OAuth 
   with the Service Principal's client ID and secret.
4. Airflow triggers the job -> Databricks validates the OAuth token
   -> Unity Catalog authorizes access based on granted permissions.
```

Now the pipeline runs securely and independently of any human account.

---

# Service Principals vs Human Users

| Aspect | Human User | Service Principal |
|--------|-----------|--------------------|
| Used by | People | Applications / Jobs |
| Login method | SSO / Password | Client ID + Secret / OAuth Token |
| Lifecycle tied to | Employment status | Application / pipeline lifecycle |
| Typical use | Interactive queries, notebooks | Automation, CI/CD, scheduled jobs |
| Auditability | Tracked as a person | Tracked as an application identity |

---

# Practical Reference

## Grant Permission to a Service Principal

```sql
GRANT SELECT ON TABLE catalog.schema.table_name
TO `service-principal-application-id`;
```

## Add Service Principal to a Group

```text
Account Console -> Groups -> Add Member -> Select Service Principal
```

## Revoke Access

```sql
REVOKE SELECT ON TABLE catalog.schema.table_name
FROM `service-principal-application-id`;
```

---

# Important Interview Questions

## What is a Service Principal in Databricks?

A Service Principal is a non-human identity used by applications, jobs, or automated processes to authenticate and interact with Databricks and Unity Catalog.

---

## Why use Service Principals instead of personal accounts for automation?

Because personal accounts tie automation to an individual employee, creating risks if the employee leaves or their credentials change; Service Principals provide a dedicated, stable, and auditable identity for automated processes.

---

## What is OAuth and why is it used?

OAuth is a token-based authentication standard that issues short-lived, scoped tokens instead of relying on long-lived shared passwords, making authentication more secure and easier to revoke.

---

## How do Service Principals and OAuth work together?

The Service Principal represents the identity performing the action, while OAuth is the mechanism used to authenticate that identity securely when making requests.

---

## Can Service Principals be granted the same permissions as regular users?

Yes. Service Principals can be granted privileges on catalogs, schemas, and tables, and can be added to groups, just like human users.

---

# Best Practices

## 1. Use Dedicated Service Principals per Workload

Avoid sharing one Service Principal across many unrelated pipelines.

---

## 2. Apply Least Privilege

Grant only the permissions a Service Principal actually needs.

---

## 3. Prefer OAuth Over Long-Lived Tokens

Use OAuth-based authentication instead of static personal access tokens where possible.

---

## 4. Rotate Secrets Regularly

Regularly rotate Service Principal secrets/tokens to reduce exposure risk.

---

## 5. Monitor Service Principal Activity

Use audit logs to track what each Service Principal is doing, just like a human user.

---

## 6. Group Service Principals Logically

Use groups to manage permissions for multiple related Service Principals efficiently.

---

# Complete Mental Model

```text
                 AUTOMATION NEEDS ACCESS
                          |
                          v
                 Service Principal Created
                          |
                          v
              OAuth used for Authentication
                          |
                          v
          Unity Catalog Permissions Applied
                          |
                          v
             Secure, Auditable, Automated Access
```

---

# Key Takeaways

- Service Principals are identities for applications, jobs, and automation — not humans.
- They remove the need to embed personal credentials in pipelines.
- OAuth provides secure, token-based authentication with expiring, scoped tokens.
- Service Principals can be granted permissions and added to groups like regular users.
- Service Principal = identity, OAuth = authentication, Permissions = authorization.
- Use least privilege and dedicated Service Principals per workload.
- Rotate secrets and monitor activity through audit logs.
- This combination enables secure, scalable automation across the Lakehouse.
