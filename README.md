# Azure Lakehouse Platform — dbt · Terraform · Purview · Dagster

![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![Terraform](https://img.shields.io/badge/Terraform-1.8-purple)
![Azure](https://img.shields.io/badge/Azure-Databricks-blue)
![Purview](https://img.shields.io/badge/Microsoft-Purview-0078D4)
![Dagster](https://img.shields.io/badge/Dagster-1.7-blueviolet)
![Python](https://img.shields.io/badge/python-3.11-blue)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

A production-grade **Azure Lakehouse data platform** extending the existing Databricks Medallion Architecture with:

- **dbt** for modular, version-controlled SQL transformations across Silver → Gold layers
- **Terraform** for IaC provisioning of the full Azure data stack (ADLS Gen2, Databricks, Purview)
- **Microsoft Purview** for enterprise data cataloguing, lineage scanning, and policy enforcement
- **Dagster** as a modern orchestrator with asset-based pipeline visibility alongside Apache Airflow

Built to complement the [Real-Time ML Feature Store](https://github.com/MukeshSaren18/realtime-ml-feature-store) and [Multi-Agent RAG system](https://github.com/MukeshSaren18) — completing the full Azure + Databricks + GenAI platform stack.

---

## Architecture

```
                    ┌──────────────────────────────────────┐
                    │         Terraform (IaC)              │
                    │  Provisions & manages all infra below│
                    └──────────────────┬───────────────────┘
                                       │
          ┌────────────────────────────▼──────────────────────────────┐
          │                  Azure Data Platform                      │
          │                                                           │
          │  ADLS Gen2          Azure Databricks       Azure Purview  │
          │  (OneLake)          (Lakehouse)             (Governance)  │
          └────────────────────────────┬──────────────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────┐
                    │       Medallion Lakehouse           │
                    │                                     │
                    │  Bronze (raw)  →  Silver (cleaned)  │
                    │         Auto Loader / ADF           │
                    └──────────────────┬──────────────────┘
                                       │
                    ┌──────────────────▼──────────────────┐
                    │         dbt Transformations         │
                    │                                     │
                    │  Silver models  →  Gold models      │
                    │  (staging)          (marts + OBT)   │
                    │  Tests · Docs · Lineage             │
                    └──────────────────┬──────────────────┘
                                       │
               ┌───────────────────────▼────────────────────────┐
               │              Orchestration                     │
               │  Dagster (asset-based) + Airflow (legacy DAGs) │
               └────────────────────────────────────────────────┘
                                       │
                    ┌──────────────────▼─────────────────────┐
                    │        Microsoft Purview               │
                    │  Catalogue · Lineage · Classifications │
                    │  Policy enforcement · Data quality     │
                    └────────────────────────────────────────┘
```

---

## Key Features

- **dbt Medallion models** — Silver staging models + Gold data marts (star schema) with full tests, macros, and docs
- **Terraform IaC** — reproducible provisioning of ADLS Gen2, Databricks workspaces, Unity Catalog, and Purview account
- **Microsoft Purview integration** — automated scanning of ADLS Gen2 + Databricks, lineage propagation, sensitivity classifications
- **Dagster asset graph** — software-defined assets wiring Bronze ingestion → dbt Silver/Gold → downstream ML feature pipelines
- **dbt → Purview lineage bridge** — pushes dbt run artifacts into Purview Atlas API for end-to-end column-level lineage
- **CI/CD** — GitHub Actions: dbt test + slim CI on PR, Terraform plan on PR / apply on merge

---

## Project Structure

```
azure-lakehouse-dbt-terraform/
├── dbt_project/
│   ├── dbt_project.yml                    # dbt project config
│   ├── profiles.yml                       # Databricks + DuckDB profiles
│   ├── packages.yml                       # dbt-databricks, dbt-utils, dbt-expectations
│   ├── models/
│   │   ├── bronze/                        # Raw source declarations (sources.yml)
│   │   ├── silver/                        # Staging models — clean, typed, deduplicated
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_transactions.sql
│   │   └── gold/                          # Marts — star schema + OBT
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       ├── fct_orders.sql
│   │       └── mart_customer_lifetime.sql
│   ├── tests/                             # Custom singular + generic tests
│   ├── macros/                            # Reusable SQL macros
│   └── snapshots/                         # SCD Type 2 snapshots
├── terraform/
│   ├── main.tf                            # Root module — wires all child modules
│   ├── variables.tf
│   ├── outputs.tf
│   ├── providers.tf                       # AzureRM + Databricks providers
│   ├── modules/
│   │   ├── adls/                          # ADLS Gen2 + containers + RBAC
│   │   ├── databricks/                    # Workspace + Unity Catalog + clusters
│   │   └── purview/                       # Purview account + collections + sources
│   └── environments/
│       ├── dev/terraform.tfvars
│       └── prod/terraform.tfvars
├── orchestration/
│   ├── dagster_jobs/
│   │   ├── assets.py                      # Software-defined assets (Bronze → Gold)
│   │   ├── jobs.py                        # Job definitions + schedules
│   │   └── resources.py                   # Databricks + dbt resource configs
│   └── airflow_dags/
│       └── lakehouse_pipeline.py          # Legacy Airflow DAG (backward-compatible)
├── purview/
│   └── lineage_bridge.py                  # Pushes dbt artifacts → Purview Atlas API
├── .github/
│   └── workflows/
│       ├── dbt_ci.yml                     # dbt slim CI on PRs
│       └── terraform_ci.yml               # Terraform plan/apply pipeline
└── docs/
    ├── architecture.md
    ├── dbt_guide.md
    └── terraform_guide.md
```

---

## Quickstart

### Prerequisites
```bash
# Python 3.11+
pip install dbt-databricks dbt-utils dagster dagster-databricks

# Terraform
brew install terraform   # or choco install terraform on Windows

# Azure CLI (for Purview + ADLS auth)
az login
az account set --subscription <SUBSCRIPTION_ID>
```

### 1 — Provision infrastructure with Terraform
```bash
cd terraform/
terraform init
terraform plan -var-file=environments/dev/terraform.tfvars
terraform apply -var-file=environments/dev/terraform.tfvars
```

### 2 — Configure dbt
```bash
cd dbt_project/
cp profiles.yml.example ~/.dbt/profiles.yml
# Update with your Databricks workspace URL + HTTP path
dbt deps         # install packages
dbt debug        # verify connection
```

### 3 — Run the full dbt pipeline
```bash
dbt seed                          # load reference data
dbt run --select staging          # Silver layer
dbt run --select marts            # Gold layer
dbt test                          # run all tests
dbt docs generate && dbt docs serve   # browse lineage
```

### 4 — Launch Dagster
```bash
cd orchestration/dagster_jobs/
dagster dev    # opens asset graph UI at http://localhost:3000
```

### 5 — Push lineage to Purview
```bash
python purview/lineage_bridge.py \
  --manifest dbt_project/target/manifest.json \
  --purview-endpoint https://<account>.purview.azure.com
```

---

## dbt Model Conventions

| Layer  | Prefix | Materialisation | Purpose |
|--------|--------|----------------|---------|
| Bronze | —      | source()        | Raw Delta tables; declared only |
| Silver | `stg_` | view            | Cleaned, typed, deduplicated |
| Gold   | `dim_` / `fct_` / `mart_` | table / incremental | Marts, star schema, OBT |

All models include:
- `not_null` + `unique` tests on primary keys
- `accepted_values` tests on enum columns
- `dbt-expectations` range checks on numeric columns
- Column-level descriptions in schema YAML (feeds Purview catalogue)

---

## Terraform — Resources Provisioned

| Resource | Module | Notes |
|----------|--------|-------|
| Resource Group | root | Environment-tagged |
| ADLS Gen2 Storage Account | `adls` | HNS enabled, TLS 1.2+ |
| Bronze / Silver / Gold containers | `adls` | RBAC-scoped per layer |
| Databricks Workspace | `databricks` | Premium tier (Unity Catalog requires it) |
| Unity Catalog Metastore | `databricks` | Single metastore per region |
| Databricks Cluster (job cluster) | `databricks` | Auto-scaling, spot instances |
| Microsoft Purview Account | `purview` | Standard tier |
| Purview Collection | `purview` | Maps to Databricks + ADLS sources |
| Managed Identity | root | Used by Purview for scanning |

---

## Microsoft Purview Integration

**What's automated:**
1. Terraform provisions the Purview account and registers ADLS Gen2 + Databricks as data sources
2. `lineage_bridge.py` reads `dbt run` artifacts (`manifest.json`, `run_results.json`) and pushes column-level lineage into Purview's Atlas API
3. Purview auto-classifies sensitive columns (PII, financial) using built-in classifiers
4. Data quality alerts from Databricks observability feed into Purview's insights dashboard

**Why this matters for enterprise Azure stacks:**
Unity Catalog handles Databricks-internal lineage. Purview extends that lineage to the full Azure estate — ADLS raw files, ADF pipelines, SQL databases — giving org-wide, cross-platform data governance.

---

## CI/CD — GitHub Actions

### dbt slim CI (on every PR)
```yaml
# Runs only models changed in this PR + their downstream dependents
dbt run --select state:modified+
dbt test --select state:modified+
```

### Terraform CI (on every PR → main)
```
PR  → terraform plan   (output posted as PR comment)
Merge → terraform apply (only prod after manual approval)
```

---

## Results & Design Decisions

| Decision | Rationale |
|----------|-----------|
| dbt on Databricks (not Synapse) | Keeps transformations in the Lakehouse; avoids dual-platform complexity |
| Dagster over pure Airflow | Asset-based model gives end-to-end observability; Airflow DAG retained for backward compat |
| Purview + Unity Catalog | Unity Catalog = Databricks-internal; Purview = cross-Azure estate. Both needed for full enterprise governance |
| dbt → Purview lineage bridge | Closes the gap between dbt's internal DAG and Purview's Atlas graph |
| Terraform modules (not monolith) | Allows dev/prod environment parity with `tfvars` overrides |

---

## Tech Stack
`dbt-databricks 1.8` · `Terraform 1.8` · `Microsoft Purview` · `Dagster 1.7` · `Azure Databricks` · `ADLS Gen2` · `Unity Catalog` · `Apache Airflow` · `Python 3.11` · `GitHub Actions`
