# 8-Week Lakehouse Platform Roadmap

A structured, 2-month engineering backlog to complete every component documented in `README.md`, add automated test coverage, and harden the Azure Lakehouse platform.

> **Cadence:** 3 tasks per week (Mon / Wed / Fri). Each task is delivered on its own feature branch for PR review and merge.

---

## Weeks 1–2: Packaging, Foundation & Missing dbt Core Assets
- [ ] **W1-1:** Add `pyproject.toml`, `requirements.txt`, and `LICENSE` (MIT) so Python tooling and Dependabot pip updates work out of the box.
- [ ] **W1-2:** Add `dbt_project/profiles.yml.example` and sanitize `dbt_project/profiles.yml` for local DuckDB + Databricks env-var config.
- [ ] **W1-3:** Add `dbt_project/seeds/` with sample reference data (e.g., `country_codes.csv`, `currency_rates.csv`) and seed schema YAML.
- [ ] **W2-1:** Implement missing Gold dimension model `dbt_project/models/gold/dim_products.sql` and register it in `models/gold/schema.yml`.
- [ ] **W2-2:** Add custom singular dbt tests under `dbt_project/tests/` (e.g., `assert_positive_order_amounts.sql`, `assert_valid_customer_ltv.sql`).
- [ ] **W2-3:** Add SCD Type 2 dbt snapshot under `dbt_project/snapshots/snap_customers.sql` with documentation.

## Weeks 3–4: Python Unit Tests & Purview Lineage Bridge Hardening
- [ ] **W3-1:** Add `tests/test_lineage_bridge.py` with pytest unit tests covering `extract_models`, `build_atlas_entities`, and `--dry-run` using a fixture manifest.
- [ ] **W3-2:** Fix and enhance `purview/lineage_bridge.py` upstream dependency FQN resolution (map `depends_on` model IDs to actual table FQNs rather than naive string replacement) and add unit tests.
- [ ] **W3-3:** Add retry/backoff handling and `--run-results` status enrichment in `purview/lineage_bridge.py` with tests.
- [ ] **W4-1:** Extract Dagster resource configs into `orchestration/dagster_jobs/resources.py` (as documented in `README.md`).
- [ ] **W4-2:** Extract Dagster job & schedule definitions into `orchestration/dagster_jobs/jobs.py` and wire `assets.py` cleanly.
- [ ] **W4-3:** Add `tests/test_dagster_assets.py` verifying asset definitions, metadata, and helper behavior with mocks.

## Weeks 5–6: Airflow Compatibility & Terraform Hardening
- [ ] **W5-1:** Implement backward-compatible `orchestration/airflow_dags/lakehouse_pipeline.py` (documented in `README.md`) with Bronze → dbt → Purview task graph.
- [ ] **W5-2:** Add unit test `tests/test_airflow_dag.py` validating DAG structure, task dependencies, and schedule without requiring an Airflow DB.
- [ ] **W5-3:** Add `variables.tf` and `outputs.tf` (with descriptions and validation rules) to `terraform/modules/adls/`.
- [ ] **W6-1:** Add `variables.tf` and `outputs.tf` (with descriptions and validation rules) to `terraform/modules/databricks/`.
- [ ] **W6-2:** Add `variables.tf` and `outputs.tf` (with descriptions and validation rules) to `terraform/modules/purview/`.
- [ ] **W6-3:** Add `terraform/backend.tf.example` (Azure Storage remote state) and `tflint` / security check step in Terraform CI.

## Weeks 7–8: Documentation, Observability & v1.0 Release
- [ ] **W7-1:** Write `docs/architecture.md` covering Medallion data flow, Unity Catalog + Purview governance boundaries, and network/RBAC design.
- [ ] **W7-2:** Write `docs/dbt_guide.md` covering local DuckDB dev workflow, slim CI, testing conventions, and snapshots.
- [ ] **W7-3:** Write `docs/terraform_guide.md` covering remote state setup, dev/prod promotion, and secret management.
- [ ] **W8-1:** Add `CONTRIBUTING.md` and pre-commit config (`.pre-commit-config.yaml`) for Ruff, YAML check, and Terraform fmt.
- [ ] **W8-2:** Add `CHANGELOG.md` summarizing all platform milestones and update `README.md` badges and quickstart accuracy.
- [ ] **W8-3:** Add a `Makefile` with one-command developer targets (`make lint`, `make test`, `make tf-fmt`, `make dbt-docs`) and prepare `v1.0.0` release notes.
