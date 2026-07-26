variable "resource_group_name"     { type = string }
variable "location"                { type = string }
variable "name_prefix"             { type = string }
variable "sku"                     { type = string }
variable "adls_account_id"         { type = string }
variable "adls_primary_access_key" {
  type      = string
  sensitive = true
}
variable "node_type"       { type = string }
variable "min_workers"     { type = number }
variable "max_workers"     { type = number }
variable "runtime_version" { type = string }
variable "tags"            { type = map(string) }

# ── Databricks Workspace ──────────────────────────────────────────────────────
resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${var.name_prefix}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = var.sku   # Must be "premium" for Unity Catalog

  tags = var.tags
}

# ── Unity Catalog Metastore ───────────────────────────────────────────────────
# NOTE: One metastore per Azure region. If a metastore already exists
# in your region, import it instead of creating a new one:
#   terraform import databricks_metastore.main <metastore-id>
resource "databricks_metastore" "main" {
  name          = "metastore-${var.name_prefix}"
  region        = var.location
  storage_root  = "abfss://gold@${split("/", var.adls_account_id)[8]}.dfs.core.windows.net/unity-catalog"
  force_destroy = false   # Prevent accidental metastore deletion
}

resource "databricks_metastore_assignment" "main" {
  metastore_id = databricks_metastore.main.id
  workspace_id = azurerm_databricks_workspace.main.workspace_id
}

# ── Unity Catalog — Catalog & Schemas ────────────────────────────────────────
resource "databricks_catalog" "main" {
  metastore_id = databricks_metastore.main.id
  name         = "main"
  comment      = "Primary Unity Catalog catalog for ${var.name_prefix}"
  depends_on   = [databricks_metastore_assignment.main]
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.main.name
  name         = "bronze"
  comment      = "Raw ingestion layer — Auto Loader + ADF writes"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.main.name
  name         = "silver"
  comment      = "Cleaned, deduplicated staging layer — dbt views"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.main.name
  name         = "gold"
  comment      = "Gold marts and dimensions — dbt tables + OBT"
}

resource "databricks_schema" "snapshots" {
  catalog_name = databricks_catalog.main.name
  name         = "snapshots"
  comment      = "dbt SCD Type 2 snapshots"
}

# ── External Location (ADLS Gen2 → Unity Catalog) ────────────────────────────
resource "databricks_storage_credential" "adls" {
  name = "sc-${var.name_prefix}-adls"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_workspace.main.managed_resource_group_id
  }
}

resource "databricks_external_location" "bronze" {
  name            = "el-bronze"
  url             = "abfss://bronze@${split("/", var.adls_account_id)[8]}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.adls.name
  comment         = "External location for Bronze Delta tables on ADLS Gen2"
}

# ── Job Cluster (auto-scaling, spot instances) ────────────────────────────────
resource "databricks_cluster" "job_cluster" {
  cluster_name            = "cluster-${var.name_prefix}-job"
  spark_version           = var.runtime_version
  node_type_id            = var.node_type
  autotermination_minutes = 30

  autoscale {
    min_workers = var.min_workers
    max_workers = var.max_workers
  }

  # Use Azure Spot instances for cost savings (appropriate for batch jobs)
  azure_attributes {
    availability       = "SPOT_WITH_FALLBACK_AZURE"
    spot_bid_max_price = 100
  }

  spark_conf = {
    "spark.databricks.delta.preview.enabled"             = "true"
    "spark.databricks.delta.optimizeWrite.enabled"       = "true"
    "spark.databricks.delta.autoCompact.enabled"         = "true"
    "spark.sql.adaptive.enabled"                         = "true"   # AQE
    "spark.sql.adaptive.coalescePartitions.enabled"      = "true"
    "spark.databricks.unityCatalog.enabled"              = "true"
  }

  spark_env_vars = {
    "ADLS_ACCOUNT_KEY" = var.adls_primary_access_key
  }

  library {
    pypi { package = "dbt-databricks>=1.8.0" }
  }
  library {
    pypi { package = "dagster-databricks>=1.7.0" }
  }

  tags = var.tags
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "workspace_id"  { value = azurerm_databricks_workspace.main.workspace_id }
output "workspace_url" { value = azurerm_databricks_workspace.main.workspace_url }
output "cluster_id"    { value = databricks_cluster.job_cluster.cluster_id }
output "metastore_id"  { value = databricks_metastore.main.id }
