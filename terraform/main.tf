locals {
  name_prefix = "${var.project}-${var.environment}"
  common_tags = merge(var.tags, {
    environment = var.environment
    project     = var.project
  })
}

# ── Resource Group ────────────────────────────────────────────────────────────
resource "azurerm_resource_group" "main" {
  name     = "rg-${local.name_prefix}"
  location = var.location
  tags     = local.common_tags
}

# ── ADLS Gen2 Module ──────────────────────────────────────────────────────────
module "adls" {
  source = "./modules/adls"

  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  name_prefix         = local.name_prefix
  containers          = var.adls_containers
  replication_type    = var.adls_account_replication
  tags                = local.common_tags
}

# ── Databricks Module ─────────────────────────────────────────────────────────
module "databricks" {
  source = "./modules/databricks"

  resource_group_name        = azurerm_resource_group.main.name
  location                   = var.location
  name_prefix                = local.name_prefix
  sku                        = var.databricks_sku
  adls_account_id            = module.adls.storage_account_id
  adls_primary_access_key    = module.adls.primary_access_key
  node_type                  = var.cluster_node_type
  min_workers                = var.cluster_min_workers
  max_workers                = var.cluster_max_workers
  runtime_version            = var.databricks_runtime_version
  tags                       = local.common_tags
}

# ── Microsoft Purview Module ──────────────────────────────────────────────────
module "purview" {
  source = "./modules/purview"
  count  = var.enable_purview ? 1 : 0

  resource_group_name      = azurerm_resource_group.main.name
  location                 = var.location
  name_prefix              = local.name_prefix
  sku                      = var.purview_sku
  adls_account_id          = module.adls.storage_account_id
  databricks_workspace_id  = module.databricks.workspace_id
  tags                     = local.common_tags
}
