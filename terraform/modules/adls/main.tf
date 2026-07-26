variable "resource_group_name" { type = string }
variable "location"            { type = string }
variable "name_prefix"         { type = string }
variable "containers"          { type = list(string) }
variable "replication_type"    { type = string }
variable "tags"                { type = map(string) }

# ── Storage Account (ADLS Gen2) ───────────────────────────────────────────────
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_storage_account" "adls" {
  name                     = "st${replace(var.name_prefix, "-", "")}${random_string.suffix.result}"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = var.replication_type
  account_kind             = "StorageV2"

  # Enable Hierarchical Namespace — required for ADLS Gen2
  is_hns_enabled = true

  # Security hardening
  min_tls_version          = "TLS1_2"
  enable_https_traffic_only = true
  public_network_access_enabled = false   # VNet-only access in prod

  blob_properties {
    # Soft delete for accidental deletion recovery
    delete_retention_policy {
      days = 30
    }
    container_delete_retention_policy {
      days = 30
    }
  }

  tags = var.tags
}

# ── Containers (Bronze / Silver / Gold / Checkpoints / MLflow) ────────────────
resource "azurerm_storage_container" "layers" {
  for_each = toset(var.containers)

  name                  = each.value
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# ── Private Endpoint (production security) ────────────────────────────────────
# Uncomment when VNet is available in your environment
# resource "azurerm_private_endpoint" "adls" {
#   name                = "pe-${var.name_prefix}-adls"
#   resource_group_name = var.resource_group_name
#   location            = var.location
#   subnet_id           = var.subnet_id
#
#   private_service_connection {
#     name                           = "psc-adls"
#     private_connection_resource_id = azurerm_storage_account.adls.id
#     subresource_names              = ["dfs"]   # dfs = ADLS Gen2 endpoint
#     is_manual_connection           = false
#   }
# }

# ── Outputs ───────────────────────────────────────────────────────────────────
output "storage_account_id"   { value = azurerm_storage_account.adls.id }
output "storage_account_name" { value = azurerm_storage_account.adls.name }
output "primary_access_key" {
  value     = azurerm_storage_account.adls.primary_access_key
  sensitive = true
}
output "dfs_endpoint" {
  value = azurerm_storage_account.adls.primary_dfs_endpoint
}
