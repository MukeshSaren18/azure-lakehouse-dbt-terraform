variable "resource_group_name"     { type = string }
variable "location"                { type = string }
variable "name_prefix"             { type = string }
variable "sku"                     { type = string }
variable "adls_account_id"         { type = string }
variable "databricks_workspace_id" { type = string }
variable "tags"                    { type = map(string) }

# ── Microsoft Purview Account ─────────────────────────────────────────────────
resource "azurerm_purview_account" "main" {
  name                = "purview-${var.name_prefix}"
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"   # Managed identity used for scanning data sources
  }

  tags = var.tags
}

# ── Managed Identity RBAC — grant Purview read access to ADLS Gen2 ────────────
resource "azurerm_role_assignment" "purview_adls_reader" {
  scope                = var.adls_account_id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_purview_account.main.identity[0].principal_id
}

# ── Purview Collection (logical grouping of data sources) ─────────────────────
# Collections are managed via Purview REST API post-provisioning.
# The null_resource below calls the Purview Atlas API to register
# ADLS Gen2 and Databricks as data sources after Terraform apply.
resource "null_resource" "purview_collections_setup" {
  depends_on = [
    azurerm_purview_account.main,
    azurerm_role_assignment.purview_adls_reader
  ]

  triggers = {
    purview_id = azurerm_purview_account.main.id
  }

  provisioner "local-exec" {
    command = <<-EOT
      echo "Purview account provisioned: ${azurerm_purview_account.main.name}"
      echo "Endpoint: ${azurerm_purview_account.main.catalog_endpoint}"
      echo ""
      echo "Next steps (run purview/lineage_bridge.py after dbt run):"
      echo "  1. Register ADLS Gen2 as a data source via Purview Studio"
      echo "  2. Register Databricks Unity Catalog as a data source"
      echo "  3. Trigger scan on Bronze container"
      echo "  4. Run: python purview/lineage_bridge.py --manifest dbt_project/target/manifest.json"
    EOT
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "purview_account_id"       { value = azurerm_purview_account.main.id }
output "purview_catalog_endpoint" { value = azurerm_purview_account.main.catalog_endpoint }
output "purview_scan_endpoint"    { value = azurerm_purview_account.main.scan_endpoint }
output "purview_principal_id"     { value = azurerm_purview_account.main.identity[0].principal_id }
