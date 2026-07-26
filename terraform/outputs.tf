output "resource_group_name" {
  description = "Name of the provisioned resource group."
  value       = azurerm_resource_group.main.name
}

output "adls_storage_account_name" {
  description = "ADLS Gen2 storage account name."
  value       = module.adls.storage_account_name
}

output "adls_dfs_endpoint" {
  description = "ADLS Gen2 DFS (Data Lake) endpoint."
  value       = module.adls.dfs_endpoint
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL."
  value       = module.databricks.workspace_url
}

output "databricks_cluster_id" {
  description = "Default job cluster ID."
  value       = module.databricks.cluster_id
}

output "purview_catalog_endpoint" {
  description = "Microsoft Purview catalog endpoint (Atlas API base URL)."
  value       = length(module.purview) > 0 ? module.purview[0].purview_catalog_endpoint : "Purview not provisioned"
}

# dbt profiles.yml hint
output "dbt_profile_hint" {
  description = "Values to paste into ~/.dbt/profiles.yml"
  value = <<-EOT
    # Paste these into ~/.dbt/profiles.yml:
    # host: ${module.databricks.workspace_url}
    # (Set DATABRICKS_HTTP_PATH from the SQL warehouse you create in the workspace)
  EOT
}
