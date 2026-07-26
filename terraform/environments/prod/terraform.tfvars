environment              = "prod"
location                 = "westeurope"
project                  = "lakehouse"
adls_account_replication = "GRS"        # Geo-redundant for production
databricks_sku           = "premium"
cluster_node_type        = "Standard_DS4_v2"
cluster_min_workers      = 2
cluster_max_workers      = 16
enable_purview           = true         # Full Purview in prod

tags = {
  managed_by  = "terraform"
  owner       = "data-engineering"
  environment = "prod"
  project     = "lakehouse"
  cost_center = "data-platform"
}
