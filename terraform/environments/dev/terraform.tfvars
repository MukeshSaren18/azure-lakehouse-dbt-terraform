environment              = "dev"
location                 = "westeurope"
project                  = "lakehouse"
adls_account_replication = "LRS"        # LRS is sufficient and cheapest for dev
databricks_sku           = "premium"    # Required for Unity Catalog
cluster_node_type        = "Standard_DS3_v2"
cluster_min_workers      = 1
cluster_max_workers      = 4            # Smaller ceiling in dev
enable_purview           = false        # Skip Purview in dev — reduces cost significantly

tags = {
  managed_by  = "terraform"
  owner       = "data-engineering"
  environment = "dev"
  project     = "lakehouse"
}
