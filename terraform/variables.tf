variable "environment" {
  description = "Deployment environment. Used to suffix resource names and apply environment-specific configs."
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "westeurope"
}

variable "project" {
  description = "Project name prefix for all resource names."
  type        = string
  default     = "lakehouse"
}

variable "tags" {
  description = "Common resource tags applied to all provisioned resources."
  type        = map(string)
  default = {
    managed_by = "terraform"
    owner      = "data-engineering"
  }
}

# ── ADLS Gen2 ────────────────────────────────────────────────────────────────
variable "adls_account_replication" {
  description = "ADLS Gen2 replication type. LRS for dev, GRS for prod."
  type        = string
  default     = "LRS"
}

variable "adls_containers" {
  description = "List of container names to create in ADLS Gen2."
  type        = list(string)
  default     = ["bronze", "silver", "gold", "checkpoints", "mlflow-artifacts"]
}

# ── Databricks ────────────────────────────────────────────────────────────────
variable "databricks_sku" {
  description = "Databricks workspace SKU. Must be 'premium' for Unity Catalog."
  type        = string
  default     = "premium"
}

variable "cluster_node_type" {
  description = "Databricks cluster node type."
  type        = string
  default     = "Standard_DS3_v2"
}

variable "cluster_min_workers" {
  type    = number
  default = 1
}

variable "cluster_max_workers" {
  type    = number
  default = 8
}

variable "databricks_runtime_version" {
  description = "Databricks Runtime version (LTS preferred for production)."
  type        = string
  default     = "14.3.x-scala2.12"   # Databricks Runtime 14.3 LTS
}

# ── Microsoft Purview ────────────────────────────────────────────────────────
variable "purview_sku" {
  description = "Purview account SKU."
  type        = string
  default     = "Standard"
}

variable "enable_purview" {
  description = "Toggle Purview provisioning. Set false in dev to reduce cost."
  type        = bool
  default     = true
}
