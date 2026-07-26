terraform {
  required_version = ">= 1.8.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.45"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Remote state — Azure Blob Storage backend
  # Update storage_account_name and container_name to match your environment
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "stterraformstate"   # must be globally unique
    container_name       = "tfstate"
    key                  = "azure-lakehouse/terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}

# Databricks provider — uses workspace URL from the azurerm Databricks resource
provider "databricks" {
  host = azurerm_databricks_workspace.main.workspace_url
  # Authentication via Azure CLI / service principal (set via environment variables):
  # ARM_CLIENT_ID, ARM_CLIENT_SECRET, ARM_TENANT_ID
}
