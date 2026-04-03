terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  # Uncomment and configure to enable remote state storage in Azure.
  # TODO: Pre-create the resource group, storage account, and container before use.
  #
  # backend "azurerm" {
  #   resource_group_name  = "rg-tfstate"
  #   storage_account_name = "sttfstateprod"
  #   container_name       = "tfstate"
  #   key                  = "spotify-de.terraform.tfstate"
  # }
}

# Authentication is handled via environment variables:
#   ARM_SUBSCRIPTION_ID
#   ARM_TENANT_ID
#   ARM_CLIENT_ID / ARM_CLIENT_SECRET  — for service principal (CI)
#   or: `az login` for interactive local use
provider "azurerm" {
  features {}
}
