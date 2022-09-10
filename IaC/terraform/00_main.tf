# TF_VAR_ environment variables
variable "PROJECT_NAME" {}
variable "LOCATION" {
  default = "West Europe"
}

data "azurerm_client_config" "current" {}

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.22.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.28.1"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}

resource "azurerm_resource_group" "project" {
  name     = "rg-${var.PROJECT_NAME}-01"
  location = var.LOCATION
}

output "rg_name" {
  value       = azurerm_resource_group.project.name
  description = "RG name"
}