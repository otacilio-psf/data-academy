# TF_VAR_ environment variables
variable "SUBSCRIPTION_ID" {}
variable "TENANT_ID" {}
variable "SP_TERRAFORM_ID" {}
variable "SP_TERRAFORM_SECRET" {}
variable "PROJECT_NAME" {}
variable "LOCATION" {
  default = "West Europe"
}

data "azurerm_client_config" "current" {}

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.0.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.15.0"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }

  subscription_id = var.SUBSCRIPTION_ID
  tenant_id       = var.TENANT_ID
  client_id       = var.SP_TERRAFORM_ID
  client_secret   = var.SP_TERRAFORM_SECRET
}

resource "azurerm_resource_group" "project" {
  name     = "rg-${var.PROJECT_NAME}-01"
  location = var.LOCATION
}

output "rg_name" {
  value       = azurerm_resource_group.project.name
  description = "RG name"
}