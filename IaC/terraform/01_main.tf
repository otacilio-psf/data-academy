variable "SUBSCRIPTION_ID" {}
variable "TENANT_ID" {}
variable "SP_ACADEMY_ID" {}
variable "SP_ACADEMY_SECRET" {}
variable "SP_ACADEMY_OBJ_ID" {}
variable "project_name" {}
variable "project_name_ss" {}
variable "location" {}

data "azurerm_client_config" "current" {}

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.0.0"
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
}

resource "azurerm_resource_group" "project" {
  name     = "rg-${var.project_name}"
  location = var.location
}