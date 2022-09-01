resource "azurerm_storage_account" "stg" {
  name                     = "stg${var.project_name_ss}01"
  resource_group_name      = azurerm_resource_group.project.name
  location                 = azurerm_resource_group.project.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "datalake" {
  name               = "datalake"
  storage_account_id = azurerm_storage_account.stg.id

  ace {
    scope = "access"
    type = "user"
    id = var.SP_ACADEMY_OBJ_ID
    permissions  = "rwx"
  }

  ace {
    scope = "default"
    type = "user"
    id = var.SP_ACADEMY_OBJ_ID
    permissions  = "rwx"
  }

  ace {
    type = "other"
    permissions  = "---"
  }
  ace {
    type = "mask"
    permissions  = "rwx"
  }
}