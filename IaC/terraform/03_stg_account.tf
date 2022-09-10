resource "azurerm_storage_account" "stga_datalake" {
  name                     = "adls2${replace(var.PROJECT_NAME, "-", "")}01"
  resource_group_name      = azurerm_resource_group.project.name
  location                 = azurerm_resource_group.project.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
  
  tags = {
    scope = "analytics"
  }

}

resource "azurerm_storage_data_lake_gen2_filesystem" "datalake" {
  name               = "datalake"
  storage_account_id = azurerm_storage_account.stga_datalake.id
}

resource "azurerm_role_assignment" "rbac_dl_contributor" {
  scope                = azurerm_storage_account.stga_datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.dl_contributor.object_id
}
