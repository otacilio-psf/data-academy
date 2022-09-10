resource "azurerm_key_vault" "akv" {
  name                       = "akv-${var.PROJECT_NAME}-01"
  location                   = azurerm_resource_group.project.location
  resource_group_name        = azurerm_resource_group.project.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7

  tags = {
    scope = "analytics"
  }

}

resource "azurerm_key_vault_access_policy" "deployer" {
  key_vault_id = azurerm_key_vault.akv.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  key_permissions = [
    "Get", "List", "Update", "Create", "Import", "Delete", "Recover", "Backup", "Restore",
  ]

  secret_permissions = [
    "Set", "Get", "List", "Delete", "Purge", "Recover", "Restore",
  ]

}
