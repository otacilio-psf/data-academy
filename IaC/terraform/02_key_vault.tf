resource "azurerm_key_vault" "akv" {
  name                       = "kv-${var.project_name}-01"
  location                   = azurerm_resource_group.project.location
  resource_group_name        = azurerm_resource_group.project.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Create",
      "Get",
    ]

    secret_permissions = [
      "Set",
      "Get",
      "List",
      "Delete",
      "Purge",
      "Recover"
    ]
  }
}

resource "azurerm_key_vault_secret" "datalake_access_sp_id" {
  name         = "sp-academy-id"
  value        = var.SP_ACADEMY_ID
  key_vault_id = azurerm_key_vault.akv.id
}


resource "azurerm_key_vault_secret" "datalake_access_sp_secret" {
  name         = "sp-academy-secret"
  value        = var.SP_ACADEMY_SECRET
  key_vault_id = azurerm_key_vault.akv.id
}

resource "azurerm_key_vault_secret" "datalake_access_sp_tenant" {
  name         = "sp-academy-tenant"
  value        = var.TENANT_ID
  key_vault_id = azurerm_key_vault.akv.id
}
