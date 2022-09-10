resource "azurerm_key_vault_secret" "datalake_access_sp_app_id" {
  name         = "sp-datalake-app-id"
  value        = azuread_application.dl_contributor.application_id
  key_vault_id = azurerm_key_vault.akv.id

  depends_on = [
    azurerm_key_vault_access_policy.deployer
  ]
}

resource "azurerm_key_vault_secret" "datalake_access_sp_obj_id" {
  name         = "sp-datalake-obj-id"
  value        = azuread_service_principal.dl_contributor.object_id
  key_vault_id = azurerm_key_vault.akv.id

  depends_on = [
    azurerm_key_vault_access_policy.deployer
  ]
}

resource "azurerm_key_vault_secret" "datalake_access_sp_secret" {
  name         = "sp-datalake-secret"
  value        = azuread_application_password.dl_contributor.value
  key_vault_id = azurerm_key_vault.akv.id

  depends_on = [
    azurerm_key_vault_access_policy.deployer
  ]
}

resource "azurerm_key_vault_secret" "datalake_access_sp_tenant" {
  name         = "sp-datalake-tenant"
  value        = var.TENANT_ID
  key_vault_id = azurerm_key_vault.akv.id

  depends_on = [
    azurerm_key_vault_access_policy.deployer
  ]
}