resource "azurerm_data_factory" "project_adf" {
  name                = "adf-${var.PROJECT_NAME}-01"
  location            = azurerm_resource_group.project.location
  resource_group_name = azurerm_resource_group.project.name
}
