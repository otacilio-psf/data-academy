resource "azurerm_data_factory" "project_adf" {
  name                = "adf-${var.project_name}-01"
  location            = azurerm_resource_group.project.location
  resource_group_name = azurerm_resource_group.project.name
}
