resource "azurerm_databricks_workspace" "project_databricks" {
  name                = "adb-${var.project_name}-01"
  location            = azurerm_resource_group.project.location
  resource_group_name = azurerm_resource_group.project.name
  sku                 = "trial"

}
