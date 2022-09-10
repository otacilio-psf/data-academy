provider "azuread" {}

data "azuread_client_config" "current" {}

resource "azuread_application" "dl_contributor" {
  display_name = "${var.PROJECT_NAME}-sp-datalake"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "dl_contributor" {
  application_id               = azuread_application.dl_contributor.application_id
  app_role_assignment_required = false
  owners                       = [data.azuread_client_config.current.object_id]
}

resource "azuread_application_password" "dl_contributor" {
  application_object_id = azuread_application.dl_contributor.object_id
  end_date_relative    = "8765h"
}


output "app_id" {
  value       = azuread_application.dl_contributor.application_id
  description = "App ID"
}

output "obj_id" {
  value       = azuread_service_principal.dl_contributor.object_id
  description = "Obj ID"
}