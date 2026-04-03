output "acr_login_server" {
  description = "ACR login server URL (e.g. crspotifydeprod.azurecr.io). Use as the image registry for docker push and container deployments."
  value       = azurerm_container_registry.main.login_server
}

output "acr_id" {
  description = "Full resource ID of the container registry."
  value       = azurerm_container_registry.main.id
}

output "acr_name" {
  description = "ACR resource name. Use with: az acr login --name <acr_name>"
  value       = azurerm_container_registry.main.name
}
