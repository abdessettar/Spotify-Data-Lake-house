resource "azurerm_container_registry" "main" {
  name                = var.acr_name
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "Basic"
  admin_enabled       = false # We use `az acr login` with personal credentials or a managed identity for pushes.
  tags                = var.tags
}
