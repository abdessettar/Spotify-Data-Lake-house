resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

module "storage" {
  source = "./modules/storage"

  resource_group_name  = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location
  storage_account_name = var.storage_account_name
  container_name       = var.container_name
  tags                 = var.tags
}

module "registry" {
  source = "./modules/registry"

  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  acr_name            = var.acr_name
  tags                = var.tags
}

module "identity" {
  source = "./modules/identity"

  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  project             = var.project
  environment         = var.environment
  storage_account_id  = module.storage.storage_account_id
  acr_id              = module.registry.acr_id
  key_vault_name      = var.key_vault_name
  tags                = var.tags
}

# Compute may deploy to a different region than the rest of the infrastructure.
# var.compute_location defaults to northeurope because westeurope Container Apps
# was at capacity during initial provisioning.
module "compute" {
  source = "./modules/compute"

  resource_group_name  = azurerm_resource_group.main.name
  location             = var.compute_location
  project              = var.project
  environment          = var.environment
  acr_login_server     = module.registry.acr_login_server
  storage_account_name = module.storage.storage_account_name
  container_name       = module.storage.container_name
  key_vault_uri        = module.identity.key_vault_uri
  managed_identity_id        = module.identity.identity_id
  managed_identity_client_id = module.identity.identity_client_id
  image_tag            = var.image_tag
  cpu                  = var.container_cpu
  memory               = var.container_memory
  tags                 = var.tags
}
