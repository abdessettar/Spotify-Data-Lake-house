# Log Analytics Workspace for Container Apps monitoring.
# retention_in_days = 30
resource "azurerm_log_analytics_workspace" "main" {
  name                = "law-${var.project}-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = var.tags
}

# Container Apps Environment (Consumption-only, no dedicated workload profiles)
resource "azurerm_container_app_environment" "main" {
  name                           = "cae-${var.project}-${var.environment}"
  location                       = var.location
  resource_group_name            = var.resource_group_name
  log_analytics_workspace_id     = azurerm_log_analytics_workspace.main.id
  tags                           = var.tags

  workload_profile {
    name                  = "Consumption"
    workload_profile_type = "Consumption"
  }
}


# Scheduled transformation + Gold job (hourly at :20, ingest is completed by then)
resource "azurerm_container_app_job" "transform_hourly" {
  name                         = "job-transform-${var.project}-${var.environment}"
  location                     = var.location
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = var.resource_group_name
  workload_profile_name        = "Consumption"
  # 90 minutes: transform rebuilds Silver, then Gold reads Silver and writes analytics tables.
  replica_timeout_in_seconds   = 5400
  replica_retry_limit          = 0
  tags                         = var.tags

  schedule_trigger_config {
    cron_expression = "20 * * * *"
  }

  template {
    container {
      name    = "spotify-transform"
      image   = "${var.acr_login_server}/spotify-platform:${var.image_tag}"
      # 2 CPU / 4Gi: Transform reads all Bronze data, rebuilds Silver star schema, then
      # Gold joins all Silver dimensions into wide analytics tables (more power than ingest).
      cpu     = 2
      memory  = "4Gi"
      command = ["python", "-m", "src.main", "transform-gold"]

      env {
        name  = "ENVIRONMENT"
        value = "PROD"
      }
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = var.storage_account_name
      }
      env {
        name  = "DATA_CONTAINER"
        value = var.container_name
      }
      env {
        name  = "KEY_VAULT_URI"
        value = var.key_vault_uri
      }
      env {
        name  = "AZURE_CLIENT_ID"
        value = var.managed_identity_client_id
      }
    }
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [var.managed_identity_id]
  }

  registry {
    server   = var.acr_login_server
    identity = var.managed_identity_id
  }
}

# Scheduled ingestion job (hourly at :05)
resource "azurerm_container_app_job" "ingest_hourly" {
  name                         = "job-ingest-${var.project}-${var.environment}"
  location                     = var.location
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = var.resource_group_name
  workload_profile_name        = "Consumption"
  replica_timeout_in_seconds   = 1800
  replica_retry_limit          = 1
  tags                         = var.tags

  schedule_trigger_config {
    cron_expression = "5 * * * *"
  }

  template {
    container {
      name    = "spotify-ingest"
      image   = "${var.acr_login_server}/spotify-platform:${var.image_tag}"
      cpu     = 1
      memory  = "2Gi"
      command = ["python", "-m", "src.main", "ingest"]

      env {
        name  = "ENVIRONMENT"
        value = "PROD"
      }
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = var.storage_account_name
      }
      env {
        name  = "DATA_CONTAINER"
        value = var.container_name
      }
      env {
        name  = "KEY_VAULT_URI"
        value = var.key_vault_uri
      }
      env {
        name  = "AZURE_CLIENT_ID"
        value = var.managed_identity_client_id
      }
    }
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [var.managed_identity_id]
  }

  registry {
    server   = var.acr_login_server
    identity = var.managed_identity_id
  }
}
