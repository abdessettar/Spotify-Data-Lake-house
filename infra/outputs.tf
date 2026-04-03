output "resource_group_name" {
  description = "Name of the provisioned resource group."
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "Storage account name. Set as AZURE_STORAGE_ACCOUNT in the application .env file."
  value       = module.storage.storage_account_name
}

output "container_name" {
  description = "Blob container name. Set as DATA_CONTAINER in the application .env file."
  value       = module.storage.container_name
}

output "acr_login_server" {
  description = "ACR login server URL. Use as the image registry for docker push and container deployments."
  value       = module.registry.acr_login_server
}

output "acr_name" {
  description = "ACR resource name. Use with: az acr login --name <acr_name>"
  value       = module.registry.acr_name
}

output "managed_identity_id" {
  description = "Full resource ID of the User-Assigned Managed Identity. Assign this to ACI / ACA / AKS workloads."
  value       = module.identity.identity_id
}

output "managed_identity_client_id" {
  description = "Client ID of the Managed Identity. Optionally set as AZURE_CLIENT_ID in the container environment to hint DefaultAzureCredential."
  value       = module.identity.identity_client_id
}

output "key_vault_uri" {
  description = "Key Vault URI. Use as vault_url in SecretClient: SecretClient(vault_url=<uri>, credential=DefaultAzureCredential())"
  value       = module.identity.key_vault_uri
}

output "transform_job_name" {
  description = "Hourly transformation job name (runs at :20). Start manually with: az containerapp job start --name <name> --resource-group <rg>"
  value       = module.compute.transform_job_name
}

output "ingest_job_name" {
  description = "Hourly ingestion job name (runs at :05). Start manually with: az containerapp job start --name <name> --resource-group <rg>"
  value       = module.compute.ingest_job_name
}
