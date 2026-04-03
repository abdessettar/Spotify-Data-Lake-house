output "storage_account_id" {
  description = "Resource ID of the storage account. Used to scope RBAC role assignments."
  value       = azurerm_storage_account.main.id
}

output "storage_account_name" {
  description = "Storage account name. Set as AZURE_STORAGE_ACCOUNT in the application .env file."
  value       = azurerm_storage_account.main.name
}

output "container_name" {
  description = "Blob container name. Set as DATA_CONTAINER in the application .env file."
  value       = azurerm_storage_container.spotify_data.name
}
