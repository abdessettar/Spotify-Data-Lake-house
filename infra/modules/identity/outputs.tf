output "identity_id" {
  description = "Full resource ID of the User-Assigned Managed Identity. Assign this to ACI / ACA / AKS workloads."
  value       = azurerm_user_assigned_identity.main.id
}

output "identity_client_id" {
  description = "Client ID of the Managed Identity. Optionally set as AZURE_CLIENT_ID in the container environment to hint DefaultAzureCredential."
  value       = azurerm_user_assigned_identity.main.client_id
}

output "identity_principal_id" {
  description = "Principal (object) ID of the Managed Identity. Useful for auditing RBAC assignments."
  value       = azurerm_user_assigned_identity.main.principal_id
}

output "key_vault_id" {
  description = "Full resource ID of the Key Vault."
  value       = azurerm_key_vault.main.id
}

output "key_vault_uri" {
  description = "Key Vault URI (e.g. https://kv-spotify-de-prod.vault.azure.net). Use as the vault_url in SecretClient."
  value       = azurerm_key_vault.main.vault_uri
}
