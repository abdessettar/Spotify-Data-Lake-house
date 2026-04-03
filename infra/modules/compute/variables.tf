variable "resource_group_name" {
  description = "Name of the resource group to deploy into."
  type        = string
}

variable "location" {
  description = "Azure region for the Container Apps environment."
  type        = string
}

variable "project" {
  description = "Project identifier. Used in resource naming."
  type        = string
}

variable "environment" {
  description = "Environment label. Used in resource naming."
  type        = string
}

variable "acr_login_server" {
  description = "ACR login server URL (e.g., crspotifydeprod.azurecr.io)."
  type        = string
}

variable "storage_account_name" {
  description = "Storage account name for the data lake."
  type        = string
}

variable "container_name" {
  description = "Blob container name."
  type        = string
  default     = "spotify-data"
}

variable "key_vault_uri" {
  description = "Key Vault URI for secret retrieval."
  type        = string
}

variable "managed_identity_id" {
  description = "Full resource ID of the user-assigned managed identity."
  type        = string
}

variable "managed_identity_client_id" {
  description = "Client ID of the user-assigned managed identity. Set as AZURE_CLIENT_ID to hint DefaultAzureCredential."
  type        = string
}

variable "image_tag" {
  description = "Docker image tag (e.g., latest, 0.1.0)."
  type        = string
  default     = "latest"
}

variable "cpu" {
  description = "CPU allocation for the container (e.g., '1', '2')."
  type        = string
  default     = "1"
}

variable "memory" {
  description = "Memory allocation for the container (e.g., '2Gi', '4Gi')."
  type        = string
  default     = "2Gi"
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
  default     = {}
}
