variable "resource_group_name" {
  description = "Name of the resource group to deploy into."
  type        = string
}

variable "location" {
  description = "Azure region for the managed identity."
  type        = string
}

variable "project" {
  description = "Project identifier. Used to construct the identity resource name."
  type        = string
}

variable "environment" {
  description = "Environment label. Used to construct the identity resource name."
  type        = string
}

variable "storage_account_id" {
  description = "Resource ID of the storage account. The managed identity is granted Storage Blob Data Contributor on this scope."
  type        = string
}

variable "acr_id" {
  description = "Resource ID of the container registry. The managed identity is granted AcrPull on this scope."
  type        = string
}

variable "key_vault_name" {
  description = "Globally unique Key Vault name (3-24 chars, alphanumeric and hyphens)."
  type        = string
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
  default     = {}
}
