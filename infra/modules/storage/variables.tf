variable "resource_group_name" {
  description = "Name of the resource group to deploy into."
  type        = string
}

variable "location" {
  description = "Azure region for the storage account."
  type        = string
}

variable "storage_account_name" {
  description = "Globally unique storage account name (3-24 chars, lowercase alphanumeric only)."
  type        = string
}

variable "container_name" {
  description = "Blob container name for the data lake."
  type        = string
  default     = "spotify-data"
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
  default     = {}
}
