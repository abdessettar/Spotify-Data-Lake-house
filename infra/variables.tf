variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "westeurope"
}

# Separate location for compute resources: westeurope Container Apps was at capacity
# during initial provisioning, so compute runs in northeurope instead.
# It was also the opportunity to apply multi-region deployment for the same project.
variable "compute_location" {
  description = "Azure region for Container Apps (differs from main region bc capacity is limited)."
  type        = string
  default     = "northeurope"
}

variable "resource_group_name" {
  description = "Name of the Azure Resource Group."
  type        = string
  default     = "rg-spotify-de"
}

variable "project" {
  description = "Project identifier. Used in resource naming and tags."
  type        = string
  default     = "spotify-de"
}

variable "environment" {
  description = "Deployment environment label (e.g. prod, dev). Used in resource naming and tags."
  type        = string
  default     = "prod"
}

variable "storage_account_name" {
  description = "Globally unique Azure Storage Account name (3-24 chars, lowercase alphanumeric only, no hyphens)."
  type        = string
  default     = "stspotifydeprod"
}

variable "acr_name" {
  description = "Globally unique Azure Container Registry name (5-50 chars, alphanumeric only, no hyphens)."
  type        = string
  default     = "crspotifydeprod"
}

variable "container_name" {
  description = "Blob container name. Must match DATA_CONTAINER in the application .env file."
  type        = string
  default     = "spotify-data"
}

variable "key_vault_name" {
  description = "Globally unique Key Vault name (3-24 chars, alphanumeric and hyphens)."
  type        = string
  default     = "kv-spotify-de-prod"
}

variable "image_tag" {
  description = "Docker image tag to deploy (e.g., latest, 0.1.0)."
  type        = string
  default     = "latest"
}

variable "container_cpu" {
  description = "CPU allocation for container app (e.g., 1, 2)."
  type        = string
  default     = "1"
}

variable "container_memory" {
  description = "Memory allocation for container app (e.g., 2Gi, 4Gi)."
  type        = string
  default     = "2Gi"
}

variable "tags" {
  description = "Resource tags applied to all Azure resources."
  type        = map(string)
  default = {
    project    = "spotify-de"
    managed_by = "terraform"
  }
}
