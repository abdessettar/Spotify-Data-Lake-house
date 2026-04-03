output "container_app_environment_id" {
  description = "Full resource ID of the Container Apps environment."
  value       = azurerm_container_app_environment.main.id
}

output "log_analytics_workspace_id" {
  description = "Full resource ID of the Log Analytics Workspace."
  value       = azurerm_log_analytics_workspace.main.id
}

output "transform_job_id" {
  description = "Full resource ID of the transformation job."
  value       = azurerm_container_app_job.transform_hourly.id
}

output "transform_job_name" {
  description = "Transformation job resource name (hourly at :20)."
  value       = azurerm_container_app_job.transform_hourly.name
}

output "ingest_job_id" {
  description = "Full resource ID of the scheduled ingestion job."
  value       = azurerm_container_app_job.ingest_hourly.id
}

output "ingest_job_name" {
  description = "Scheduled ingestion job resource name (hourly at :05)."
  value       = azurerm_container_app_job.ingest_hourly.name
}
