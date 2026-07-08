output "resource_group_name" {
  description = "Azure resource group name."
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = azurerm_storage_account.hermes_lakehouse.name
}

output "storage_container_names" {
  description = "Hermes lakehouse container names."
  value       = [for container in azurerm_storage_container.hermes_lakehouse_containers : container.name]
}

output "data_factory_name" {
  description = "Azure Data Factory name."
  value       = azurerm_data_factory.main.name
}

output "databricks_workspace_name" {
  description = "Azure Databricks workspace name."
  value       = azurerm_databricks_workspace.main.name
}

output "databricks_workspace_url" {
  description = "Azure Databricks workspace URL"
  value       = azurerm_databricks_workspace.main.workspace_url
}

output "key_vault_name" {
  description = "Key Vault name."
  value       = azurerm_key_vault.main.name
}

output "log_analytics_workspace_name" {
  description = "Log analytics workspace name."
  value       = azurerm_log_analytics_workspace.main.name
}