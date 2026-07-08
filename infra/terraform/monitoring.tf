resource "azurerm_log_analytics_workspace" "main" {
  name                = "log-${local.name_prefix}-${random_string.rand_suffix.result}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  sku               = "PerGB2018" # pricing tier for log analytics, standard pay per GB
  retention_in_days = 30

  tags = local.tags
}