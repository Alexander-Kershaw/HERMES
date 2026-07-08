resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.name_prefix}-${random_string.rand_suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = var.databricks_sku

  tags = local.tags
}
