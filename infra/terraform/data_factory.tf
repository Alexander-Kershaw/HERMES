
# ADF gets a managed identity so it can lated get access to ADLS

resource "azurerm_data_factory" "main" {
  name                = "adf-${local.name_prefix}-${random_string.rand_suffix.result}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

