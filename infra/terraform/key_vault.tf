data "azurerm_client_config" "current_client_config" {}

resource "azurerm_key_vault" "main" {
  name                       = "kv-${local.name_prefix}-${random_string.rand_suffix.result}"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  tenant_id                  = data.azurerm_client_config.current_client_config.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = false
  soft_delete_retention_days = 7
  rbac_authorization_enabled = true
  tags                       = local.tags
}