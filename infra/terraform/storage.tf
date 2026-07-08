resource "azurerm_storage_account" "hermes_lakehouse" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = var.storage_account_replication_type

  is_hns_enabled = true

  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = true

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  tags = local.tags

}

# Defines the storage containers (bronze, silver, gold, etc...) and makes private
resource "azurerm_storage_container" "hermes_lakehouse_containers" {
  for_each = toset(local.adls_containers)

  name                  = each.key
  storage_account_id    = azurerm_storage_account.hermes_lakehouse.id
  container_access_type = "private"
}

# This resource allows ADF managed identity to read and write lakehouse storage
resource "azurerm_role_assignment" "adf_storage_blob_contributor" {
  scope                = azurerm_storage_account.hermes_lakehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}