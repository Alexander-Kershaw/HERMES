
# Azure storage account names are to be globally unique, lowercase, and not too long, so using random suffix

resource "random_string" "rand_suffix" {
  length  = 7
  upper   = false
  special = false
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
    workload    = "retail-lakehouse"
  }

  storage_account_name = lower(replace("st${var.project_name}${var.environment}${random_string.rand_suffix.result}", "-", ""))

  #==================================================================================================================================

  # landing     = raw upload/drop zone before formal Bronze
  # bronze      = raw ingested data
  # silver      = cleaned trusted data
  # gold        = modelled business data
  # quarantine  = failed records
  # audit       = reports and pipeline run metadata

  #==================================================================================================================================

  adls_containers = [
    "bronze",
    "silver",
    "gold",
    "quarantine",
    "audit",
    "landing"
  ]
}