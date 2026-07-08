variable "project_name" {
  description = "Portfolio project name used for naming conventions with the Azure resources"
  type        = string
  default     = "hermes"
}

variable "environment" {
  description = "The deployment environment"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "The Azure region set for all resources"
  type        = string
  default     = "uksouth"
}

variable "resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = "rg-hermes-dev-uksouth"
}

#==================================================================================================================================

# Locally Redundant Storage (LRS) is the decided storage replication type
# it copies the data three times within
# a single physical datacenter in the chosen primary region. It is the 
# most affordable replication option on Microsoft Azure. It protects 
# data against hardware issues like server, rack, or drive failures.

#==================================================================================================================================

variable "storage_account_replication_type" {
  description = "Storage replication type"
  type        = string
  default     = "LRS"
}

#==================================================================================================================================

# The Azure Databricks SKU (Stock Keeping Unit) is a pricing and feature
# tier that determines the workspace capabilities. It defines the specific 
# set of tools available for data engineering and AI workloads. 
# Chosen premium tier that provides the core features, and extensive
# data governance tools. and standard tier is depreciated as of 2026

#==================================================================================================================================

variable "databricks_sku" {
  description = "Azure Databricks workspace SKU"
  type        = string
  default     = "premium"
}