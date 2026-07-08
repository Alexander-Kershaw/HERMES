# HERMES Terraform Infrastructure

This directory contains Terraform infrastructure for the HERMES Azure batch lakehouse.

## Resources Provisioned

The initial batch infrastructure provisions I have implemented include:

- Resource Group
- ADLS Gen2 storage account
- Lakehouse containers: landing, bronze, silver, gold, quarantine, audit
- Azure Data Factory
- Azure Databricks workspace
- Azure Key Vault
- Log Analytics workspace

## Authentication

Local Terraform execution uses Azure CLI authentication.

```bash
az login
az account set --subscription "<subscription-id>"
```

## Terraform Usage

Within the terraform directory, I execute the following:

```bash
cd infra/terraform
terraform init
terraform fmt
terraform validate
terraform plan -out=tfplan
terraform apply tfplan
```

Then to avoid any unwanted Azure costs I destroy the infrastructure with:

```bash
terraform destroy
```

## Cost Control Considerations

The terraform infrastructure is designed as a development environment rather than a full blown data engineering deployment.

I am keeping Databricks clusters terminated when not in use and destroying environments when testing is completed.

---