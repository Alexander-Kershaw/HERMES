# Azure + Databricks CLI Commands for HERMES

This document records the Azure CLI and Databricks CLI commands used during the development of Hermes.

Workflow covered:
- Azure login and subscription selection
- Terraform output extraction
- ADLS Gen2 storage access
- Azure RBAC assignments
- Service principle setups
- Databricks CLI profile authentication
- Databricks secret scope management
- ADLS test script verification

---

### Azure CLI Authentication

Log in with 

```bash
az login
```

To show the currently active Azure account:

```bash
az account show --output table
```

To list avaliable subscriptions:
```bash
az account list --output table
```

Set the active subscription:
```bash
az account set --subscription "<subscription-id-or-name>"
```

Show useful account details:

```bash
az account show \
  --query "{subscriptionId:id, tenantId:tenantId, user:user.name}" \
  --output table
```


---


### Terraform Output Extraction

Terraform to be used in a dedicated terraform directory where it has been initiated:

```bash
cd/infra/terraform
```

Get the ADLS Gen2 storage account name

```bash
terraform output -raw storage_account_name
```

Get the Azure data factory name:

```bash
terraform output -raw data_factory_name
```

Get the Azure Databricks workspace URL:

```bash
terraform output -raw databricks_workspace_url
```

Or can just output the values as defined in the full terraform outputs.tf file:

```bash
terraform output
```

Outputs can be assigned as shell variables for later commands, eg:

```bash
export HERMES_STORAGE_ACCOUNT=$(terraform output -raw storage_account_name)
export HERMES_ADF_NAME=$(terraform output -raw data_factory_name)
export HERMES_DATABRICKS_URL="https://$(terraform output -raw databricks_workspace_url)"
```

Check the shell variable values with echo:

```bash
echo "$HERMES_STORAGE_ACCOUNT"
echo "$HERMES_ADF_NAME"
echo "$HERMES_DATABRICKS_URL"
```

---

### Core Hermes Azure Variables

Set Hermes resource group:

```bash
export HERMES_RG="rg-hermes-dev-uksouth"
```

Get the storage account resource ID:

```bash
export HERMES_STORAGE_ID=$(az storage account show \
  --name "$HERMES_STORAGE_ACCOUNT" \
  --resource-group "$HERMES_RG" \
  --query id \
  --output tsv)
```

Then verify its value:

```bash
echo "$HERMES_STORAGE_ID"
```

---

### Azure Storage / ADLS Commands

List the storage containers using microsift entra authentication:

```bash
az storage container list \
  --account-name "$HERMES_STORAGE_ACCOUNT" \
  --auth-mode login \
  --output table
```

For uploading raw data to the ADLS landing container:

```bash
az storage blob upload-batch \
  --account-name "$HERMES_STORAGE_ACCOUNT" \
  --destination landing \
  --destination-path hermes/raw \
  --source data/sample/raw \
  --auth-mode login \
  --overwrite
```

List all blobs under the uploaded path:

```bash
az storage blob list \
  --account-name "$HERMES_STORAGE_ACCOUNT" \
  --container-name landing \
  --prefix hermes/raw \
  --auth-mode login \
  --output table
```

List only blob names under the uploaded raw path:

```bash
az storage blob list \
  --account-name "$HERMES_STORAGE_ACCOUNT" \
  --container-name landing \
  --prefix hermes/raw \
  --auth-mode login \
  --query "[].name" \
  --output table
```

Verify the Delta smoke test output is in the silver container:

```bash
az storage blob list \
  --account-name "$HERMES_STORAGE_ACCOUNT" \
  --container-name silver \
  --prefix hermes/smoke_test/orders_delta \
  --auth-mode login \
  --output table
```

---

### Azure RBAC: developer user permissions

Get the signed-in Azure user object ID:

```bash
export HERMES_USER_OBJECT_ID=$(az ad signed-in-user show \
  --query id \
  --output tsv)
```

Assign the signed-in user the Storage Blob Data Contributor roles on the Hmermes storage account:

```bash
az role assignment create \
  --assignee "$HERMES_USER_OBJECT_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "$HERMES_STORAGE_ID"
```

List role assignments on the storage account:

```bash
az role assignment list \
  --scope "$HERMES_STORAGE_ID" \
  --output table
```

Can list only specific role assignments (Storage Blob Data Contributor in this case) with: 

```bash
az role assignment list \
  --scope "$HERMES_STORAGE_ID" \
  --query "[?roleDefinitionName=='Storage Blob Data Contributor'].[principalName, principalType, roleDefinitionName]" \
  --output table
```

---

### Azure RBAC: Azure Data Factory Managed Identity 

Get the Azure data factory managed identity principle ID:

```bash
export HERMES_ADF_PRINCIPAL_ID=$(az datafactory show \
  --name "$HERMES_ADF_NAME" \
  --resource-group "$HERMES_RG" \
  --query identity.principalId \
  --output tsv)
```

Then can assign Azure data factory the Storage Blob Data Contributor role on the Hermes storage account:

```bash
az role assignment create \
  --assignee "$HERMES_ADF_PRINCIPAL_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "$HERMES_STORAGE_ID"
```

---


### Azure Service Principal For Databricks ADLS Access

Create a service principal for Databricks to allow it ADLS access:

```bash
export HERMES_SP_NAME="sp-hermes-dev-databricks-adls"

az ad sp create-for-rbac \
  --name "$HERMES_SP_NAME" \
  --role "Storage Blob Data Contributor" \
  --scopes "$HERMES_STORAGE_ID"
```

The output contains:

```text
appId -> Databricks secret: adls-client-id
password -> Databricks secret: adls-client-secret
tenant -> Databricks secret: tenant-id
```

Keep these values somewhere.

Then find the service principal later by display name:

```bash
az ad sp list \
  --display-name "sp-hermes-dev-databricks-adls" \
  --query "[].{displayName:displayName, appId:appId, objectId:id}" \
  --output table
```

Set the serivce principal app/client ID:

```bash
export HERMES_SP_APP_ID="<appId>"
```

Get the service principal object ID:

```bash
export HERMES_SP_OBJECT_ID=$(az ad sp show \
  --id "$HERMES_SP_APP_ID" \
  --query id \
  --output tsv)
```

Check the service principal role assignments:

```bash
az role assignment list \
  --assignee "$HERMES_SP_OBJECT_ID" \
  --scope "$HERMES_STORAGE_ID" \
  --query "[].{role:roleDefinitionName, scope:scope}" \
  --output table
```

Assign the service principle storage access if its missing with:

```bash
az role assignment create \
  --assignee "$HERMES_SP_OBJECT_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "$HERMES_STORAGE_ID"
```

---

### Reset The Service Principal Secret (If Needed)

If the original service principal password was lost are incorrectly inputted, a fresh client secret can be generated with:

```bash
az ad app credential reset \
  --id "$HERMES_SP_APP_ID" \
  --display-name "hermes-dev-databricks-adls-secret" \
  --query "{appId:appId, password:password, tenant:tenant}" \
  --output json
```


---


### Service Principal Login Test

To test if the service principal credentials are valid:

```bash
az login \
  --service-principal \
  --username "<appId>" \
  --password "<password>" \
  --tenant "<tenant>"
```

Verify a storage access token exists:

```bash
az account get-access-token \
  --resource https://storage.azure.com/ \
  --query "{expiresOn:expiresOn, tenant:tenant, tokenType:tokenType}" \
  --output table
```

If test is successful, log back in as the normal user:

```bash
az login
az account set --subscription "<subscription-id>"
```

---

### Databricks CLI Installation and Version Check

Check Databricks CLI version with:

```bash
databricks version
```

Install databricks CLI if needed:

```bash
brew tap databricks/tap
brew install databricks
```

---

### Databricsk CLI Profile Setup

Set the profile name with:

```bash
export HERMES_DATABRICKS_PROFILE="hermes-azure"
```

Authenticate the Databricks CLI against the Azure Databricks workspace:

```bash
databricks auth login \
  --host "$HERMES_DATABRICKS_URL" \
  --profile "$HERMES_DATABRICKS_PROFILE"
```

Check the avaliable authenticated profiles:

```bash
databricks auth profiles
```

Verify the profile works with:

```bash
databricks current-user me \
  -p "$HERMES_DATABRICKS_PROFILE"
```

---


### Databricks CLI Config Debugging

Can inspect with databricks config file with:

```bash
cat ~/.databrickscfg
```

Config file can be edited in IDE or with something like nano.

Editing the databricks config file can help define the hermes databricks profiles and which ones are defaulted.

Sometimes databricks related environment variables want to be reassigned. So the check the databricks environment variables use:

```bash
env | grep -i DATABRICKS
```

If bad variables are set they can be unassigned with something similar to this:

```bash
unset DATABRICKS_HOST
unset DATABRICKS_TOKEN
unset DATABRICKS_CONFIG_PROFILE
unset DATABRICKS_CLIENT_ID
unset DATABRICKS_CLIENT_SECRET
```

---

### Databricks Secrets Setup

To create a databricks backed secret scope use:

```bash
databricks secrets create-scope hermes-dev \
  -p "$HERMES_DATABRICKS_PROFILE"
```

List the secret scopes with:

```bash
databricks secrets list-scopes \
  -p "$HERMES_DATABRICKS_PROFILE"
```

---

### Databricks Manual Secret Setting

Add the service principle app/client ID:

```bash
databricks secrets put-secret hermes-dev adls-client-id \
  -p "$HERMES_DATABRICKS_PROFILE"
```

Also add the service principal client secret value (password):

```bash
databricks secrets put-secret hermes-dev adls-client-secret \
  -p "$HERMES_DATABRICKS_PROFILE"
```

Also add the tenant ID:

```bash
databricks secrets list-secrets hermes-dev \
  -p "$HERMES_DATABRICKS_PROFILE"
```

### Typical Terraform Commands Used

Initialize Terraform inside a dedicated terraform directory with:

```bash
terraform init
```

Format Terraform files with:

```bash
terraform fmt
```

Validate configuration with:

```bash
terraform validate
```

Create a Terraform plan with:

```bash
terraform plan -out=tfplan
```

Apply the plan:

```bash
terraform apply tfplan
```

Show the Terraform outputs:

```bash
terraform output
```

Destroy environment when no longer needed:

```bash
terraform destroy
```

---




