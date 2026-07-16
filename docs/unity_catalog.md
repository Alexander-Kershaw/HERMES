# HERMES: Unity Catalog

For HERMES the use of Azure Databricks premium warrants the use of Unity Catalog since this is automatically enabled for workspaces. Unity Catalog operarates underneath every data interaction within the workspace, typically enforcing access control when querying tables or accessing an AI model.

Unity Catalog also tracks linage as data is being used, logs activity for auditing, amonst many other things, and is a general enterprise data governance layer.

Every asset that is governed in Unity Catalog is modeled as a securable object, which is an object on which permissions can be granted to users, service principals or groups. Data assets such as tables (amonst many other things) follow the namespace `catalog.schema.object`. Tables can be managed, where Unity Catalog handles the governance and file storage lifecycle. Objects such as storage credentials and external locations sit under the metastore.

Overall, Unity Catalog is now necessary for Databricks and is also a very beneficial data governance signal for this portfolio. Therefore, before the registration of Silver Tables ready for dbt Gold modelling, I am going to implement Unity Catalog.

---

## Implementation 

Unity Catalog requires external tables to use a cloud path covered by a registered external location. External location combine a storage path with a storage credential that authorizes access to that storage path.

In the context of HERMES, I created a Unity Catalog external location for the Silver storage container/path, then register tables under a Unity Catalog catalog/schema.

For silver this looks like:

- Catalog: dbw_hermes_dev_9s5nbox (the catalog automatically made with Azure Databricks)
- Schema: silver
- Tables: dbw_hermes_dev_9s5nbox.silver.customers

And the same for the other tables (orders, stores, promotions, etc...)

Then for the gold layer:

- Schema: gold
- Tables: hermes_uc.gold.dim_customers (and the other dimension tables, etc...)

### 1: Checking the current catalog context

I ran the following in a Databricks SQL query:

```sql
SELECT current_catalog(), current_schema();
```

to get:

```txt
current_catalog() = dbw_hermes_dev_9s5nbox
current_schema()  = default
```

This confirms that the workspace already has a Unity Catalog catalog. So no new catalog is needed. But, an external location is needed.

### 2: Creating An Azure Databricks Access Connector

Creating an Access Connector:

```bash
az databricks access-connector create \
  --name ac-hermes-dev-uksouth \
  --resource-group rg-hermes-dev-uksouth \
  --location uksouth \
  --identity-type SystemAssigned
```

Then getting its managed identity principal ID:

```bash
az databricks access-connector show \
  --name ac-hermes-dev-uksouth \
  --resource-group rg-hermes-dev-uksouth \
  --query identity.principalId \
  --output tsv
```

I like to save the managed identity principal ID to env variables:

```bash
export ACCESS_CONNECTOR_PRINCIPAL_ID=<principal-id>
```

### 3: Granding Access Connected Access to ADLS

The Access Connecter needs access to ADLS by granting it Storage Blob Data Contributor on the ADLS storage account. More specifically, this access connector grant can be specific to individual containers.

I ran the following:

```bash
STORAGE_ACCOUNT_ID=$(az storage account show \
  --name sthermesdev9s5nbox \
  --resource-group rg-hermes-dev-uksouth \
  --query id \
  --output tsv)

az role assignment create \
  --assignee "$ACCESS_CONNECTOR_PRINCIPAL_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "$STORAGE_ACCOUNT_ID"
```

In this circumstance I gave full storage account scope rather than granting Storage Blob Data Contributor permissions on just specific containers, so all the containers are now accessible to the Access Connector.


### 4: Creating a Unity Catalog Storage Credential

Using Databricks SQL, I executed the following:

```sql
CREATE STORAGE CREDENTIAL IF NOT EXISTS hermes_adls_credential
WITH AZURE_MANAGED_IDENTITY
'{{access_connector_resource_id}}';
```

Where `{{access_connector_resource_id}}` is founnd full Azure resource ID for the access connector which if found with:

```bash
az databricks access-connector show \
  --name ac-hermes-dev-uksouth \
  --resource-group rg-hermes-dev-uksouth \
  --query id \
  --output tsv
```


### 5: Creating External Locations

I created an external location at the HERMES storage root in ADLS so it covers both silver and gold storage layers with SQL (can also use databricks in the catelog section and creating credentials and external locations there):

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS hermes_lakehouse_location
URL 'abfss://silver@sthermesdev9s5nbox.dfs.core.windows.net/hermes'
WITH (STORAGE CREDENTIAL hermes_adls_credential);
```

Individual specific locations can be made too, I made on specifically pointing to the gold layer since later dbt will be writing into gold:

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS hermes_gold_location
URL 'abfss://gold@sthermesdev9s5nbox.dfs.core.windows.net/hermes/gold'
WITH (STORAGE CREDENTIAL hermes_adls_credential);
```

### 6: Creating The Catalog and Schemas

```sql
CREATE CATALOG IF NOT EXISTS dbw_hermes_dev_9s5nbox

CREATE SCHEMA IF NOT EXISTS dbw_hermes_dev_9s5nbox.silver

CREATE SCHEMA IF NOT EXISTS dbw_hermes_dev_9s5nbox.gold

```

Note that these queries can be executed within a registration script using PySpark SQL which is what I have elected to do in practice.

Also, depending on the level of ownership the user has, in order to create these objects, grants might need to be permitted.

---

