# Event-001: Project Start

## Date

2026-07-01

## Summary

I have started HERMES, a retail lakehouse project designed to demonstrate Azure Databricks data engineering capability.

The project will simulate a retail data modernisation programme using batch and streaming data pipelines, Delta Lake medallion architecture, PySpark, dbt, Terraform, and Azure services.

## Why this project exists

The project aims to evidence:

- Azure data engineering
- Databricks
- PySpark and Spark SQL
- Kafka streaming
- Delta Lake
- data warehousing and lakehouse modelling
- CI/CD and infrastructure as code
- data quality and governance
- retail data platform experience

## Initial decision

The project will be built locally first, then deployed to Azure in progressive stages.

This reduces cost risk and avoids building a fragile cloud architecture before the core transformations and data model are understood.

## Initial architecture direction

The platform will use:

- synthetic retail source data
- local lakehouse storage during early development
- PySpark for Bronze to Silver transformations
- dbt for Silver to Gold modelling
- Delta Lake for table storage
- local Kafka streaming before Azure Event Hubs
- Azure Data Factory for batch orchestration
- Azure Databricks for managed Spark processing
- Terraform for infrastructure
- GitHub Actions for CI/CD

## Trade-offs noted

### Local-first vs cloud-first

Local-first is slower but safer and more disciplined. It ensures that the core engineering logic works before cloud infrastructure is introduced and avoid any expensive mistakes.

### PySpark and dbt

PySpark will handle ingestion, cleansing, conformance, and streaming heavy processing. dbt will handle governed SQL modelling, tests, documentation, and Gold layer marts.

### Synthetic data vs public dataset

Synthetic data gives control over schema, volume, and business scenarios. 

## Next step

Create the project tooling files, install development dependencies, run basic checks, and commit the foundation.

---

# Event-002: Batch Source Data Generation

## Date

2026-07-02

## Summary

I completed an initial version of the batch source data generation.

The first batch source files represent the proceeding elements of retail data:

- customers
- stores
- products
- orders
- order items
- product inventory snapshots
- product promotions

The generator uses controlled random seeds to make the local development repeatable and reproducible, and to initially limit the scope of complexity.

This data is intially and intentionally generated locally before introducing Spark, Delta Lake, or Azure. This is to establish the underlying data engineering architecture and relationships first before using resources without solid design foundations.

---

# Event-003: Bronze Ingestion

## Date

2026-07-02

## Summary

I added a local bronze ingestion workflow. This pipeline reads the synthetic retail source data (as CSV files) and writes local bronze parquet outputs with accompanying ingestion metadata

### Sources Ingested

- customers
- stores
- products
- orders
- order_items
- inventory_snapshots
- promotions

### Bronze metadata columns

Each Bronze table includes:

- _bronze_source_name
- _bronze_source_file
- _bronze_source_path
- _bronze_ingested_datetime
- _bronze_ingestion_date


### Design Decisions

The sources files remain as CSV to simulate typical legacy operational batch data extractions.

The bronze layer writes parquet to provde a more efficient columnar formate before later introducing Delta Lake for the silver and gold layers.

**Note:** Delta Lake is the ultimate target lakehouse format, but I did not want to introduce it instantaneously as the first ingestion step as this would force Spark into the project before confirmation that the source layer is stable. Therefore, parquet bronze is an appropriately pragmatic intermediate step before augmenting the complexity of this project.

Also, the raw source paths are being kept. The presence of the source metadata support data lineage, auditability, and important debugging. Keeping the source metadata and sources present will be helpful when bad records appear downstream, so it would be useful to identify the source of bad records. These malformed records will be intentionally injected as the project progresses.

---

# Event-004: Silver Layer

## Summary

Added the first local Silver transformation layer using PySpark and Delta Lake.

The pipeline reads Bronze Parquet tables, applies explicit schemas, deduplicates records by primary key, adds Silver metadata, and writes Delta tables locally.

## Tables transformed

- customers
- stores
- products
- orders
- order_items
- inventory_snapshots
- promotions

## Design decision

PySpark is used for Bronze to Silver transformations because this layer represents data engineering heavy processing.

dbt will be introduced later for Silver to Gold modelling, where SQL business logic, documentation, and tests are more appropriate.

## Trade offs

### Explicit schemas vs inferred schemas

Explicit schemas are more verbose but definitely safer as opposed to an infered schema.

They make the expected shape of each table clear and reduce any nasty surprises when source files change.

### Overwrite mode vs incremental mode

The first version uses overwrite mode for simplicity.

Incremental processing will be introduced later once data contracts, quality rules, and partition strategy are clearer and everything has taken more shape.

### Local Delta before Azure Delta

Local Delta provides the lakehouse table semantics before Azure Databricks is introduced.

This keeps cloud cost low while preserving my architectural direction.

---

# Event-005: Silver Data Quality Contracts

## Summary

Added the first silver layer data quality validation framework.

The framework uses YAML contracts to define expected rules for silver tables and PySpark to validate those rules against local Delta tables.

## Rules implemented

- not_null
- unique
- min_value
- accepted_values
- regex

## Tables covered

All of them:

- customers
- stores
- products
- orders
- order_items
- inventory_snapshots
- promotions

## Design decision

A custom YAML in conjunction with PySpark validation framework was selected instead of immediately using Great Expectations or Soda.

This keeps the framework transparent and easy to explain while still demonstrating governance, validation, and auditability.

Great Expectations in particular is very likely to be implemented at a later date.

## Trade offs

### Custom framework

Benefits:

- simple
- transparent
- testable
- easy to adapt

Limitations:

- fewer built-in features
- more custom maintenance
- not as mature as specialist data quality tools

### Validation after Silver

Validation currently runs after silver tables are written.

Later versions may validate before writing trusted Silver outputs and send failed records to quarantine.

---

# Event-006: Silver Table Relationship Validation

## Summary

I added relationship validation for silver tables.

The validation framework now checks that child table keys (foreign keys) exist in parent tables.

## Relationships checked

- `orders.customer_id` and `customers.customer_id`
- `order_items.order_id` and `orders.order_id`
- `order_items.product_id` and `products.product_id`
- `inventory_snapshots.store_id` and `stores.store_id`
- `inventory_snapshots.product_id` and `products.product_id`
- `promotions.product_id` and `products.product_id`

## Design decision

Relationship checks are stored separately from the column rule table contracts.

Column rule contracts describe rules within a table. Whereas, relationship contracts describe integrity rules between tables.

## Implementation detail

Relationship validation uses Spark left anti joins to find orphan key (keys without a corresponding key counterpart in another table that it is being validated against).

## Trade Off

The current implementation reports relationship failures but does not yet quarantine offending records.

So, quarantine handling will be added next.

---

# Event-007: Quarantine Handling

## Summary

Added the first local quarantine mechanism for failed silver validation records.

The quarantine layer captures records that fail column level validation checks and writes them to local Parquet files for inspection.

## Quarantine output

Failed records are written under:

```text
data/quarantine/silver/<table_name>/<rule_name>_<column_name>.parquet
```

For each quarantined record, there are additional quarantine metadata columns:

- `_quarantine_table_name`
- `_quarantine_column_name`
- `_quarantine_rule_name`
- `_quarantine_failed_reason`
- `_quarantine_created_at`
- `_quarantine_created_date`


## Design Choice

I have implemented quarantine after validation rather than embedded direction within the silver transformation logic at this time. This keeps the incremental development simple while allowing validation failures to be inspected and audited as required.

## Trade Offs

This first quarantine implementation only deals with column level validation rule failures as of now. 

The table relation level quarantine will require different logic since the failed records are dependant on cross table ophan key checks

---

# Event-008: Naming Mismatch Corrections

## Calibration note

Initial quarantine testing exposed naming mismatches between generated source fields and contract expectations, which caused excessive quarantine output.

The contracts and generator were corrected so that Silver validation now passes successfully with no quarantine incidents, allowing a smoother transition to dbt gold layer modelling next.

This confirmed that the quarantine mechanism works, but also highlighted the need to distinguish genuine data quality failures from contract calibration issues.

---

# Event-009: dbt Gold Modelling

## Summary

Started the dbt Gold modelling layer.

The dbt project defines staging, intermediate, and mart models for trusted retail analytics.

## Models added

Staging:

- stg_customers
- stg_stores
- stg_products
- stg_orders
- stg_order_items
- stg_inventory_snapshots
- stg_promotions

Intermediate:

- int_order_revenue
- int_inventory_position
- int_promotion_attribution

Marts:

- dim_customer
- dim_product
- dim_store
- fct_sales
- fct_inventory_snapshot
- mart_daily_retail_kpis
- mart_promotion_performance

## Design decision

dbt is used for data modelling from silver to gold layers, not for ingestion or silver data engineering.

PySpark remains responsible for engineering intensive transformations. dbt is responsible for business intelligence models, tests, documentation, and lineage.

## Trade Offs

The dbt project is designed for Databricks execution with dbt-databricks. It is scaffolded locally for now, but full execution will occur once silver delta tables are registered in Databricks.

This avoids forcing dbt onto local delta files in a way that does not represent the target architecture, which would just be a rather fruitless waste of time.

---

# Event-010: Local Batch Orchestration

## Summary

Added a local batch orchestration command for the batch lakehouse pipeline.

The runner executes source generation, Bronze ingestion, Silver transformation, Silver validation, relationship validation, quarantine summary, and dbt parse.

## Design decision

The local runner is intentionally simple and Pythonic.

It acts as a local equivalent of the Azure batch orchestration that will later be implemented with Azure Data Factory and Databricks jobs.

## Trade Offs

The runner uses `dbt parse` rather than `dbt run` because the dbt project is designed to execute against Databricks.

This avoids forcing dbt into an unrealistic local execution setup.

---

# Event-011: Azure Batch Infrastructure

## Summary

Succesfully added the Azure batch infrastructure.

Added Terraform definitions for the first Azure landing zone.

## Resources included

- Resource Group
- ADLS Gen2 storage account
- lakehouse containers
- Azure Data Factory
- Azure Databricks workspace
- Key Vault
- Log Analytics workspace

## Design decision

The infrastructure is for the batch pipeline first.

Streaming resources are intentionally excluded for now to keep the architecture focused and avoid premature complexity.

Initially I wanted to used the standard databricks SKU however this is no longer supported as of 2026 so the databricks workspace uses the premium SKU.

## Trade Offs

The first Terraform implementation is flat rather and intentionally a little sparce for now.

This keeps the infrastructure easy to read and debug during early development. Modules may be introduced later if the infrastructure grows.

---

# Entry-012: Databricks ADLS Smoke Test

## Summary

I validated that Azure Databricks can read from and write to the ADLS Gen2 lakehouse.

The smoke test read uploaded CSV data from the `landing` container, wrote Delta output to the `silver` container, and read the Delta output back successfully. Confirming read and write capabilities fundamantal to the pipeline.

## What was validated

- Azure CLI upload to ADLS landing
- Databricks workspace access
- Dedicated/single-user Databricks compute
- Databricks secret scope usage
- Microsoft Entra service principal OAuth authentication
- ADLS Gen2 read via `abfss://`
- Delta write to ADLS
- Delta read back from ADLS

## Access pattern

The smoke test uses a Microsoft Entra service principal with `Storage Blob Data Contributor` permissions on the Hermes storage account.

Credentials are stored in the Databricks secret scope `hermes-dev` and retrieved at runtime using `dbutils.secrets.get`.

## Issues Resolved

Initial attempts failed because the notebook was attached to the wrong compute type and because the service principal secret mapping needed correcting.

The successful configuration used dedicated Databricks compute and the correct mapping:

- `appId` to `adls-client-id`
- `password` to `adls-client-secret`
- `tenant` to `tenant-id`

---

# Entry-013: Runtime Configuration

## Summary

I added runtime configuration for local and Azure execution modes.

The new configuration layer centralises lakehouse paths and allows HERMES to switch between local filesystem paths and ADLS Gen2 `abfss://` paths.

## Design decision

The project will use one pipeline implementation with configurable paths rather than maintaining separate local and Azure codebases.

## Runtime modes

Local mode uses repository-local paths under `data/`.

Azure mode builds ADLS Gen2 paths from the `HERMES_STORAGE_ACCOUNT` environment variable.

---

# Entry-014: Running Batch Pipeline on Azure Databricks

## Summary

My objective was to move the batch pipeline from a local development workflow into an Azure Databricks runtime. 

In order to do this transition from local to cloud, I needed to following to run in Azure:

- Ingestion of raw retail CSV source files from the ADLS landing container (the landing container already has its contents uploaded)
- Writing of the Bronze tables to the ADLS bronze storage container
- Leverage Spark transformations for converting raw Bronze data into curated Silver Delta tables in the Silver ADLS container
- Apply packaged YAML data contracts for the validation of Silver tables
- Quarantine invalid / malformed Silver records into the dedicated ADLS quarantine container
- Write both validation and data audit reports to the ADLS audit container
- Verify that the cloud runtine behaviour is consistant with the results of the local batch pipeline to verify Cloud reproduction of the complete batch worflow

This was unmistakenly the most important section of the project so far as it represented a key deliverable I had planned: The transformation of a legacy omnichannel retail stream, to a local lakehouse prototype, into a fully cloud-deployed data engineering pipeline running on Azure Databricks.

## Runtime Architecture

The Azure batch pipeline uses the following infrastructure and runtime tooling choices:

- For the compute layer, I am using Azure Databricks running PySpark ingestion, transformation, and data validation jobs, utillising a Spark compute cluster.

- Storage is handled with ADLS Gen2 (Azure Delta Lake Storage), which stores landing, bronze, silver, quarantine, and data audit outputs.

- Cloud infrastructure is managed by Terraform, allowing consistent and reproducible provisioning of Azure resources.

- Secrets are handled with the Databricks Secrets Scope which serves to store service principal credentials for ADLS OAuth access.

- Authentication is managed by Azure service principal, which give permissions for Databricks to perform reads/writes on the ADLS Gen2 paths.

- Data transformations are handled with PySpark and Delta Lake, PySpark transformations convert Bronze data into the curated Silver Delta tables.

- Data quality is managed by defined YAML contracts in conjunction with PySpark validation, for the purpose of validating data at the table column-level and table relationship level.

- Auditability is encouraged with ADLS audit outputs that store ingestion and validation audits and reports.


The ADLS container layout is as follows:

- landing/
- bronze/
- silver/
- gold/
- quarantine/
- audit/

Gold is handled with dbt modelling and intends to be layered on top of the existing batch pipeline architecture and workflow.

## Runtime Configuration Refactoring

Before I could run anything in Azure, the pipeline needed support for both local and cloud workflows, manifesting as needing support for both local and cloud paths, without duplicating existing pipeline logic.

The local pipeline prototype has the following structure:

```txt
data/sample/raw
data/bronze
data/silver
data/quarantine
data/audit
```

Whereas, in Azure, the same logic and overall architecture exists, but resolve to ABFS paths such as:

```txt
abfss://landing@<storage-account>.dfs.core.windows.net/hermes/raw
abfss://bronze@<storage-account>.dfs.core.windows.net/hermes/bronze
abfss://silver@<storage-account>.dfs.core.windows.net/hermes/silver
abfss://quarantine@<storage-account>.dfs.core.windows.net/hermes/quarantine
abfss://audit@<storage-account>.dfs.core.windows.net/hermes/audit
```

In order to support this change, divergence in logic was necessary. So, I introduced a runtime configuration layer controlled by environment variables:

```bash
HERMES_RUNTIME_ENV=azure
HERMES_STORAGE_ACCOUNT=<storage-account-name>
```

The runtime settings resolve the approprate paths depending on if the execution environment is local or Azure. 

This allowed for the same pipeline to be used for both local and Azure runtime environments. And also allowed pipeline execution in Databricks with thin runners instead of entirely new pipelines dedicated to specific cloud-based logic.

The permitted sychronised local testing and deployability in Databricks without continuously copying transformation code and snippets of the pipeline into notebooks in Databricks.


## Databricks ADLS Authentication

Databricks was configured to access ADLS Gen2 using OAuth credentials stored in a Databricks secret scope.

These secrets are:
```bash
adls-client-id
adls-client-secret
tenant-id
```

The notebook configures Spark with the service principal credentials stored in the secret scope:

```python
client_id = dbutils.secrets.get("hermes-dev", "adls-client-id")
client_secret = dbutils.secrets.get("hermes-dev", "adls-client-secret")
tenant_id = dbutils.secrets.get("hermes-dev", "tenant-id")

account_fqdn = f"{storage_account}.dfs.core.windows.net"

spark.conf.set(f"fs.azure.account.auth.type.{account_fqdn}", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{account_fqdn}",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_fqdn}", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_fqdn}", client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
)
```

## Batch Pipeline Execution

The Databricks notebooks executes the individual parts of the pipeline in sequence using the packaged pipeline functions cloned from the HERMES GitHub Repo:

```python
from hermes.ingestion.bronze_ingestion import (
    BronzeIngestionMeta,
    full_source_ingestion_to_bronze,
)
from hermes.transforms.bronze_silver_transform import transform_all_bronze_to_silver
from hermes.quality.contract_validators import (
    validate_silver_table_relationships,
    validate_silver_tables,
)

bronze_ingestion_results: list[BronzeIngestionMeta] = full_source_ingestion_to_bronze()

silver_results = transform_all_bronze_to_silver(spark=spark)

validation_results = validate_silver_tables(spark=spark)

relationship_results = validate_silver_table_relationships(
    validation_spark_session=spark
)
```

The pipeline execution was successful, verifying the pipeline can run in Azure Databricks with ADLS storage.

## Issues Encountered

### Databricks Ran the Pipeline in Local mode instead of Azure mode

The first Databricks pipeline execution attempt erroneously used local paths instead of ADLS paths.

The HERMES runtime environment variables were not set inside the Databricks Python process after package installation and restarting the notebook.

Specifically the package required:

```python
os.environ["HERMES_RUNTIME_ENV"] = "azure"
os.environ["HERMES_STORAGE_ACCOUNT"] = storage_account
```

The resolution here was to set environment variables after every package installation/respart since with restarts using `pip install` clears the session variables.

So this solution was implemented:

```python
import os

storage_account = "<storage-account-name>"

os.environ["HERMES_RUNTIME_ENV"] = "azure"
os.environ["HERMES_STORAGE_ACCOUNT"] = storage_account
```

### Stale Packages Installed in Databricks

After local fixes to the pipeline were made, Databricks persisted in throwing errors originating from old code because Databricks was running on the previously installed version of the HERMES package.

To resolve this I introduced using force reinstallation from GitHub after each code fix with:

```python
%pip install --force-reinstall git+https://github.com/<username>/<repo>.git
dbutils.library.restartPython()
```

After each restart, runtime variables and the Spark OAuth configuration were rerun.

### Syntax Error Resolutions

- Bronze ingestion failed due to incorrect PySpark syntax using `withColumns` instead of `withColumn` when adding metadata fields to the bronze tables. Resolved this by correcting syntax.

- Bronze ingestion also failed with a timestamp formatting bug where `isoformat()` was malformed like `isoformat(())`, resolved with correcting syntax.

- Bronze audit writing failed due to an incorrect SparkSession import. Accidentally used just `spark` instead of `spark.sql` for importing SparkSession. Correcting the import resolved the issue.

- Incorrect pattern: `functions(column_name)` corrected to `functions.col(column_name)`

### YAML Contracts Missing in Databricks

Silver validation failed because the contract directory storing the YAML data contracts did not exist in Databricks yet.

Because the YAML data contracts were stored outside the HERMES Python package. When HERMES was installed into Databricks using `pip install` only the package files were avaliable, and none of the data contracts were present. 

To resolve this I copied the contracts into the HERMES package:

```txt
src/hermes/contracts/silver/
```

The package configuration was updated to include all the data contract YAML files:

```toml
[tool.setuptools.package-data]
hermes = ["contracts/**/*.yml", "contracts/**/*.yaml"]
```

For the contracts from within the package to be used, the contract loader was updated to use `importlib.resource.files`:

```python
from importlib.resources import files
from pathlib import Path


def default_silver_contracts_dir() -> Path:
    return Path(str(files("hermes") / "contracts" / "silver"))


def default_relationship_contract_path() -> Path:
    return Path(
        str(
            files("hermes")
            / "contracts"
            / "silver"
            / "table_relationships"
            / "table_relationships.yml"
        )
    )
```

This exposed the fact that data contracts should be inherently included as part of the pipeline logic, and are not just lakehouse data. They should be version controlled and packaged with code, rather than copied into ADLS as runtime data files.

### Customers Table Malformation After Azure Ingestion

The pipeline ran fully, however the Silver validation results showed a large number of customer records being quarantined, containing thousands of malformed rows. The customer row count was also unexpectetly inflated, indicating somehow additional data was being added for no reason.

The cause of this was that the Customers CSV file contained multiline address fields as produced by the Python Faker library which spanned over multiple lines inside a quoted CSV field.

By default, Spark CSV reading does not enable multiline parsing. Without the multiline support, Spark interpreted the address line breaks as completely new records which ended up splitting a single customer row into multiple malformed rows.

This caused all the downstream data to also be malformed, triggering the massive instance of data quarantine in the pipeline run.

To resolve this, multiline support needed to be added, as well as extra care for quotation within CSV fields:

```python
source_data_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(source_path)
)
```

After adding the multiline support, the pipeline re-run was successful and all the bronze and silver tables formed correctly, no quarantine was triggered as expected.


---

# Entry-015: Silver Delta Table Registration With Unity Catalog

I completed the registration of Silver Delta tables in Databricks so that they can be queried as logical tables and used as dbt sources for the intended Gold layer.

The Azure Databricks batch pipeline has already been successfully run, therefore the Silver data already existed physically in ADLS Gen2 as Delta tables.

However, it is not effective for dbt to model directly against raw ADLS paths such as:

```bash
abfss://silver@<storage-account>.dfs.core.windows.net/hermes/silver/customers
abfss://silver@<storage-account>.dfs.core.windows.net/hermes/silver/orders
abfss://silver@<storage-account>.dfs.core.windows.net/hermes/silver/order_items
```

Instead, by registing logical tables, dbt can read from logical database objects:

```
dbw_hermes_dev_9s5nbox.silver.customers
dbw_hermes_dev_9s5nbox.silver.orders
dbw_hermes_dev_9s5nbox.silver.order_items

```

Therefore, this part of the project has been focused on creating a governed table layer between the physical lakehouse storage and the intended analytics engineering layer.

### The Necessity of Silver Registration

Although the batch pipeline writes Silver data as Delta tables in ADLS, this is not enough for downstream SQL modelling.

Without effective table registration, the downstream SQL logic will have to now the physical ADLS storage paths:

```sql
SELECT *
FROM delta.`abfss://silver@<storage-account>.dfs.core.windows.net/hermes/silver/customers;
```

Instead of so tightly coupling the analytics logic with the underlying storage layout, I decided to expose the Silver layer through Unity Catalog tables which look more like this:

```sql
SELECT *
FROM dbw_hermes_dev_9s5nbox.silver.customers
```

The benefits of this approach are as followes:

- dbt can define sources using catalog/schema/table names instead of verbose explicty storage paths

- The physical ADLS paths are hidden from the Gold model SQL

- Databricks permissions can be applied to catalog, schema, tables, and external locations (physical storage paths)

- The neater structure reflects a more authentic lakehouse data governance expectation and patterns

- The silver layer is easier to insepct, document, test, and query


Overall, this is a pretty authentic, well-governed, and logical step that bridges the gap between data engineering and analytics engineering for this project.


### Implementation And Issues Encountered

The implementation was executed using a Databricks Notebook with cells executed sequentially covering the following:

- Configuring the storage account
- Configuring the ADLS OAuth access in Spark
- Defining the Silver base path
- Creating the schemas
- Looping over the Silver tables names
- Registering each Silver Delta location as a table
- Verifying silver tables and row counts

#### Issue 1: Unity Catalog External Locations

Initially table registration failed because the Databricks workspace was using Unity Catalog governance by default. Although the notebook spark OAuth allowed Spark to read and write ADLS paths, the Unity catalog table registration had an additional governence requirement that the cloud storage path must be covered by a Unity Catalog external location.

To resolve this I created and used Unity External locations. The required governance follows this chain:

- Azure Databricks Access Connector
- Managed Identity with ADLS permissions
- Unity Catalog storage credential
- Unity Catalog external location definition
- External Delta table registration

So it's worth noting in future that the notebook-level spark credentials and the unity catalog storage governance are seperate layers and need addressing individually.

It's also worth noting that existing Unity Catalogs can exist by default in Databricks. This is checked using:

```sql
SELECT current_catalog(), current_schema();
```

Which in my case, returned:

```txt
current_catalog() = dbw_hermes_dev_9s5nbox
current_schema()  = default
```

So the workspace was already using a viable Unity Catalog catalog. Ultimately, rather than creating an entirely new catalog, I just used the existing workspace catalog and created the dedicated schemas inside of it:

```txt
dbw_hermes_dev_9s5nbox.silver
dbw_hermes_dev_9s5nbox.gold
```

Then I set up the Unity Catalog external locations. I created an external location over the silver and gold cloud paths to expose Silver to dbt source, and gold for the curated data marts from dbt.

The external connects connects the ADLS paths to a Unity Catalog Storage credential which is backed by an Azure managed identity which authorises external table creation against the silver delta paths.

### File Events Warning

During the creation of the Unity Catalog external locations, I got a warning from Databricks:

```txt
File Events Permissions Not Verified

Your storage credential can read and write to this location, but file events permissions could not be verified.
File events are optional but recommended; they improve ingestion performance and reduce cloud storage listing costs.
```

I found that this was because the storage credential had enough permission to read and write to the ADLS paths. However, Databricks could not verify or provision the additional cloud resources used for files events.

File events are more useful for event-driven data ingestion such as an Auto Loader as they reduce the need for repeated directory listing. 

However, since I am currently working on controlled batch jobs over known input and output paths, the current batch workflow does not rely on file event notifications.

Therefore, the external location was forcefully created for now. In future, when I implement the streaming side of the project, then File Events are more significant. For streaming and Auto Loader ingestion, the access connector managed identity will be granted some additional Azure permissions required for file events. 

### Table Verification

After the silver tables were registered I verified them using Databricks SQL. All the expected tables with the expect row counts were present, and with direct inspection of table samples, everything seemed in order. Table metadata also showed that the location of the table pointed to the correct ADLS silver delta paths.

### Outcome

Now that the Silver Delta tables are registered with Unity Catalog, the silver layer can now be queried through more logical table names, and the project is better governed.

This matter for dbt, as it works best when sources are declared as database objects rather than physical file paths.


---

# Entry-016: Gold Layer with dbt / Azure Databricks

The final stretch of the Hermes batch pipeline deployment was building the Gold analytics layer using dbt on Azure Databricks.

This involved connecting my local dbt staging models, dimensional models, gold marts and test definitions to Azure Databricks.

Before beginning the dbt Gold layer work, the silver delta tables had already been registered in Unity Catalog as the following:

```txt
dbw_hermes_dev_9s5nbox.silver.customers
dbw_hermes_dev_9s5nbox.silver.stores
dbw_hermes_dev_9s5nbox.silver.products
dbw_hermes_dev_9s5nbox.silver.orders
dbw_hermes_dev_9s5nbox.silver.order_items
dbw_hermes_dev_9s5nbox.silver.inventory_snapshots
dbw_hermes_dev_9s5nbox.silver.promotions
```

These registered tables are the governed source layer for dbt, and the gold target schema was defined as `dbw_hermes_dev_9s5nbox.gold`, and was the intended output of the models (stagining viewss, dimensions, facts, intermediate models. and retail marts) created by dbt.

### dbt Databricks Connection Setup

Initially, I had to configure dbt so that it could connect to my Azure Databricks workspace. The generic pattern used for the dbt profile configuring was like the following:

```yaml
hermes:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: dbw_hermes_dev_9s5nbox
      schema: gold
      host: adb-7405606138336902.2.azuredatabricks.net
      http_path: /sql/protocolv1/o/7405606138336902/<cluster-http-path>
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 4
```

The token was not hardcoded and injected as an environment variable. The type and host just define the Databricks connection, with host being the Azure Databricks workspace address. The http path references the particular Spark compute cluster I intended to use for the dbt gold transformations, I just used the http path for the basic and cheap cluster I made for the Hermes project.

The connection was validated with `dbt debug` and passed.

### dbt Source Configuration

The silver unity catalog tables were declared as the dbt sources. I defined the source YAML so it pointed dbt at the governed Silver scheme existing the the Databricks workspace:

```yaml
version: 2

sources:
  - name: hermes_silver
    database: dbw_hermes_dev_9s5nbox
    schema: silver
    description: 
    tables:
      - name: customers
      - name: stores
      - name: products
      - name: orders
      - name: order_items
      - name: inventory_snapshots
      - name: promotions
```

By doing this, I had allowed the dbt staging models to reference the governed silver tables with `{{ source('hermes_silver', 'orders') }}` for example which is much neater than referenceing the ADLS Gen2 paths and more proper dbt usage.


### Important dbt Dependencies

I found it was important to make sure the necessary dbt dependencies are installed, especially with more modern syntax changes. The existing gold models and test used package macros from `dbt_utils` and `dbt_expectations`, I made sure there were in the `packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0

  - package: calogica/dbt_expectations
    version: 0.10.9
```

Dependencies were installed with `dbt deps`.


### Corrections to dbt Syntax

Initial dbt runs failed due to some syntax errors that needed correcting:

- Instances of `dbt.utils` were corrected to `dbt_utils`
- Instances of `REF` were corrected to `ref` as these are case sensitive

During initial runs I had an issue of running a downstream model without parents. Models have upstream dependencies that must run before the downstream models. So when running individual dbt models this must be kept in mind in future.

For example running `dbt run --select dim_product` the product dimensions table requires running its upstream dependency which is done with `dbt run --select +dim_product` which runs the staging model for product `stg_product` and then the dimension model `dim_product`.

### Schema Name Issue

I noticed that dbt was creating or looking for schemas such as `gold_gold` or `gold_silver_staging` resulting in outputs like `gold_gold.dim_product`.

This was unwanted and caused by default dbt custom schema behaviour the concatentates the target schema with the custom schema names.

Since the profile target schema was `schema: gold`, and the model was configured with `+schema: gold`, then dbt resolves the final schema name as `gold_gold`.

To resolve this the custom schema configuration was simplified so the dbt project allows the profile target schema to control the dbt output model:

```yaml
models:
  hermes:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

With the profile target schema set to `schema: gold` the model now build just `gold` as the schema and not the unusual composite names.

So, note in future that the defualt dbt schema generation can produce unexpected names if there are custom schemas layered on top of target schemas.

### Addressing Schema Drift

Running dbt on a full refresh `dbt run --full-refresh` came up with failures indicating schema drift. This was due to some missing column definitions and incorrect spellings / mismatches between staging, sources, dimensions, intermediate models, and the gold marts. 

Synchronising the column names and references between models resolved this issue.

Another issue was after the successful building of the dbt gold models, using `dbt test` one test remained failing. This test was for checking the acceptecd values for the promotions channel field.

I resolved this by updating the staging model logic so that the accepted values mirrored that generated retial promotions data.

After this other minor correction, all dbt tests passed.


### The Gold Models Built

After the resolution of the issues described above, the gold later was successfully built producing the following:

**Staging Views*
```txt
stg_customers
stg_stores
stg_products
stg_orders
stg_order_items
stg_inventory_snapshots
stg_promotions
```

**Core Gold Models**
```txt
dim_customer
dim_product
dim_stores
fct_sales
fct_inventory_snapshot
```

**Intermediate Models**
```txt
int_order_revenue
int_inventory_position
int_promotion_attribution
```

**Retail KPI Marts**
```txt
mart_daily_retail_kpis
mart_promotion_performance
```

### dbt Testing

The dbt tests covered a lot of the same quality checks as done within the silver validation layer:

```txt
not_null tests
unique tests
relationships tests
accepted_values tests
dbt_expectations regex tests
```

This may be considered a little overboard as realistically after silver validation a lot of these data quality issues are likely to be caught. Although having another test layer serves to further protect the pipeline structurally and from a business intelligence standpoint.

### Note on Gold Storage

The silver layer is stored as external Delta tables in ADLS Gen2 and registered in Unity Catalog.

Whereas, the Gold dbt models are currently materialised as Unity Catalog managed tables under `dbw_hermes_dev_9s5nbox.gold` which means that the gold tables are visible and queryable in the Databricks Unity Catalog, but do not appear as files within the ADLS gold container.

A future enhacement to the project will be to materialise Gold as external Delta tables in ADLS gold container for a full physical medallion lakehouse architecture. However, gold being queryable is acceptable before moving to the streaming aspects of Hermes that will be developed soon.

### Final Batch Pipeline Architecture

At this point in development, the Hermes batch lakehouse pipeline is:

- ADLS landing
- Databricks bronze ingestion
- ADLS Bronze
- Databricks silver transformations
- ADLS silver delta tables
- Silver data contracts, quarantine and audit
- Unity Catalog silver registration to external tables
- dbt staging views
- dbt gold dimensions, facts, and gold marts
- dbt tests

This is a complete batch medallion lakehouse implementation on Azure Databricks

Next is working on the streaming aspects of Hermes that will definitely add to the complexity of these works.

---





