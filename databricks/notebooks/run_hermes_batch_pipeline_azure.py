"""
==============================================================================================================================
=====| DATABRICKS NOTEBOOKS: HERMES AZURE BATCH PIPELINE RUNNER |=====
==============================================================================================================================

This script is intended to be imported to Databricks Notebooks and run using a simple compute cluster (limit costs and 
terminate clusters when idle and no need to choose particularly powerful compute cluster configurations).

Attached cluster configuration for optimal cost control:

- All-purpose
- Dedicated / single-user
- Non-serverless
- Automatic cluster termination (10 mins minimum)
- Lower spec cluster compute 

Note: This batch pipeline runner covers a pipeline running from bronze ingestion, silver transformation, and data validation,
quarantine, and audit. Gold layer transformations is handled with dbt-databricks after the registration of the silver tables
to take advantageous of dbt native functionality such as liniage graphs and SQL transformations in the creation of data marts
and key business intelligence KPIS.

Also Note: I am separating sections of the notebook into individuals cells to be executed one at a time, 

------------------------------------------------------------------------------------------------------------------------------

The purpose of the Databricks Notebook includes the following:

- Configuration of ADLS (Azure Delta Lake Storage) OAuth access using the established Databricks secrets (details in 
documentation, specifically: docs/azure_databricks_CLI_commands.md).

- Set the Hermes Azure runtime variables (hermes runtime environment set to "azure" and the hermes storage account provided)

- Run the Hermes batch pipeline against ADLS Gen2 (Note: service principle should be defined for appropriate permissions 
allowing ADLS Gen2 and Azure Databricks to work together).

------------------------------------------------------------------------------------------------------------------------------

Expected ADLS pipeline flow:

- Landing / raw source CSV files

- Ingestion to Bronze as parquet

- Transformation from Bronze to Silver delta lake

- Run data validation, quarantine, and audit

------------------------------------------------------------------------------------------------------------------------------

Note: All transformations are handled with Apache Spark since this is native to the Databricks environment. Running with the 
hermes runtime environment set to "azure" runs the batch pipeline with Spark exclusively, the local environment counterpart
uses conventional Pandas, partially PySpark, and Path objects intead of str paths which WILL NOT work for Azure workflows.

==============================================================================================================================
"""


"""
==============================================================================================================================
SECTION 1: Environment Variables Definition and Setting 
==============================================================================================================================
"""

import os

# From Terraform output: Azure hermes storage account name
HERMES_AZURE_STORAGE_ACCOUNT_NAME: str = "sthermesdev9s5nbox"

# Set env valirables so runtime uses Azure mode and sets storage account name
os.environ["HERMES_RUNTIME_ENV"] = "azure"
os.environ["HERMES_STORAGE_ACCOUNT"] = HERMES_AZURE_STORAGE_ACCOUNT_NAME

print("HERMES_RUNTIME_ENV: ", os.environ["HERMES_RUNTIME_ENV"])
print("HERMES_STORAGE_ACCOUNT: ", os.environ["HERMES_STORAGE_ACCOUNT"])


"""
==============================================================================================================================
SECTION 2: OAuth Configuration
==============================================================================================================================
"""


client_id: str = dbutils.secrets.get("hermes-dev", "adls-client-id")
client_secret: str = dbutils.secrets.get("hermes-dev", "adls-client-secret")
tenant_id: str = dbutils.secrets.get("hermes-dev", "tenant-id")

account_fqdn = f"{HERMES_AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

spark.conf.set(f"fs.azure.account.auth.type.{account_fqdn}", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{account_fqdn}",
    f"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_fqdn}", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_fqdn}", client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

print(f"Configured OAuth for {account_fqdn}")


"""
==============================================================================================================================
SECTION 3: Raw Source Data Landing
==============================================================================================================================
"""

landing_raw_path: str = f"abfss://landing@{HERMES_AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/hermes/raw"

print("Listing landing raw source data path:")
display(dbutils.fs.ls(landing_raw_path))


"""
==============================================================================================================================
SECTION 4: HERMES Modules Imporation (HERMES should be installed on the compute cluster as a package)
==============================================================================================================================

PACKAGE INSTALLATION
------------------------------------------------------------------------------------------------------------------------------

From within a Databricks Notebook cell:

%pip install git+https://github.com/Alexander-Kershaw/HERMES.git
dbutils.library.restartPython()

OR:

built wheel locally with

python -m build

Then upload this to the wheel on Databricks

==============================================================================================================================
"""

from hermes.ingestion.bronze_ingestion import BronzeIngestionMeta, full_source_ingestion_to_bronze
from hermes.transforms.bronze_silver_transform import SilverTransformationMeta, transform_all_bronze_to_silver
from hermes.quality.contract_validators import (
    ContractValidationResult,
    RelationContractValidationResult,
    validate_silver_table_relationships,
    validate_silver_tables
)

"""
==============================================================================================================================
SECTION 5: Bronze Ingestion
==============================================================================================================================
"""

print("=====| Starting HERMES Azure Bronze Ingestion |=====")

bronze_ingestion_results: list[BronzeIngestionMeta] = full_source_ingestion_to_bronze()

for ingestion_result in bronze_ingestion_results:
    print(ingestion_result)


"""
==============================================================================================================================
SECTION 6: Bronze To Silver Transformation
==============================================================================================================================
"""

print("=====| Starting HERMES Azure Bronze to Silver Transformation |=====")

silver_transform_results: list[SilverTransformationMeta] = transform_all_bronze_to_silver(spark=spark)

for transform_result in silver_transform_results:
    print(transform_result)


"""
==============================================================================================================================
SECTION 7: Silver Validation (Table Column-level)
==============================================================================================================================
"""

print("=====| Starting HERMES Azure Silver Table (Column-level) Validation |=====")

silver_validation_results: list[ContractValidationResult] = validate_silver_tables(spark=spark)

failed_silver_validation_results: list[ContractValidationResult] = [
    result for result in silver_validation_results if not result.passed
    ]

print(f"Total column-level validation checks: {len(silver_transform_results)}")
print(f"Failed colum-level validation checks: {len(failed_silver_validation_results)}")

for result in failed_silver_validation_results:
    print(result)


"""
==============================================================================================================================
SECTION 8: Silver Validation (Table Relationships)
==============================================================================================================================
"""

print("=====| Starting HERMES Azure Silver Table Relationship Validation |=====")

silver_tbl_relationships_results: list[RelationContractValidationResult] = validate_silver_table_relationships(validation_spark_session=spark)

failed_tbl_relation_results: list[RelationContractValidationResult] = [
    result for result in silver_tbl_relationships_results if not result.passed
    ]

print(f"Total table relationship checks: {len(silver_tbl_relationships_results)}")
print(f"Failed table relationship checks: {len(failed_tbl_relation_results)}")

for result in failed_tbl_relation_results:
    print(result)


"""
==============================================================================================================================
SECTION 9: Pipeline Outputs Listing
==============================================================================================================================
"""   

bronze_path: str = f"abfss://bronze@{HERMES_AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/hermes/bronze/"
silver_path: str = f"abfss://silver@{HERMES_AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/hermes/silver/"
audit_path: str = f"abfss://audit@{HERMES_AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/hermes/audit/"
quarantine_path: str = (f"abfss://quarantine@{HERMES_AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/hermes/quarantine/")

print("Bronze output:")
display(dbutils.fs.ls(bronze_path))

print("Silver output:")
display(dbutils.fs.ls(silver_path))

print("Audit output:")
display(dbutils.fs.ls(audit_path))

print("Quarantine output:")
try:
    display(dbutils.fs.ls(quarantine_path))
except Exception as exc:
    print(f"No quarantine output found or unable to list quarantine path: {exc}")


"""
==============================================================================================================================
SECTION 10: Validation Failure Exception
==============================================================================================================================
""" 

if failed_silver_validation_results or failed_tbl_relation_results:
    raise Exception(
        "HERMES Azure batch pipeline completed with validation failures. "
        "Check audit and quarantine outputs."
    )

print("HERMES Azure batch pipeline completed successfully.")