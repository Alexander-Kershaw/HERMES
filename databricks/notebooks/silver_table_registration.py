"""
================================================================================================================================================
SILVER TABLE REGISTRATION
================================================================================================================================================

The purpose of this script is to register ADLS Silver delta tables as Databricks tables for later dbt-databricks usage in creating
the gold layer of the HERMES data lakehouse.

The batch pipeline script writes Delta files to ADLS in the following format: 

- abfss://silver@<storage-account>.dfs.core.windows.net/hermes/silver/<table>

------------------------------------------------------------------------------------------------------------------------------------------------

The sections of the script (to be executed sequentially within a Databricks notebook) exposes the Delta file locations as logical 
Databricks tables such as:

- hermes_silver.customers
- hermes_silver.orders
- hermes_silver.stores

With the establishment of these tables in Databricks, dbt Gold models can read from them.


------------------------------------------------------------------------------------------------------------------------------------------------

Note: Unity Catalog is required for table registration, details given in DEVLOG and documentation.

================================================================================================================================================
"""

# ruff: noqa

"""
================================================================================================================================================
CELL 1: Configure Runtime Variables
================================================================================================================================================
"""

import os

HERMES_AZURE_STORAGE_ACCOUNT_NAME: str = "sthermesdev9s5nbox"

os.environ["HERMES_RUNTIME_ENV"] = "azure"
os.environ["HERMES_STORAGE_ACCOUnT"] = HERMES_AZURE_STORAGE_ACCOUNT_NAME

silver_ADLS_container: str = "silver"
catalog: str = "dbw_hermes_dev_9s5nbox"
silver_schema:str = "hermes_silver"
gold_schema:str = "hermes_gold" 

silver_base_ADLS_path = f"abfss://{silver_ADLS_container}@{HERMES_AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/hermes/silver"

print(f"Azure Storage Account: {HERMES_AZURE_STORAGE_ACCOUNT_NAME}")
print(f"Silver base Path: {silver_base_ADLS_path}")
print(f"Silver schema: {silver_schema}")
print(f"Gold schema: {gold_schema}")


"""
================================================================================================================================================
CELL 2: Configure ADLS OAuth Access
================================================================================================================================================
"""

client_id: str = dbutils.secrets.get("hermes-dev", "adls-client-id")
client_secret: str = dbutils.secrets.get("hermes-dev", "adls-client-secret")
tenant_id: str = dbutils.secrets.get("hermes-dev", "tenant-id")

account_fqdn = f"{HERMES_AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

spark.conf.set(f"fs.azure.account.auth.type.{account_fqdn}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_fqdn}", f"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_fqdn}", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_fqdn}", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

print(f"Configured OAuth for {account_fqdn}")


"""
================================================================================================================================================
CELL 3: Confirm Silver Data Paths Exist
================================================================================================================================================
"""

display(dbutils.fs.ls(silver_base_ADLS_path))


"""
================================================================================================================================================
CELL 4: Define All Silver Tables In ADLS 
================================================================================================================================================
"""

silver_tables: list[str] = [
    "customers",
    "stores",
    "products",
    "orders",
    "order_items",
    "inventory_snapshots",
    "promotions"
]

for silver_table_name in silver_tables:
    table_path: str = f"{silver_base_ADLS_path}/{silver_table_name}"
    print(f"""
        -----------------------------------------------------------------------------------------------------------------------------------
        Table Name: {silver_table_name}
        Table Path: {table_path}
        ----------------------------------------------------------------------------------------------------------------------------------- 
        """)
            

"""
================================================================================================================================================
CELL 5: Create The Databricks Schemas
================================================================================================================================================
"""

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

print(f"Created Schema: {silver_schema}")
print(f"Created Schema: {gold_schema}")

"""
================================================================================================================================================
CELL 6: Register Silver Delta Tables
================================================================================================================================================
"""   

for silver_table_name in silver_tables:
    table_path: str = f"{silver_base_ADLS_path}/{silver_table_name}"
    new_table_name: str = f"{catalog}.{silver_schema}.{silver_table_name}"

    create_tbl_query: str = f"""
    CREATE TABLE IF NOT EXIST {new_table_name}
    USING DELTA
    LOCATION '{table_path}'
    """

    print(f"Registering Table: {new_table_name}")
    print(create_tbl_query)

    spark.sql(create_tbl_query)

print("Silver Table Registration Complete")


"""
================================================================================================================================================
CELL 7: Display Registered Tables
================================================================================================================================================
""" 

display(spark.sql(f"SHOW TABLES IN {silver_schema}"))


"""
================================================================================================================================================
CELL 8: Validate Tables (Row Counts)
================================================================================================================================================
""" 

from pyspark.sql import functions

row_count_res: list = []

for silver_table_name in silver_tables:
    registered_table_name = f"{silver_schema}.{silver_table_name}"

    table_row_count_query = spark.sql(f"""SELECT COUNT (*) AS row_count
                                            FROM {registered_table_name}""")
    
    row_count = table_row_count_query.collect()[0]["row_count"]

    row_count_res.append(
        {
            "schema_name": silver_schema,
            "table_name": silver_table_name,
            "registered_table_name": registered_table_name,
            "row_count": row_count
        }
    )

row_count_df = spark.createDataFrame(row_count_res)

display(row_count_df.orderBy(functions.col("table_name").asc()))


"""
================================================================================================================================================
CELL 9: Table Previews
================================================================================================================================================
""" 

display(spark.sql(f"SELECT * FROM {silver_schema}.customers LIMIT 10"))

display(spark.sql(f"SELECT * FROM {silver_schema}.orders LIMIT 10"))

display(spark.sql(f"SELECT * FROM {silver_schema}.order_items LIMIT 10"))

display(spark.sql(f"SELECT * FROM {silver_schema}.stores LIMIT 10"))

display(spark.sql(f"SELECT * FROM {silver_schema}.inventory_snapshots LIMIT 10"))

display(spark.sql(f"SELECT * FROM {silver_schema}.products LIMIT 10"))

display(spark.sql(f"SELECT * FROM {silver_schema}.promotions LIMIT 10"))


"""
================================================================================================================================================
CELL 10: Table Metadata Description
================================================================================================================================================
""" 

for silver_table_name in silver_tables:
    registered_table_name: str = f"{silver_schema}.{silver_table_name}"
    print(f"=====| Description For: {registered_table_name} |=====")
    display(spark.sql(f"DESCRIBE DETAIL {registered_table_name}"))


"""
================================================================================================================================================
CELL 11: Registration Completion
================================================================================================================================================
""" 

print("=====| HERMES Silver Table Registration Complete |=====")
print(f"Registered {len(silver_tables)} Silver Delta tables in schema: {silver_schema}")
print(f"Prepared Gold schema for dbt models: {gold_schema}")

