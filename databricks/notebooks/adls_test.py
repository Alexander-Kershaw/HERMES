"""
ADLS smoke test:

- Confirms if Databricks can read the raw CSV retail data source files from ADLS landing.
- Confirms if Databricks can write Delta outputs to ADLS.
- Conforms if Databricks can then read the Delta outputs back.

Note this is ran inside Azure Databricks on a simple compute cluster

"""
# Ignore ruff check here since dependencies are satified in the Azure Databricks environment
# ruff: noqa

hermes_storage_account_name = "hermes_storage_account_name"  # not the real name

# Configuring databricks OAuth
client_id = dbutils.secrets.get("hermes-dev", "adls-client-id")

client_secret = dbutils.secrets.get("hermes-dev", "adls-client-secret")

tenant_id = dbutils.secrets.get("hermes-dev", "tenant-id")

account_fqdn = f"{hermes_storage_account_name}.dfs.core.windows.net"

spark.conf.set(f"fs.azure.account.auth.type.{account_fqdn}", "OAuth")

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{account_fqdn}",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)

spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_fqdn}", client_id)

spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_fqdn}", client_secret)

spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

print(f"Configured OAuth for {account_fqdn}")


# Neccessary paths
landing_orders_blob_path = f"abfss://landing@{hermes_storage_account_name}.dfs.core.windows.net/hermes/raw/orders.csv"

silver_test_path = f"abfss://silver@{hermes_storage_account_name}.dfs.core.windows.net/hermes/test/order_delta"

print(landing_orders_blob_path)

# Reading orders from landed source data
orders_df = spark.read.option("header", True).option("inferSchema", True).csv(landing_orders_blob_path)

# Proving read
print("Landing orders schema:")
orders_df.printSchema()

orders_count = orders_df.count()
print(f"Orders row count: {orders_count}")

print("Orders data sample:")
display(orders_df.limit(20))


# Proving writing to delta into silver test cloud storage folder

(orders_df.write.format("delta").mode("overwrite").save(silver_test_path))

print(f"Wrote Delta test table to: {silver_test_path}")


# Proving reading of delta files

test_df = spark.read.format("delta").load(silver_test_path)

test_count = test_df.count()
print(f"Orders delta row count (read back): {test_count}")

assert test_count == orders_count, f"Test failed: expected order row count {orders_count}, to be equal to order delta count {test_count}"


print("Test orders data delta table sample:")
display(test_df.limit(20))

print("ADLS test passed")
