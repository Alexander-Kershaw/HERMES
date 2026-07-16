# HERMES Silver Table Registration

The intention of this document is to detail the intermediate development phase situated between the proven Azure Databricks batch pipeline run, and dbt gold layer data modelling. Up to this point, the bronze ingestion, silver transformation, and data validation and quarantine have been proven in Azure Databricks.

Before using dbt for gold data modelling, Azure Databricks must be prepared for dbt by registering the Silver Delta tables that are situated in ADLS. For dbt to work with the data, the Silver Delta table paths must be used to register Databricks tables. 

Note: this is less pure PySpark data engineering at this stage. The intention of using dbt here instead of continuing using PySpark is to demonstrate the transision from tradiational data engineering to a more business intelligence focused analytics engineering layer. dbt is less computationally powerful of a tool for transformations, however at this stage the bulk of serious transformation is covered by PySpark in the Bronze to Silver transformation. dbt however, is superior for semantics, business intelligence, dashboards, data governance and lineage, making it superior for the gold layer (in my humble opinion).

## Table Registration Process

### Creating Databricks Schemas

I require the following Databricks schemas in Databricks SQL (can just be done in a notebook too):

```sql
CREATE SCHEMA IF NOT EXISTS hermes_silver;
CREATE SCHEMA IF NOT EXISTS hermes_gold;
```

### Registering The Tables

I also need tables for each of the business data channels (customers, stores, products, etc...) explictly using delta and referencing the location of the relevant channel data in ADLS. For example, like customers:

```sql
CREATE TABLE IF NOT EXISTS hermes_silver.customers
USING DELTA
LOCATION 'abfss://silver@<storage-account>.dfs.core.windows.net/hermes/silver/customers';
```

### Table Verification

To test everything is working as intended I can use Databricks SQL to execute some basic sanity check queries:

```sql
SHOW TABLES IN hermes_silver;

SELECT COUNT(*) FROM hermes_silver.customers;
SELECT * FROM hermes_silver.customers LIMIT 10;
```

### Updating dbt Source YAML

With the tables registered and verified, the dbt source YAML is to be updated to point to the Databricks tables rather than the ABFS paths. dbt will also be connected to the Databricks SQL Warehouse or compute cluster I am using.

---
