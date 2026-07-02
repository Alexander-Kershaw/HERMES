# HERMES Lakehouse Design

## Overview

I am using the conventional medallion lakehouse architecture with Bronze, Silver, and Gold layers for HERMES.

The architecture is developed locally first and later deployed to Azure using ADLS Gen2 and Azure Databricks.

## Individual Layer Responsibilities

### Bronze Layer

The Bronze layer stores raw ingested source data with minimal transformation.

**Responsibilities:**

- preserve source records
- retain source file metadata
- add ingestion timestamps
- support data auditability
- provide a stable foundation for Silver transformations

Bronze is explicitly not intended to apply any business rules beyond basic ingestion metadata.

Current local path:

```text
data/lakehouse/bronze/
```

### Silver Layer

This silver layer will be used to store the cleaned, explicitly typed, deduplicated, and data contract conformative data.

**Responsibilities:**

- enforce schema contracts
- standardise column types
- validate table relationships
- remove duplicates
- quarantine malformed / erronious records
- form the foundation of trusted retail data entities

Local path:

```text
data/lakehouse/silver
```

### Gold Layer

The gold layer will store fact and dimention tables, and curated KPI data marts that are directly prepared for business use. This will be the key analytics engineering layer to exposes useful data for business initiatives.

**Responsibilities:**

- dimentional modelling
- business metrics definition
- business intelligence aggregations 
- dbt documentation and tests
- analytics tables designed for stakeholder engagement

Local path:

```text
data/lakehouse/gold/
```

## Bronze Layer Implementation

The bronze layer implementation reads source CSV files generated from the retail data generation script, reading source files from `data/sample/raw/`, and writes parquet files to the bronze layer in the data lakehouse (local first) at `data/lakehouse/bronze/<source_name>/<source_name>.parquet`.

Each bronze table includes metadata columns for auditability and appropriate data lineage tracing:

- `_bronze_source_name`
- `_bronze_source_path`
- `_bronze_source_file`
- `_bronze_ingestion_datetime`
- `_bronze_ingestion_date`

The source files are CSV and is used to simulate batch data extracts from various operational systems within the business. This is simple, common in legacy systems (the project purpose is essentially system modernization so this is arguably quite realistic), CSV is easier to inspect and validate whether the simulation emits the expected data, and is useful for demonstrating the source data to bronze layer ingestion.

However, CSV has some limitations such as weak typing, inefficient for analytics, and has generally poor schema evolution support.

The bronze files themselves are parquet. This is beneficial due to columnar storage, better read performance than CSV, preserves the typed tabular structure, and is closer to modern data lakehouse storage. However, parquet is non transactional, and does not provide delta lake ACID guarantees which risks transactions being unsafe due to partial writes.

ACID (atomicity, consistency, isolation, and durability) is important and tradiational data lakes like raw parquet files do not support transactions. The moder data lakehouse bring ACID properties to object storage (AWS S3 or Azure ADLS) using table formats such as Apache Iceberg, Delta Lake, or Apache Hudi.

The focus will be on Data Lake that uses an ordered JSON transaction log to track commits and forces changes to happen in a clear and orderly sequence. 

## Silver and Gold Layers: Delta Lake

The silver and gold layers move towards Delta Lake.

This is particularly important and comes with the following benefits:

- ACID transactions
- schema enforcement
- merge / upsert support
- time travel
- better quality lakehouse semantics

The bronze layer could have been written directly as Delta. However, in this case I elected to go with bronze implementation with raw parquet to simplify the early local development and to avoid introducing Spark before the source ingestion is fully capitulated.

Delta Lake is introduced with the PySpark transformations, not in raw bronze ingestion.

---