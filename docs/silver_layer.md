# HERMES Silver Layer

## Overview

The Silver layer contains cleaned, typed, deduplicated, and conformed retail entities.

It is produced from the Bronze layer using PySpark and Delta Lake.

## Current Silver tables

- customers
- stores
- products
- orders
- order_items
- inventory_snapshots
- promotions

## Responsibilities

The Silver layer is responsible for:

- applying explicit schemas
- casting source columns into correct data types
- removing duplicate records by primary key
- adding Silver processing metadata
- preparing trusted inputs for Gold modelling

## Current implementation

Bronze tables are read from local Parquet files.

Silver tables are written as Delta Lake tables under:

```text
data/lakehouse/silver/
```

Each of the silver tables produced includes:

- business entity columns
- Metadata: `_silver_processed_at`, `_silver_processing_date`

## The Use of PySpark

I have used PySpark in the silver layer since this layer represents a bulk of the engineering heavy processing in the transformation of bronze data to cleaned and schema enforced silver data.

Distributed data processing, such as with PySpark is useful for:

- schema enforcement
- type casting
- record deduplication
- large table processing
- later relationship validation
- later quarantine logic

dbt was not used here since that will be reserved for data modelling within the gold layer of the data lakehouse. Basically, the silver transformation is pure data engineering, with PySpark being a better fit with its processing power. Analytics engineering is more suited for dbt in the gold layer.

