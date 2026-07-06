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

