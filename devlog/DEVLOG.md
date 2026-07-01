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