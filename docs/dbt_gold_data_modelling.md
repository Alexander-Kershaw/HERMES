# HERMES dbt Gold Modelling

## Overview

I am using dbt for gold layer analytics modelling.

The dbt layer is responsible for turning trusted Silver entities into business intelligence facts, dimensions, and retail marts.

## Why dbt?

dbt is used because gold modelling benefits from:

- modular SQL models
- lineage
- documentation
- tests
- clear model dependencies
- analytics engineering conventions

It may be argued that simply sticking to PySpark and just using databricks is more streamlined and reduces the complexities of the project. This is true. However, I wanted to still use dbt for the batch pipeline processing side of this project to emphasise data lineage, testing, and documentation. For the streaming aspect of the project, dbt will not be used.

## Layer split

So far I am using the following split:

- PySpark handles Bronze to Silver data engineering transformations.
- dbt handles Silver to Gold analytics modelling.

This keeps engineering intensive processing separate from business intelligence SQL modelling which acts more as an analytics engineering layer.

## dbt model layers

### Sources

Sources reference trusted silver tables produced from Spark transformations.

### Staging

Staging models provide light cleanup and stable dbt prepared model names.

### Intermediate

Intermediate models contain reusable business logic such as revenue calculation, inventory valuation, and promotion attribution. And act as a gateway to the more precise and pointed business intelligence data marts and KPIs.

### Marts

Mart models produce final facts, dimensions, and KPI tables.

## Initial Gold models

Core models:

- dim_customers
- dim_products
- dim_stores
- fct_sales
- fct_inventory_snapshot

Retail marts:

- mart_daily_retail_kpis
- mart_promotion_performance

## Current execution status

As of now the dbt project is scaffolded locally and designed to run on Databricks once the Azure batch platform is deployed.

The local project can be parsed and validated structurally, but full dbt execution requires a Databricks connection and registered silver tables.

## Trade Offs

Running dbt directly against local Delta files would add complexity that is not representative of the target architecture.

The project therefore keeps dbt aligned to the intended Databricks execution environment and acts as another data quality and transformation layer exclusive to the gold layer of the data lakehouse architecture.

As stated before, I could have achieved this with PySpark transformations and other means. But, I like dbt tests and lineage functionalities. PySpark will be employed more with streaming pipelines. 

---