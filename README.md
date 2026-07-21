![banner](assets/hermes_banner.png)

# HERMES

**HERMES** is a data engineering project intended to simulate a cloud data modernisation initiative for a ficticious omnichannel retail organisation.

This project demonstrates batch processing using Azure, Databricks, PySpark, Spark SQL, Delta Lake, and Kafka compatible streaming. A streaming pipeline is intended in future.

---

## The Purpose of HERMES

This project is designed in such a way to show practical working knowledge and capacity across the following:

- Azure data engineering
- Development using Databricks and PySpark
- Constructing Delta Lake medallion datalake architectures
- Batch data ingestion and orchestration
- Kafka compatible event streaming
- Data modelling in the retail industry context
- Analytics engineering using dbt gold layer data marts
- Ensurance of data quality and governance
- CI/CD and industry standard engineering standards
- Cloud infrastructure as code (IaC) using Terraform
- Appropriate documentation, ADRs, runbooks, and delivery planning

---

## The Scenario 

For HERMES I specifically wanted to apply data engineering principles in the context of a retail business.

The underlying context of these works are as follows.

I am assuming the role of a data engineer working underneath a fictional omnichannel retailer.

The retailed intends to modernise legacy data infrastructure. This includes modernising fragmented batch reporting and delayed operational extracts into a scalable lakehouse platform.

This platform supports the proceeding:

- Sales analytics,
- Inventory monitoring
- Customer behavioural analysis
- Promotion performance reporting
- (Near) real-time KPIs
- Governed data marts prepared for business intelligence needs

---


## Project Status

**COMPLETED:**

- Synthetic retail data generation across multiple retail channels
- Local batch pipeline prototype 
- Provisioned Azure infrastructure with Terraform
- ADLS Gen2 medallion data lakehouse containers
- Azure Databricks batch pipeline execution
- Bronze data ingestion using PySpark
- Silver Delta table transformation
- YAML data contracts (packaged with the python project to Databricks)
- Silver validation and quarantine with audit reporting
- Unity Catalog external silver table registration
- dbt connection to Azure Databricks
- dbt Gold layer dimensional models, fact tables, and data marts

**LATER OBJECTIVES:**

- Streaming data ingestion
- Kafka / Event Hubs / Auto Loader
- Construction of structured streaming pipeline
- Databricks workflow or Azure Data Factor orchestration
- Externalisation of gold later to ADLS gold container (current Unity Catalog tables)
- Finalization of project, cost optimisation, data governance hardening


---

## Architecture

```mermaid
flowchart LR
    subgraph Local["Local Development"]
        A1[Python Synthetic Data Generator]
        A2[pytest + ruff]
        A3[HERMES Python Package]
        A1 --> A3
        A2 --> A3
    end

    subgraph Azure["Azure Infrastructure"]
        B1[Terraform]
        B2[Resource Group]
        B3[ADLS Gen2 Containers]
        B4[Azure Databricks Workspace]
        B5[Key Vault / Secrets]
        B1 --> B2
        B1 --> B3
        B1 --> B4
        B1 --> B5
    end

    subgraph ADLS["ADLS Gen2 Lakehouse"]
        C1[Landing<br/>Raw CSV]
        C2[Bronze<br/>Parquet + ingestion metadata]
        C3[Silver<br/>Delta curated tables]
        C4[Quarantine<br/>Failed validation records]
        C5[Audit<br/>Ingestion + validation reports]
    end

    subgraph Databricks["Azure Databricks Batch Runtime"]
        D1[Runtime Config<br/>HERMES_RUNTIME_ENV=azure]
        D2[ADLS OAuth Config]
        D3[Bronze Ingestion<br/>PySpark CSV Reader<br/>multiLine enabled]
        D4[Silver Transformation<br/>PySpark + Delta]
        D5[Silver Validation<br/>YAML Contracts]
    end

    subgraph UC["Unity Catalog"]
        E1[External Location<br/>Silver ADLS Path]
        E2[Silver Schema<br/>External Tables]
        E3[Gold Schema<br/>Managed Tables]
    end

    subgraph dbt["dbt Databricks"]
        F1[Silver Sources<br/>hermes_silver]
        F2[Staging Views]
        F3[Intermediate Models]
        F4[Dimensions]
        F5[Facts]
        F6[Retail Marts]
        F7[dbt Tests]
    end

    A3 --> D3
    B3 --> C1
    B4 --> D1
    B5 --> D2

    C1 --> D3
    D3 --> C2
    C2 --> D4
    D4 --> C3
    C3 --> D5

    D5 -->|valid data| E1
    D5 -->|invalid records| C4
    D5 --> C5

    E1 --> E2
    C3 --> E2

    E2 --> F1
    F1 --> F2
    F2 --> F3
    F2 --> F4
    F2 --> F5
    F3 --> F6
    F4 --> F6
    F5 --> F6
    F6 --> F7
    F7 --> E3

    classDef local fill:#f8fafc,stroke:#64748b,stroke-width:1px,color:#0f172a;
    classDef azure fill:#e8f1ff,stroke:#2563eb,stroke-width:1px,color:#0f172a;
    classDef adls fill:#eff6ff,stroke:#1d4ed8,stroke-width:1px,color:#0f172a;
    classDef databricks fill:#fff7ed,stroke:#ea580c,stroke-width:1px,color:#0f172a;
    classDef uc fill:#f5f3ff,stroke:#7c3aed,stroke-width:1px,color:#0f172a;
    classDef dbt fill:#fdf2f8,stroke:#db2777,stroke-width:1px,color:#0f172a;

    class A1,A2,A3 local;
    class B1,B2,B3,B4,B5 azure;
    class C1,C2,C3,C4,C5 adls;
    class D1,D2,D3,D4,D5 databricks;
    class E1,E2,E3 uc;
    class F1,F2,F3,F4,F5,F6,F7 dbt;

```


---

## Stack

- Programming / Query languages: **Python, SQL**
- Big data processing: **PySpark**
- Lakehouse storage: **ADLS Gen2, Delta Lake, Parquet**
- Cloud platform: **Microsoft Azure**
- Compute: **Azure Databricks clusters / SQL warehouse**
- Infrastructure: **Terraform**
- Data quality: **YAML contracts, PySpark validation, dbt tests**
- Governance: **Unity Catalog**
- Analytics engineering: **dbt Databricks**
- Testing: **pytest, dbt tests**
- Formatting: **ruff**

---

## Data Models

Project HERMES models a synthetic retail businesses with a multitude of difference source data entities:

- Customers
- Stores
- Products
- Order Items
- Orders
- Inventory Snapshots
- Promotions

The dbt gold layer produces staging models, dimension and fact tables, intermediate models, and retail / KPI data marts.

### Silver Tables

The Silver tables are registered under Unity Catalog by defining the ADLS Gen2 silver layer containing the silver Delta tables as an external location.

The registered silver tables are under `dbw_hermes_dev_9s5nbox.silver`.

### Gold Models

The gold models were built with dbt and exist as Unity Catalog tables under `dbw_hermes_dev_9s5nbox.gold`.

The gold models include:

```txt
stg_customers
stg_stores
stg_products
stg_orders
stg_order_items
stg_inventory_snapshots
stg_promotions

dim_customer
dim_product
dim_stores

fct_sales
fct_inventory_snapshot

int_order_revenue
int_inventory_position
int_promotion_attribution

mart_daily_retail_kpis
mart_promotion_performance
```

---

## Medallion Layers

### Landing

The landing layer contains the raw CSV files uploaded to ADLS Gen2. These CSV files represent the legacy retail data. 

### Bronze

The bronze layer stores the ingested source data from the landing layer with the addition of metadata columns including:

- `_bronze_source_name`
- `_bronze_source_file`
- `_bronze_source_path`
- `_bronze_ingested_at`
- `_bronze_ingestion_date`

The Azure bronze ingestion uses Spark CSV reading with multiline support to correctly handle multiple line fields such as customer addresses.

### Silver

The silver layer contains the cleaned and standardised Delta tables.

The data cleaning involved: explecity typing, standardised naming, deduplication of records, cleaned business entities, and consistent schemas for downstream validation and data modelling.

### Gold

The gold layer was built with dbt on Databricks for a stronger governance and analytics engineering element to the project.

The gold layer reponsibilities included: 

- Staging views over Unity Catalog Silver sources
- Dimensional models
- Fact models
- Intermediate models for revenue, inventory and promotions
- Business intelligence prepared retail marts and KPIs
- dbt testing and documentation (such as lineage graphs)

---

## Data Quality

HERMES uses two complimentary data validation layers in silver and gold.

### Silver Validation

This is the first validation layer that uses custom YAML data contracts, and performs the validation using PySpark.

The YAML contracts define data quality rules such as:

- Enforcement of values to be present (not null)
- Uniqueness of column values
- Particular regex pattern enforcement
- Accepted value definitions 
- Numeric rules
- Table relationship checks

For example, a customer identification values must match the expected ID format, order identifiers must be unique, order status must be one of the accepted values, and foreign key relationship must hold between orders and customers.

Invalid records that fail the data contract rules are written to a quarantine layer. Furthermore, validation reports are written to an audit layer in ADLS Gen2.

### Gold Validation

For the gold layer, the secondary layer of validation is accomplished using dbt tests that check data quality rules similar to that of the silver layer.

The combination of the gold and silver validation layers add more trust in data throughout the pipeline.

Silver quality is oriented around validating transformations of the source / bronze ingested data and effectively quarantining invalid records.

Gold quality is more focused on data model integrity, relationships between tables, and analytical correctness, supporting effective analytics engineering.

---

## Quarantine and Audits

When a record fails the silver validation checks, they are written to a dedicated ADLS quarantine path. Audit reports are also written to another dedicated audit ADLS path. This supports validation being observable and debuggable rather than silently dropping or ignoring failed data.

---

## Azure Databricks Deployment

The batch pipeline runs on Azure Databricks and reads / writes ADLS Gen2 using service principle OAuth configuration.

Setting runtime environment variables controls whether the pipeline runs using local paths or Azure paths

```bash
HERMES_RUNTIME_ENV=azure
HERMES_STORAGE_ACCOUNT=<storage_account_name
```

The HERMES pipeline package was imported to Azure Databricks. The Databricks notebook is just a runner for the pipeline that configures runtime (set to azure), configures ADLS OAuth (allowing reads / writes to ADLS Gen2), calls HERMES package functions (that run elements of the batch pipeline), and inspects the results.

The actual pipeline logic and data contracts remain inside the Python package, making it easy to iterate upon locally with prototyping and testing, and reusable.

---

## Note on Gold Storage

The silver tables are registered as external Unity Catalog tables over Delta files in ADLS Gen2.

The gold models are currently materialised bt dbt at Unity Catalog managed tables under `dbw_hermes_dev_9s5nbox.gold`. This means that the gold tables are visible and queryable in Databricks Unity Catalog, but are not present as files in the ADLS gold container.

One of the future enhacement I intend for this project is to materialise gold as external Delta tables in the ADLS gold container.

---

## Local Development

Create and activate the environment:

```bash
conda activate HERMES-env
```

Install the project:

```bash
pip install -e ".[dev,spark,dbt]"
```

Run tests:

```bash
pytest
```

Linting and formatting:

```bash
ruff check . --fix
ruff format .
```

Running full local batch pipeline:

```bash
hermes-run-batch-local
```

---


## dbt Usage

dbt commands are to be executed in the `dbt_hermes` directory.

Install dbt packages with:

```bash
dbt deps
```

Verify Databricks connection is correct:

```bash
dbt debug
```

Parse and compile with:

```bash
dbt parse
dbt compile
```

Run dbt models with:

```bash
dbt run --full-refresh
```

Run dbt tests:

```bash
dbt test
```

Generate documentation with:

```bash
dbt docs generate
dbt docs serve
```

---


## Future Work

The batch pipeline is complete. The next part of the project will be introducing streaming pipelines.

Planned streaming implementation:

- Event Hubs or Kafka source
- Databricks Auto Loader or Structures Streaming
- Streaming bronze ingestion
- Streaming silver transformation
- Checkpointing
- Late arrival data handling
- Streaming data validation 
- Databricks Workflows or Azure Data Factory orchestration

Other improvements planned:

- Materalise gold as external Delta tables in ADLS gold
- CI/CD for Terraform and dbt
- Stricter permissions
- Cost control documentation
- Power BI Dashboard

---
