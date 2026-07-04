# HERMES Delivery Plan

## Delivery approach

HERMES will be delivered incrementally.

I intend to be very thorough and ensure the following for each stage of development:

- working code or documentation
- tests where applicable
- updated devlog
- trade offs documented

## Stage 0: Project foundation

Create the repository structure, development tooling, initial documentation, ADRs, and Git workflow.

Deliverables:

- project skeleton
- README
- pyproject.toml
- Makefile
- .gitignore
- .env.example
- project brief
- delivery plan
- first ADR
- first devlog

## Stage 1: Retail data generation

Create synthetic retail source data for customers, products, stores, orders, order items, inventory, promotions, and events.

Deliverables:

- data generation scripts
- sample data
- source system documentation
- initial data dictionary

Note: I will not be obsessing over the simulation as this would be a complete time sink. The purpose of the project is data engineering, not retail simulation. I will employ whatever tools necessary to get an appropriate retail data simulation and work from there. I am not developing a retail digital twin.

## Stage 2: Local lakehouse

Build a local Bronze/Silver/Gold lakehouse using PySpark and Delta Lake.

Deliverables:

- Bronze ingestion
- Silver cleaning and conformance
- Gold prototype tables
- local lakehouse documentation
- initial pipeline tests

## Stage 3: Data quality framework

Implement data contracts, validation rules, quarantine handling, and audit outputs.

Deliverables:

- YAML data contracts
- PySpark validation framework
- quarantine outputs
- data quality documentation
- data quality tests

## Stage 4: dbt Gold modelling

Use dbt to create governed Gold-layer retail marts.

Deliverables:

- dbt staging models
- dbt intermediate models
- dbt mart models
- dbt tests
- dbt documentation
- dimensional model documentation

## Stage 5: Local streaming

Build a local Kafka-compatible event streaming pipeline using Redpanda or Kafka.

Deliverables:

- event producer
- local streaming infrastructure
- streaming Bronze ingestion
- streaming Silver processing
- streaming documentation

## Stage 6: Azure infrastructure

Provision Azure infrastructure using Terraform.

Deliverables:

- resource group
- ADLS Gen2 storage
- Data Factory
- Event Hubs
- Databricks workspace
- Key Vault
- Log Analytics
- cloud architecture documentation
- cost control documentation

## Stage 7: Azure batch deployment

Deploy the batch lakehouse pipeline to Azure.

Deliverables:

- ADF batch pipeline
- ADLS Bronze/Silver/Gold layout
- Databricks jobs
- dbt execution on Databricks
- batch runbook

## Stage 8: Azure streaming deployment

Deploy streaming ingestion and processing using Azure Event Hubs and Databricks Structured Streaming.

Deliverables:

- Event Hubs producer
- Databricks streaming job
- checkpointing
- bad event handling
- streaming runbook

## Stage 9: Performance and observability

Measure, tune, and document platform behaviour.

Deliverables:

- Spark explain plan examples
- partitioning strategy
- Delta optimisation notes
- runtime benchmarks
- audit logs
- observability documentation
- incident playbook

## Stage 10: Final polish

Prepare the project for portfolio presentation and interview discussion.

Deliverables:

- final README
- architecture diagrams
- screenshots
- interview notes
- lessons learned


---

## Delivery strategy update

I've decided to restructure to project to prioritise full batch deployment first.

The batch retail lakehouse will be completed locally and then deployed to Azure before any streaming is introduced.

This enables me to be confident in the foundations first and the transiation from local development to Azure, before extending to event processing.

---
