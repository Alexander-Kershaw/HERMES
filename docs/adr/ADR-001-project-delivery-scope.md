# ADR-001: Project Scope and Delivery Strategy

## Date

2026-07-01

## Context

This project is largely based off a job description that requires strong capability across Azure, Databricks, PySpark, Spark SQL, Kafka, Delta Lake, ETL/ELT design, lakehouse architecture, data quality, governance, CI/CD, infrastructure as code, retail data solutions, and technical leadership.

Admittedly, a single portfolio project cannot honestly prove extensive enterprise leadership experience (obviously). However, it can demonstrate architecture judgement, delivery structure, documentation discipline, and practical implementation across the required technical areas.

I want this project first and foremost to be both a technical flex and an authentic exploration of modern data engineering practices. Hopefully, demonstrating the technical knowledge, and ability to consider trade offs between different tools and technical approaches.

## Decision

Build HERMES as a real time retail lakehouse project using a local first, and Azure later delivery strategy.

The project will simulate an omnichannel retail data modernisation initiative and include:

- batch retail data ingestion
- Kafka  event streaming
- Bronze/Silver/Gold lakehouse design
- PySpark and Spark SQL transformations
- Delta Lake storage
- dbt Gold layer modelling
- data contracts and quality validation
- Azure Data Factory orchestration
- Azure Event Hubs streaming ingestion
- Azure Databricks processing
- Terraform infrastructure
- CI/CD and engineering standards
- documentation, runbooks, ADRs, and devlogs

## Rationale

This scope provides strong alignment with the target role I found while remaining feasible as a solo project.

The retail domain was selected because the job description that took my interest explicitly references scalable retail data solutions. Retail provides natural examples for batch data, streaming events, dimensional modelling, stock monitoring, promotion analytics, and KPIs.

A local first approach is most logical since it reduces cloud cost risk and forces the data engineering logic to work before managed cloud services are introduced. 

## Alternatives considered

### Build only a batch lakehouse

Rejected as the final scope because it would miss the real time/Kafka signal which I ideally want.

### Build only a streaming project

Rejected because enterprise retail platforms still rely heavily on batch ingestion, dimensional modelling, and historical analytics.

### Use AWS instead of Azure

Rejected for this project because I already have projects using AWS and want to try something different.

### Use dbt as the main transformation layer

Rejected because the role requires strong Spark and distributed processing capability so I want to explicitly preserve that. dbt will still be used for Gold layer modelling, not for raw ingestion or complex distributed processing.

### Use every listed Azure service immediately

Rejected because this would likely be expensive and not necessary. Azure services will be introduced only when they solve a specific engineering problem rather than just floating around costing money while being useless.