# HERMES - Project Brief

## Purpose

HERMES is a real time retail lakehouse on Azure Databricks and is a data engineering portfolio project intended to demonstrate the design, development, and delivery of a modern retail lakehouse platform. This project will primarily use Azure, Databricks, PySpark, Delta Lake, dbt, and streaming data pipelines.

Overall, this is a simulation of an organisations cloud data modernisation initiative for a fictional retail business.

## Key Skills on Display

This project is designed to demonstrate my exposure and capability in delivering the following:

- Azure data engineering
- Databricks development
- PySpark and Spark SQL
- Batch and streamed data pipelines
- Kafka event streaming
- Delta Lake and lakehouse medallion architecture
- ETL / ELT pipeline design
- Data modelling in the context of retail
- Data quality and governance
- CI/CD and infrastructure as code
- Technical documentation and delivery planning

## Project Context

A retailer current is receiving fragmented data from a multitude of operational systems:

- Online orders
- Store level sales
- Product catalogues
- Customer records
- Inventory snapshots
- Promotion records
- Web and basket events
- Stock movement events

This is obviously not ideal for a modern day retailer. Thus, the retailer wants a governed data lakehouse that supports both historical data analytics and real time operational business intelligence insights.

## Core Aspects Supported By HERMES

The core of what HERMES supports is business intelligence, both historically, and in the present moment. Therefore, questions such as the following will be supported by the platform:

- What are the daily sales by channel, store, and product category?
- Which products are most at risk of being out of stock?
- How does promotional activities impact revenue?
- What is the conversion funnel from product view, to the confirmation of an order?
- What is the real time order volume by channel?
- Which specific stores or product categories are underperforming?
- Where are the data quality issues that tend to occur in the pipeline?

## Technical Scope

This project will include:

- Generation of synthetic retail data
- Local medallion lakehouse development
- PySpark transformation
- Delta Lake tables
- Data contracts and validation
- dbt gold layer data marts / models
- Local kafka event streaming
- Axure data factory batch orchestration
- Azure event hubs streaming ingestion
- Azure databricks jobs and workflows
- Terraform infrastructure
- CI/CD checks
- Documentation, runbooks, devlogs...

I will not be attempting things too far outside the scope of a portfolio project such as:

- Building a full production retail application
- Simulating every single possible retail domain process
- Use every Azure service
- Prioritise dashboards over data engineering credibility
- Run expensive Azure / databricks servies without good reason

