# ADR-002: Batch Priority Delivery Strategy

## Context

This project includes both batch and streaming data processing. The original roadmap introduced local streaming before Azure batch deployment.

However, the project is primarily a retail lakehouse modernisation project. Most enterprise platforms probably first establish governed batch ingestion, trusted silver entities, gold marts, orchestration, and observability before adding streaming extensions. Therefore, I am pivoting to prioritise full batch processing deployment first.

## Decision

Deliver the batch lakehouse first.

The project will complete:

- local batch sources
- local Bronze/Silver/Gold lakehouse
- data quality and quarantine
- local batch orchestration
- Azure batch infrastructure
- Azure batch deployment
- batch observability and optimisation

Once that is satisfied, I will then implement:

- local streaming
- streaming quality
- Azure Event Hubs
- Azure streaming deployment

## Rationale

Batch delivery as a first priority provides a cleaner architecture, stronger cloud cost control, and potentially a more realistic enterprise delivery narrative.

Streaming will be treated as an extension to the governed lakehouse rather than a parallel project from now.

---