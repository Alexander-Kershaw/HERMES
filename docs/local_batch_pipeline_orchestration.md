# HERMES Local Batch Orchestration

## Overview

I have included a local batch orchestration command that runs the batch lakehouse pipeline full from end to end.

The command is:

```bash
hermes-run-batch-pipeline-local
```

This command executes all the batch pipeline stages locally:

- Synthetic retail source data generation
- Bronze ingestion
- Bronze to Silver transformation
- Silver column level validation
- Silver table relationship validation
- Quarantined record summary
- dbt project parsing

This local orchestration provides a repeatable batch pipeline before migrating it to Azure cloud deployment.

This provides a clear migration path from local batch pipeline runner to:

- Azure Data Factory pipeline
- Databricks jobs
- dbt tasks

Note that the local runner uses `dbt parse` rather than a full `dbt run` because full dbt execution is intended for Databricks once the silver delta tables are registered in the cloud environment.

## Pipeline audit output

Each run of the batch pipeline writes a CSV audit file to `data/audit/` which records the following:

- stage name
- status
- start time
- finish time
- duration
- details

---