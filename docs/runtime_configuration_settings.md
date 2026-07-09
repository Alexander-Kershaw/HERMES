# HERMES Runtime Configuration

## Overview

Hermes supports two runtime environments:

- `local`
- `azure`

The runtime environment controls where the pipeline reads and writes lakehouse data. Either locally or to Azure depending on the values of the `HERMES_RUNTIME_ENV`.

## Local mode

Local mode is the default.

```bash
HERMES_RUNTIME_ENV=local
```

This deals with local paths:

```bash
data/sample/raw
data/lakehouse/bronze
data/lakehouse/silver
data/lakehouse/gold
data/quarantine
data/audit
```

## Azure mode

Instead uses ADLS Gen2 paths and requires the following env variables defined:

```bash
HERMES_RUNTIME_ENV=azure
HERMES_STORAGE_ACCOUNT=<storage-account-name>
```

Azure paths:

```bash
abfss://landing@<storage>.dfs.core.windows.net/hermes/raw
abfss://bronze@<storage>.dfs.core.windows.net/hermes/bronze
abfss://silver@<storage>.dfs.core.windows.net/hermes/silver
abfss://gold@<storage>.dfs.core.windows.net/hermes/gold
abfss://quarantine@<storage>.dfs.core.windows.net/hermes/quarantine
abfss://audit@<storage>.dfs.core.windows.net/hermes/audit
```

## Design Choice

To avoid maintaining seperate local and cloud date pipeline implementations, I am using a singular pipeline codebase with runtime specific paths defined from env variables.

The local pipeline runner is still useful for core development and testing, while Azure mode allows the same transformation logic to run against ADLS Gen2.

---

