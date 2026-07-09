# HERMES Cloud Storage Layout

The Azure cloud infrastructure includes storage with the following cloud path conventions:

```text
abfss://landing@<storage_account>.dfs.core.windows.net/hermes/raw/
abfss://bronze@<storage_account>.dfs.core.windows.net/hermes/bronze/
abfss://silver@<storage_account>.dfs.core.windows.net/hermes/silver/
abfss://gold@<storage_account>.dfs.core.windows.net/hermes/gold/
abfss://quarantine@<storage_account>.dfs.core.windows.net/hermes/quarantine/
abfss://audit@<storage_account>.dfs.core.windows.net/hermes/audit/
```

For privacy the storage account name is not included in these references.

This cloud storage layout mirrors that of the local environment:

- raw source retail data
- bronze data
- silver data
- gold data
- quarantine 
- data audits

---

The storage output name is used to upload the raw source retail data to Azure:

```bash
az storage blob upload-batch \
  --account-name "$HERMES_STORAGE_ACCOUNT" \
  --destination landing \
  --destination-path hermes/raw \
  --source data/sample/raw \
  --auth-mode login \
  --overwrite
```

For validation that upload was succesful I also ran:

```bash
az storage blob list \
  --account-name "$HERMES_STORAGE_ACCOUNT" \
  --container-name landing \
  --prefix hermes/raw \
  --auth-mode login \
  --output table
```