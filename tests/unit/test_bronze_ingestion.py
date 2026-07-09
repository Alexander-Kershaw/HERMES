from pathlib import Path

import pandas as pd

from hermes.ingestion.bronze_ingestion import BronzeIngestionConfig, BronzeIngestionMeta, _ingest_source_to_bronze_layer_local, bronze_ingestion_config_setting, full_source_ingestion_to_bronze


def test_ingest_source_to_bronze_adds_metadata(tmp_path: str) -> None:
    source_path: Path = Path(tmp_path / "customers.csv")
    output_dir: Path = Path(tmp_path / "bronze")

    pd.DataFrame(
        [
            {"customer_id": "CUST-000001", "email": "test@example.com"},
            {"customer_id": "CUST-000002", "email": "test2@example.com"},
        ]
    ).to_csv(source_path, index=False)

    result: BronzeIngestionMeta = _ingest_source_to_bronze_layer_local(source_name="customers", source_path=source_path, output_dir=output_dir, file_format="parquet")

    bronze_df: pd.DataFrame = pd.read_parquet(result.bronze_path)

    assert result.row_count == 2
    assert result.source_name == "customers"
    assert result.bronze_path.exists()

    assert "_bronze_source_name" in bronze_df.columns
    assert "_bronze_source_file" in bronze_df.columns
    assert "_bronze_source_path" in bronze_df.columns
    assert "_bronze_ingested_at" in bronze_df.columns
    assert "_bronze_ingestion_date" in bronze_df.columns

    assert set(bronze_df["_bronze_source_name"]) == {"customers"}
    assert set(bronze_df["_bronze_source_file"]) == {"customers.csv"}


def test_ingest_source_to_bronze_rejects_missing_file(tmp_path: str) -> None:
    missing_source_path: Path = Path(tmp_path / "missing.csv")
    output_dir: Path = Path(tmp_path / "bronze")

    try:
        _ingest_source_to_bronze_layer_local(source_name="missing", source_path=missing_source_path, output_dir=output_dir)
    except FileNotFoundError as exc:
        assert "Source file does not exist" in str(exc)
    else:
        raise AssertionError("Expected FileNotFoundError")


def test_ingest_all_sources_to_bronze(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("HERMES_RUNTIME_ENV", "local")
    monkeypatch.delenv("HERMES_STORAGE_ACCOUNT", raising=False)

    source_dir: Path = tmp_path / "raw"
    output_dir: Path = tmp_path / "bronze"
    audit_dir: Path = tmp_path / "audit"

    source_dir.mkdir(parents=True)

    source_files: dict[str, list[dict[str, str]]] = {
        "customers.csv": [{"customer_id": "CUST-000001"}],
        "stores.csv": [{"store_id": "STORE-0001"}],
        "products.csv": [{"product_id": "PROD-000001"}],
        "orders.csv": [{"order_id": "ORDER-00000001"}],
        "order_items.csv": [{"order_item_id": "ORDER-00000001-001"}],
        "inventory_snapshots.csv": [{"inventory_snapshot_id": "INV-001"}],
        "promotions.csv": [{"promotion_id": "PROMO-0001"}],
    }

    for file_name, rows in source_files.items():
        pd.DataFrame(rows).to_csv(source_dir / file_name, index=False)

    config = BronzeIngestionConfig(source_dir=str(source_dir), output_dir=str(output_dir), audit_dir=str(audit_dir))

    results: list[BronzeIngestionMeta] = full_source_ingestion_to_bronze(config)

    assert len(results) == 7

    for result in results:
        assert Path(result.bronze_path).exists()
        assert result.row_count == 1
        assert result.column_count >= 6  # checks if orignal columns are there + metadata cols


def test_bronze_config_uses_local_paths(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_RUNTIME_ENV", "local")
    monkeypatch.delenv("HERMES_STORAGE_ACCOUNT", raising=False)

    config: BronzeIngestionConfig = bronze_ingestion_config_setting()

    assert config.source_dir == "data/sample/raw"
    assert config.output_dir == "data/lakehouse/bronze"
    assert config.audit_dir == "data/audit"
    assert config.file_format == "parquet"


def test_bronze_config_uses_azure_paths(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_RUNTIME_ENV", "azure")
    monkeypatch.setenv("HERMES_STORAGE_ACCOUNT", "sthermesdevexample")

    config: BronzeIngestionConfig = bronze_ingestion_config_setting()

    assert config.source_dir == ("abfss://landing@sthermesdevexample.dfs.core.windows.net/hermes/raw")

    assert config.output_dir == ("abfss://bronze@sthermesdevexample.dfs.core.windows.net/hermes/bronze")

    assert config.audit_dir == ("abfss://audit@sthermesdevexample.dfs.core.windows.net/hermes/audit")
