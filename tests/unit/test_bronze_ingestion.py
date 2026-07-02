from pathlib import Path

import pandas as pd

from hermes.ingestion.bronze_ingestion import BronzeIngestionConfig, full_source_ingestion_to_bronze, ingest_source_to_bronze_layer


def test_ingest_source_to_bronze_adds_metadata(tmp_path: Path) -> None:
    source_path = tmp_path / "customers.csv"
    output_dir = tmp_path / "bronze"

    pd.DataFrame(
        [
            {"customer_id": "CUST-000001", "email": "test@example.com"},
            {"customer_id": "CUST-000002", "email": "test2@example.com"},
        ]
    ).to_csv(source_path, index=False)

    result = ingest_source_to_bronze_layer(source_name="customers", source_path=source_path, output_dir=output_dir, file_format="parquet")

    bronze_df = pd.read_parquet(result.bronze_path)

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


def test_ingest_source_to_bronze_rejects_missing_file(tmp_path: Path) -> None:
    missing_source_path = tmp_path / "missing.csv"
    output_dir = tmp_path / "bronze"

    try:
        ingest_source_to_bronze_layer(source_name="missing", source_path=missing_source_path, output_dir=output_dir)
    except FileNotFoundError as exc:
        assert "Source file does not exist" in str(exc)
    else:
        raise AssertionError("Expected FileNotFoundError")


def test_ingest_all_sources_to_bronze(tmp_path: Path) -> None:
    source_dir = tmp_path / "raw"
    output_dir = tmp_path / "bronze"
    source_dir.mkdir(parents=True)

    source_files = {
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

    config = BronzeIngestionConfig(source_dir=source_dir, output_dir=output_dir)

    results = full_source_ingestion_to_bronze(config)

    assert len(results) == 7

    for result in results:
        assert result.bronze_path.exists()
        assert result.row_count == 1
