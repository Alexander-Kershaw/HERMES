from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import functions
from pyspark.sql.types import TimestampType

from hermes.ingestion.bronze_ingestion import _ingest_source_to_bronze_layer_local
from hermes.transforms.bronze_silver_transform import (
    SilverTransformationConfig,
    SilverTransformationMeta,
    _deduplicate,
    resolve_bronze_table_path,
    resolve_silver_table_path,
    silver_transformation_config_setting,
    transform_bronze_to_silver_table,
)
from hermes.utils.spark import create_local_spark_session


def test_transform_customers_bronze_to_silver(tmp_path: Path, monkeypatch) -> None:

    monkeypatch.setenv("HERMES_RUNTIME_ENV", "local")
    monkeypatch.delenv("HERMES_STORAGE_ACCOUNT", raising=False)

    spark = create_local_spark_session("test_transform_customers_bronze_to_silver")

    source_path: Path = tmp_path / "customers.csv"
    bronze_dir: Path = tmp_path / "bronze"
    silver_dir: Path = tmp_path / "silver"

    pd.DataFrame(
        [
            {
                "customer_id": "CUST-000001",
                "first_name": "Ada",
                "last_name": "Lovelace",
                "email": "ada@example.com",
                "phone_number": "+44 09834 233455",
                "city": "London",
                "region": "Greater London",
                "postcode": "SW1A 1AA",
                "loyalty_tier": "gold",
                "creation_datetime": "2026-01-01 10:00:00",
                "is_active": True,
            },
            {
                "customer_id": "CUST-000001",
                "first_name": "Ada",
                "last_name": "Lovelace",
                "email": "ada@example.com",
                "phone_number": "+44 09834 233455",
                "city": "London",
                "region": "Greater London",
                "postcode": "SW1A 1AA",
                "loyalty_tier": "gold",
                "creation_datetime": "2026-01-01 10:00:00",
                "is_active": True,
            },
        ]
    ).to_csv(source_path, index=False)

    _ingest_source_to_bronze_layer_local(
        source_name="customers",
        source_path=str(source_path),
        output_dir=str(bronze_dir),
    )

    result: SilverTransformationMeta = transform_bronze_to_silver_table(
        spark=spark,
        table_name="customers",
        bronze_base_path=str(bronze_dir),
        silver_base_path=str(silver_dir),
    )

    silver_df = spark.read.format("delta").load(str(result.silver_path))

    assert result.row_count == 1
    assert silver_df.count() == 1
    assert "_silver_processed_at" in silver_df.columns
    assert "_silver_processing_date" in silver_df.columns


def test_deduplicate_keeps_latest_record() -> None:

    spark = create_local_spark_session(name="Test deduplication logic removes oldest duplicate")

    df = spark.createDataFrame(
        [
            ("C001", "Alex", "2023-01-01 10:00:00"),
            ("C001", "Alexander", "2023-01-02 10:00:00"),
            ("C002", "Bingus", "2023-01-01 10:00:00"),
        ],
        ["customer_id", "first_name", "creation_datetime"],
    )

    df = df.withColumn(
        "creation_datetime",
        functions.col("creation_datetime").cast(TimestampType()),
    )

    result = _deduplicate(
        df=df,
        primary_keys=["customer_id"],
        order_by_cols=["creation_datetime"],
    )

    rows: dict = {row["customer_id"]: row["first_name"] for row in result.collect()}

    assert rows["C001"] == "Alexander"
    assert rows["C002"] == "Bingus"
    assert result.count() == 2

    spark.stop()


def test_resolve_local_bronze_table_path_prefers_parquet_file(tmp_path: Path) -> None:

    bronze_base: Path = tmp_path / "bronze"
    table_dir: Path = bronze_base / "customers"
    table_dir.mkdir(parents=True)

    parquet_file: Path = table_dir / "customers.parquet"
    parquet_file.write_text("dummy")

    resolved_path: str = resolve_bronze_table_path(
        bronze_base_path=str(bronze_base),
        table_name="customers",
    )
    assert resolved_path == str(parquet_file)


def test_resolve_local_bronze_table_path_falls_back_to_directory(tmp_path: Path) -> None:

    bronze_base: Path = tmp_path / "bronze"
    table_dir: Path = bronze_base / "customers"

    table_dir.mkdir(parents=True)

    resolved_path: str = resolve_bronze_table_path(
        bronze_base_path=str(bronze_base),
        table_name="customers",
    )
    assert resolved_path == str(table_dir)


def test_resolve_local_bronze_table_path_raises_when_missing(tmp_path: Path) -> None:

    with pytest.raises(expected_exception=FileNotFoundError):
        resolve_bronze_table_path(
            bronze_base_path=str(tmp_path / "bronze"),
            table_name="customers",
        )


def test_resolve_azure_bronze_table_path() -> None:

    resolved_path: str = resolve_bronze_table_path(
        bronze_base_path="abfss://bronze@sthermes.dfs.core.windows.net/hermes/bronze",
        table_name="customers",
    )

    assert resolved_path == "abfss://bronze@sthermes.dfs.core.windows.net/hermes/bronze/customers"


def test_resolve_silver_table_path_local() -> None:

    assert resolve_silver_table_path(silver_base_path="data/lakehouse/silver", table_name="orders") == "data/lakehouse/silver/orders"


def test_resolve_silver_table_path_azure() -> None:

    assert (
        resolve_silver_table_path(
            silver_base_path="abfss://silver@sthermes.dfs.core.windows.net/hermes/silver",
            table_name="orders",
        )
        == "abfss://silver@sthermes.dfs.core.windows.net/hermes/silver/orders"
    )


def test_silver_transformation_config_uses_local_runtime(monkeypatch) -> None:

    monkeypatch.setenv("HERMES_RUNTIME_ENV", "local")
    monkeypatch.delenv("HERMES_STORAGE_ACCOUNT", raising=False)

    config: SilverTransformationConfig = silver_transformation_config_setting()

    assert isinstance(config, SilverTransformationConfig)
    assert config.bronze_path == "data/lakehouse/bronze"
    assert config.silver_path == "data/lakehouse/silver"


def test_silver_transformation_config_uses_azure_runtime(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_RUNTIME_ENV", "azure")
    monkeypatch.setenv("HERMES_STORAGE_ACCOUNT", "sthermesdevexample")

    config: SilverTransformationConfig = silver_transformation_config_setting()

    assert config.bronze_path == ("abfss://bronze@sthermesdevexample.dfs.core.windows.net/hermes/bronze")

    assert config.silver_path == ("abfss://silver@sthermesdevexample.dfs.core.windows.net/hermes/silver")
