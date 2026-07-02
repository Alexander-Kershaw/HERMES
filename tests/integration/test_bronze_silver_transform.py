from pathlib import Path

import pandas as pd
from pyspark.sql import functions
from pyspark.sql.types import TimestampType

from hermes.ingestion.bronze_ingestion import ingest_source_to_bronze_layer
from hermes.transforms.bronze_silver_transform import _deduplicate, transform_bronze_to_silver_table
from hermes.utils.spark import create_local_spark_session


def test_transform_customers_bronze_to_silver(tmp_path: Path) -> None:
    spark = create_local_spark_session("test_transform_customers_bronze_to_silver")

    source_path = tmp_path / "customers.csv"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

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

    ingest_source_to_bronze_layer(
        source_name="customers",
        source_path=source_path,
        output_dir=bronze_dir,
    )

    result = transform_bronze_to_silver_table(
        spark=spark,
        table_name="customers",
        bronze_base_dir=bronze_dir,
        silver_base_dir=silver_dir,
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

    rows = {row["customer_id"]: row["first_name"] for row in result.collect()}

    assert rows["C001"] == "Alexander"
    assert rows["C002"] == "Bingus"
    assert result.count() == 2

    spark.stop()
