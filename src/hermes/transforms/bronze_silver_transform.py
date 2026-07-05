from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, SparkSession, functions
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from hermes.utils.logging import config_logging
from hermes.utils.paths import hermes_bronze_dir, hermes_silver_dir
from hermes.utils.spark import create_local_spark_session


@dataclass(frozen=True)
class SilverTransformationMeta:
    table_name: str
    silver_path: Path
    row_count: int
    column_count: int


CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("address", StringType(), True),
        StructField("loyalty_tier", StringType(), True),
        StructField("creation_datetime", TimestampType(), True),
        StructField("is_active", BooleanType(), True),
    ]
)

STORES_SCHEMA = StructType(
    [
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("store_format", StringType(), True),
        StructField("opening_date", DateType(), True),
        StructField("is_active", BooleanType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("sku", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("cost_price", DoubleType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("creation_datetime", TimestampType(), True),
    ]
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("currency", StringType(), True),
        StructField("gross_amount", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("net_amount", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("created_by_system", StringType(), True),
        StructField("source_file_date", DateType(), True),
        StructField("customer_note", StringType(), True),
    ]
)

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), True),
        StructField("line_number", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("gross_amount", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("net_amount", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("inventory_snapshot_id", StringType(), False),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("snapshot_date", DateType(), True),
        StructField("stock_on_hand", IntegerType(), True),
        StructField("reorder_point", IntegerType(), True),
        StructField("reorder_quantity", IntegerType(), True),
        StructField("is_stockout", BooleanType(), True),
        StructField("is_below_reorder_point", BooleanType(), True),
    ]
)

PROMOTIONS_SCHEMA = StructType(
    [
        StructField("promotion_id", StringType(), False),
        StructField("promotion_name", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("discount_pct", IntegerType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("channel", StringType(), True),
        StructField("is_active", BooleanType(), True),
    ]
)


SILVER_SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "stores": STORES_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "inventory_snapshots": INVENTORY_SCHEMA,
    "promotions": PROMOTIONS_SCHEMA,
}

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "stores": ["store_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "order_items": ["order_item_id"],
    "inventory_snapshots": ["inventory_snapshot_id"],
    "promotions": ["promotion_id"],
}

DEDUP_ORDER_COLUMNS = {
    "customers": ["creation_datetime"],
    "stores": [],
    "products": ["creation_datetime"],
    "orders": ["order_timestamp", "source_file_date"],
    "order_items": [],
    "inventory_snapshots": ["snapshot_date"],
    "promotions": ["start_date", "end_date"],
}


def read_bronze_table(spark: SparkSession, table_name: str, bronze_base_dir: Path) -> DataFrame:
    """

    Reads the LOCAL bronze tables, this naturally will be expanded upon later when cloud
    storage is introduced.

    """

    path = bronze_base_dir / table_name / f"{table_name}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Bronze table does not exist: {path}")

    return spark.read.parquet(str(path))


def _cast_to_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """

    Reshapes the unclean bronze dataframe to match the silver schemas, namely by
    casting data types to the types explicitly enforced in the silver schemas.

    This is the basis of enforcing a schema contract.

    If a silver scheme column is not present in bronze data, it is still preserved
    as a column in the silver data, just filled with nulls so the expected
    silver columns are still present.

    returns a dataframe with only the columns defined in the silver schemas

    """

    columns = []

    for field in schema.fields:
        if field.name in df.columns:
            columns.append(functions.col(field.name).cast(field.dataType).alias(field.name))
        else:
            columns.append(functions.lit(None).cast(field.dataType).alias(field.name))

    return df.select(*columns)


def _add_silver_metadata(df: DataFrame) -> DataFrame:
    """

    Adding silver metadata columns for data lineage purposes, essentially replacing
    the bronze metadata columns that are dropped in the _cast_to_schema function call
    since the bronze metadata columns are not included in the silver schema control and are
    dropped.

    """

    return df.withColumn("_silver_processed_at", functions.current_timestamp()).withColumn("_silver_processing_date", functions.current_date())


def _deduplicate(df: DataFrame, primary_keys: list[str], order_by_cols: list[str] | None = None) -> DataFrame:
    """

    Drops duplicates in the primary key of the tables (drops oldest duplicate as priority):

    - Missing primary keys are validated to exist
    - Finds ustable timestamp / data ordering columns such as creation_datetime or
        _bronze_ingested_at which helps determine which duplicate is more recent
    - If no data ordering columns are avaliable, the duplicate is dropped with no regard
        to its recency (weaker deduplication but better than nothing)
    - If a valid data ordering column is present, creates a window that groups rows
        by primary key, and sorts each group by timestamp / data ordering column from the
        newest, to oldest
    - Removes duplicate primary key that is oldest

    """

    missing_primary_keys = [col for col in primary_keys if col not in df.columns]

    if missing_primary_keys:
        raise ValueError(f"Missing primary key columns fro deduplication: {missing_primary_keys}")

    order_cols_in_df = [col for col in (order_by_cols or []) if col in df.columns]

    if not order_cols_in_df:
        logger.warning("No valid timestamp/ordering columns found for deduplication purposesNow falling back to dropping duplicates on primary key: {}", primary_keys)

        return df.dropDuplicates(primary_keys)

    window = Window.partitionBy(*[functions.col(col) for col in primary_keys]).orderBy(*[functions.col(col).desc_nulls_last() for col in order_cols_in_df])

    return df.withColumn("_deduplication_row_number", functions.row_number().over(window)).filter(functions.col("_deduplication_row_number") == 1).drop("_deduplication_row_number")


def transform_bronze_to_silver_table(
    spark: SparkSession,
    table_name: str,
    bronze_base_dir: Path,
    silver_base_dir: Path,
) -> SilverTransformationMeta:

    if table_name not in SILVER_SCHEMAS:
        raise ValueError(f"No Silver schema configured for table: {table_name}")

    logger.info(f"Transforming Bronze to Silver: {table_name}")

    bronze_df = read_bronze_table(spark, table_name, bronze_base_dir)

    """
    
    Bronze to silver transformation chain:

    silver_df is the product of a chain of spark transformations:

    - the bronze dataframe is cast to the correct silver schema for each table in bronze
    - this is then deduplicated ensuring primary key uniqueness
    - silver layer metadata is then appended to the dataframe
    
    The silver data is then written to Delta Lake format. This is essentially parquet with
    extra metadata information on top such as: transaction history, schema management,
    safer overwrites, ACID guarantees, etc...

    This is much more safe and appropriate for modern data lakehouses

    Parquet are essentially files in a filing cabinet, hold information but not 
    organised and subject to error or misplacement

    Delta lake is the filing cabinet, with a librarian who tracks every change in a 
    ledger.

    """

    silver_df = (
        bronze_df.transform(lambda df: _cast_to_schema(df, SILVER_SCHEMAS[table_name]))
        .transform(lambda df: _deduplicate(df=df, primary_keys=PRIMARY_KEYS[table_name], order_by_cols=DEDUP_ORDER_COLUMNS.get(table_name, [])))
        .transform(_add_silver_metadata)
    )

    output_path = silver_base_dir / table_name

    """
    
    mode("overwrite") and option("overwriteSchema", "true") replaces the
    existing silver tables when the pipeline is re-run, and if the schema
    has changed then the existing scheme is overwtitten.

    For now I am keeping this, however down the line I intend to append new
    records rather than overwrite, merge/upsert, partition overwrite and
    use incremental processing.

    Overwritting schema is fine for now as the project is still evolving and
    the schema may be subject to change, so no need to have a super strong
    grip on the current schema shapes.
    
    """

    (silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(output_path)))

    row_count = silver_df.count()
    column_count = len(silver_df.columns)

    logger.info(f"Wrote Silver table {table_name}: {row_count:,} rows to {output_path}")

    return SilverTransformationMeta(
        table_name=table_name,
        silver_path=output_path,
        row_count=row_count,
        column_count=column_count,
    )


def transform_all_bronze_to_silver(
    spark: SparkSession | None = None,
    bronze_base_dir: Path | None = None,
    silver_base_dir: Path | None = None,
) -> list[SilverTransformationMeta]:

    config_logging()

    spark = spark or create_local_spark_session(name="HERMES Bronze to Silver")
    bronze_base_dir = bronze_base_dir or hermes_bronze_dir()
    silver_base_dir = silver_base_dir or hermes_silver_dir()

    results = []

    for table_name in SILVER_SCHEMAS:
        result = transform_bronze_to_silver_table(
            spark=spark,
            table_name=table_name,
            bronze_base_dir=bronze_base_dir,
            silver_base_dir=silver_base_dir,
        )
        results.append(result)

    return results


def main() -> None:
    transform_all_bronze_to_silver()


if __name__ == "__main__":
    main()
