from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, SparkSession, functions
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from hermes.config.paths import join_uri
from hermes.config.runtime import HermesRuntimeEnvironment, HermesRuntimeSettings, retrieve_hermes_runtime_settings
from hermes.utils.spark import create_local_spark_session

"""
====================================================================================================
REFACTOR NOTES:
====================================================================================================

The refactor here (like bronze ingestion) involves changing dataclass definitions to use strings
instead of Path types to allow cloud URI paths to work

Since silver transformation already uses spark there is minial refactoring for the overall
transformation structure.

====================================================================================================
"""


@dataclass(frozen=True)
class SilverTransformationConfig:
    bronze_path: str
    silver_path: str


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


def check_if_azure_cloud_path(path: str) -> bool:
    return str(path).startswith("abfss://")


def silver_transformation_config_setting() -> SilverTransformationConfig:
    """

    Dependiing on the runtime environment (local or cloud Azure) returns the
    relevant path definitions for the silver and bronze layers (either local
    directores or Azure Storage containers)

    """

    runtime_settings = retrieve_hermes_runtime_settings()

    return SilverTransformationConfig(bronze_path=runtime_settings.bronze_path, silver_path=runtime_settings.silver_path)


def _local_bronze_table_path(bronze_base_path: str, table_name: str) -> str:
    """

    Resolve local Bronze table path.

    Local Bronze ingestion currently writes:

    - data/lakehouse/bronze/<table>/<table>.parquet

    Azure Bronze ingestion writes Spark parquet directories:

    - abfss://bronze@.../hermes/bronze/<table>

    """

    file_path: Path = Path(bronze_base_path) / table_name / f"{table_name}.parquet"

    if file_path.exists():
        return str(file_path)

    directory_path: Path = Path(bronze_base_path) / table_name

    if directory_path.exists():
        return str(directory_path)

    raise FileNotFoundError(f"Bronze table does not exist for: {file_path} and {directory_path}")


def resolve_bronze_table_path(bronze_base_path: str, table_name: str) -> str:
    """

    Resolves the bronze table paths for either local or Azure runtime

    """

    if check_if_azure_cloud_path(bronze_base_path):
        return join_uri(bronze_base_path, table_name)

    return _local_bronze_table_path(bronze_base_path=bronze_base_path, table_name=table_name)


def resolve_silver_table_path(silver_base_path: str, table_name: str) -> str:
    """

    Resolves the transformed silver output table path for local or Azure runtime

    """

    return join_uri(silver_base_path, table_name)


def read_bronze_table(spark: SparkSession, table_name: str, bronze_base_path: str) -> DataFrame:
    """

    Reads the local OR Azure ADLS bronze tables (singular table).

    """

    bronze_table_path: str = resolve_bronze_table_path(bronze_base_path=bronze_base_path, table_name=table_name)

    logger.info(f"Reading Bronze table: {table_name} | from: {bronze_table_path} ")

    return spark.read.parquet(bronze_table_path)


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

    columns: list = []

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

    missing_primary_keys: list[str] = [col for col in primary_keys if col not in df.columns]

    if missing_primary_keys:
        raise ValueError(f"Missing primary key columns fro deduplication: {missing_primary_keys}")

    order_cols_in_df: list[str] = [col for col in (order_by_cols or []) if col in df.columns]

    if not order_cols_in_df:
        logger.warning("No valid timestamp/ordering columns found for deduplication purposesNow falling back to dropping duplicates on primary key: {}", primary_keys)

        return df.dropDuplicates(primary_keys)

    window = Window.partitionBy(*[functions.col(col) for col in primary_keys]).orderBy(*[functions.col(col).desc_nulls_last() for col in order_cols_in_df])

    return df.withColumn("_deduplication_row_number", functions.row_number().over(window)).filter(functions.col("_deduplication_row_number") == 1).drop("_deduplication_row_number")


def transform_bronze_to_silver_table(
    spark: SparkSession,
    table_name: str,
    bronze_base_path: str,
    silver_base_path: str,
) -> SilverTransformationMeta:

    if table_name not in SILVER_SCHEMAS:
        raise ValueError(f"No Silver schema configured for table: {table_name}")

    logger.info(f"Transforming Bronze to Silver: {table_name}")

    bronze_df = read_bronze_table(spark=spark, table_name=table_name, bronze_base_path=bronze_base_path)

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
        bronze_df.transform(lambda df: _cast_to_schema(df=df, schema=SILVER_SCHEMAS[table_name]))
        .transform(lambda df: _deduplicate(df=df, primary_keys=PRIMARY_KEYS[table_name], order_by_cols=DEDUP_ORDER_COLUMNS.get(table_name, [])))
        .transform(_add_silver_metadata)
    )

    output_path: str = resolve_silver_table_path(silver_base_path=silver_base_path, table_name=table_name)

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

    (silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path))

    row_count: int = silver_df.count()
    column_count: int = len(silver_df.columns)

    logger.info(f"Wrote Silver table {table_name}: {row_count:,} rows to {output_path}")

    return SilverTransformationMeta(
        table_name=table_name,
        silver_path=output_path,
        row_count=row_count,
        column_count=column_count,
    )


def transform_all_bronze_to_silver(spark: SparkSession | None = None, config: SilverTransformationConfig | None = None) -> list[SilverTransformationMeta]:
    """

    The main refactor here to allow local or Azure spark transformation is the distinction between
    what spark sessions to use:

    - For local, the create_local_spark_session() function is called to handle the building of the
    spark session as defined for the local workflow that works.

    - For Azure, builds a new spark session that will attach to the existing cluster compute session
    currently attached in Azure Databricks.

    """

    runtime_settings: HermesRuntimeSettings = retrieve_hermes_runtime_settings()
    config = config or silver_transformation_config_setting()

    if spark is None:
        if runtime_settings.environment == HermesRuntimeEnvironment.LOCAL_ENV:
            spark = create_local_spark_session(name="Hermes Bronze to Silver transformation")

        else:
            spark = SparkSession.builder.getOrCreate()

    logger.info("=====| STARTING BRONZE TO SILVER TRANSFORMATION |=====")
    logger.info(f"Bronze base path: {config.bronze_path}")
    logger.info(f"Silver base path: {config.silver_path}")

    results: list[SilverTransformationMeta] = []

    for table_name in SILVER_SCHEMAS:
        result: SilverTransformationMeta = transform_bronze_to_silver_table(spark=spark, table_name=table_name, bronze_base_path=config.bronze_path, silver_base_path=config.silver_path)

        results.append(result)

    return results


def main() -> None:
    transform_all_bronze_to_silver()


if __name__ == "__main__":
    main()
