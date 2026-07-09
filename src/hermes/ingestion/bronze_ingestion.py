from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from loguru import logger

from hermes.config.paths import join_uri
from hermes.config.runtime import HermesRuntimeSettings, retrieve_hermes_runtime_settings
from hermes.utils.logging import config_logging
from hermes.utils.paths import hermes_bronze_dir, hermes_data_audit_dir, raw_sample_data_dir

"""
====================================================================================================
REFACTOR NOTES:
====================================================================================================

The refactor intends to allow the module to achieve bronze ingestion for both local and azure cloud
environments, but the distinction is that the logic between the two branches internally:

- Local paths maintain using pandas for reads and writes

- Azure paths (abfss paths) uses spark for reads and writes (since I will be running with azure 
databricks that uses Spark)

Furthermore, Path cannot be used for cloud URIs (paths to Azure storage containers) so functions
used for Azure mode will be using str paths rather than using the pathlib Paths. Local versions of
key functions will use Path exclusively when needed.

Also changed audit writing logic to write depending on Azure or Local mode being active.
====================================================================================================
"""


@dataclass(frozen=True)
class BronzeIngestionMeta:
    source_name: str
    source_path: str
    bronze_path: str
    row_count: int
    column_count: int
    ingestion_datetime: datetime


@dataclass(frozen=True)
class BronzeIngestionConfig:
    source_dir: str | None = None
    output_dir: str | None = None
    audit_dir: str | None = None
    file_format: str = "parquet"


DATA_SOURCE_FILES: dict[str, str] = {
    "customers": "customers.csv",
    "stores": "stores.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "inventory_snapshots": "inventory_snapshots.csv",
    "promotions": "promotions.csv",
}

"""Logic that forks off Cloud and Local executions"""


def check_if_azure_cloud_path(path: str | Path) -> bool:
    return path.startswith("abfss://")


def bronze_ingestion_config_setting() -> BronzeIngestionConfig:
    """

    Depending on defined Hermes environment variables returns bronze
    ingestion config with local paths or Azure ADLS Gen2 paths.

    """

    runtime_settings: HermesRuntimeSettings = retrieve_hermes_runtime_settings()

    return BronzeIngestionConfig(source_dir=runtime_settings.raw_source_data_path, output_dir=runtime_settings.bronze_path, audit_dir=runtime_settings.audit_path, file_format="parquet")


"""

Resolving directories now use str instead of Path so Azure mode using cloud URIs
can work without changing much of the functional logic within this module.

"""


def _resolve_source_dir(config: BronzeIngestionConfig) -> str:
    return config.source_dir or str(raw_sample_data_dir())


def _resolve_output_dir(config: BronzeIngestionConfig) -> str:
    return config.output_dir or str(hermes_bronze_dir())


def _resolve_audit_dir(config: BronzeIngestionConfig) -> str:
    return config.audit_dir or str(hermes_data_audit_dir())


def _read_source_csv_file_local(source_data_path: str) -> pd.DataFrame:
    """

    Note: changed name to explicitly mention local, as mentioned before, pandas dataframes
    are not used for Azure mode logic, Spark is used instead for that.

    """

    local_source_data_path = Path(source_data_path)

    if not local_source_data_path.exists():
        logger.error(f"Source file does not exist: {local_source_data_path}")
        raise FileNotFoundError(f"Source file does not exist: {local_source_data_path}")

    return pd.read_csv(local_source_data_path)


def _attach_bronze_metadata_local(source_df: pd.DataFrame, source_name: str, source_path: str, ingestion_datetime: datetime) -> pd.DataFrame:

    bronze_df: pd.DataFrame = source_df.copy()

    bronze_df["_bronze_source_name"] = source_name
    bronze_df["_bronze_source_file"] = Path(source_path).name
    bronze_df["_bronze_source_path"] = str(source_path)
    bronze_df["_bronze_ingested_at"] = ingestion_datetime.isoformat()
    bronze_df["_bronze_ingestion_date"] = ingestion_datetime.date().isoformat()

    return bronze_df


def _write_bronze_table_local(
    df: pd.DataFrame,
    output_dir: str,
    source_name: str,
    file_format: str,
) -> str:

    bronze_table_dir: Path = Path(output_dir) / source_name
    bronze_table_dir.mkdir(parents=True, exist_ok=True)

    if file_format == "parquet":
        output_path: Path = bronze_table_dir / f"{source_name}.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    if file_format == "csv":
        output_path = bronze_table_dir / f"{source_name}.parquet"
        df.to_csv(output_path, index=False)
        return output_path

    logger.error(f"Unsupported bronze file format: {file_format}")
    raise ValueError(f"Unsupported bronze file format: {file_format}")


def _ingest_source_to_bronze_layer_local(
    source_name: str,
    source_path: str,
    output_dir: str,
    file_format: str = "parquet",
) -> BronzeIngestionMeta:

    ingestion_datetime: datetime = datetime.now(tz=UTC)

    source_data_df: pd.DataFrame = _read_source_csv_file_local(source_path)
    bronze_data_df: pd.DataFrame = _attach_bronze_metadata_local(source_data_df, source_name, source_path, ingestion_datetime)

    bronze_data_path: str = _write_bronze_table_local(bronze_data_df, output_dir, source_name, file_format)

    bronze_ingestion_result = BronzeIngestionMeta(
        source_name=source_name, source_path=source_path, bronze_path=bronze_data_path, row_count=len(bronze_data_df), column_count=len(bronze_data_df.columns), ingestion_datetime=ingestion_datetime
    )

    logger.info(f"INGESTED {source_name}:{bronze_ingestion_result.row_count:,} rows, {bronze_ingestion_result.column_count:,} columns ->>> {bronze_data_path}")

    return bronze_ingestion_result


def _ingest_source_to_bronze_layer_spark(source_name: str, source_path: str, output_dir: str, file_format: str = "parquet") -> BronzeIngestionMeta:
    """

    Azure mode version of bronze ingestion using spark intended for Databricks / ADLS execution

    """

    from pyspark.sql import SparkSession, functions

    ingestion_datetime: datetime = datetime.now(tz=UTC)

    spark = SparkSession.builder.getOrCreate()

    source_data_df = spark.read.option("header", True).option("inferSchema", True).csv(source_path)

    bronze_df = (
        source_data_df.withColumns("_bronze_source_name", functions.lit(source_name))
        .withColumns("_bronze_source_file", functions.lit(source_path.rstrip("/").split("/")[-1]))
        .withColumns("_bronze_source_path", functions.lit(source_path))
        .withColumns("_bronze_ingested_at", functions.lit(ingestion_datetime.isoformat(())))
        .withColumns("_bronze_ingestion_date", functions.lit(ingestion_datetime.date().isoformat()))
    )

    bronze_table_path: str = join_uri(output_dir, source_name)

    bronze_df.write.format(file_format).mode("overwrite").save(bronze_table_path)

    row_count: int = bronze_df.count()
    column_count: int = len(bronze_df.columns)

    bronze_ingestion_result = BronzeIngestionMeta(
        source_name=source_name, source_path=str(source_path), bronze_path=bronze_table_path, row_count=row_count, column_count=column_count, ingestion_datetime=ingestion_datetime
    )

    logger.info(
        f"INGESTED {source_name}: Row count: {bronze_ingestion_result.row_count}, Column Count: {bronze_ingestion_result.column_count}, Ingested to Azure storage container: {bronze_table_path}"
    )

    return bronze_ingestion_result


def ingest_source_data_to_bronze_layer(source_name: str, source_path: str, output_dir: str, file_format: str = "parquet") -> BronzeIngestionMeta:
    """

    Ingestion of one source data file into the bronze layer, the logic forks depending on if
    environment is configured to local or azure mode:

    - Local paths use pandas
    - Cloud paths use Spark

    """

    if check_if_azure_cloud_path(source_path) or check_if_azure_cloud_path(output_dir):
        return _ingest_source_to_bronze_layer_spark(source_name=source_name, source_path=source_path, output_dir=output_dir, file_format=file_format)

    return _ingest_source_to_bronze_layer_local(source_name=source_name, source_path=source_path, output_dir=output_dir, file_format=file_format)


def write_ingestion_audit(results: list[BronzeIngestionMeta], audit_dir: str) -> str:

    audit_dir = audit_dir or str(hermes_data_audit_dir())

    bronze_audit_df = pd.DataFrame(
        [
            {
                "source_name": result.source_name,
                "source_path": str(result.source_path),
                "bronze_path": str(result.bronze_path),
                "row_count": result.row_count,
                "column_count": result.column_count,
                "ingestion_datetime": result.ingestion_datetime.isoformat(),
            }
            for result in results
        ]
    )

    """Azure Mode"""
    if check_if_azure_cloud_path(audit_dir):
        from pyspark import SparkSession

        spark = SparkSession.builder.getOrCreate()
        audit_output_path: str = join_uri(audit_dir, "bronze_ingestion_audit")

        (spark.createDataFrame(bronze_audit_df).write.mode("overwrite").option("header", True).csv(audit_output_path))

        logger.info(f"Wrote bronze ingestion audit to: {audit_output_path}")

        return audit_output_path

    """Local Mode"""
    audit_output_dir = Path(audit_dir)
    audit_output_dir.mkdir(parents=True, exist_ok=True)

    audit_output_path = audit_output_dir / "bronze_ingestion_audit.csv"
    bronze_audit_df.to_csv(audit_output_path, index=False)

    logger.info(f"Wrote bronze ingestion audit to {audit_output_path}")

    return str(audit_output_path)


def full_source_ingestion_to_bronze(config: BronzeIngestionConfig | None = None) -> list[BronzeIngestionMeta]:

    bronze_config: BronzeIngestionConfig = config or bronze_ingestion_config_setting()

    source_data_dir: str = _resolve_source_dir(config=bronze_config)
    bronze_output_dir: str = _resolve_output_dir(config=bronze_config)
    audit_dir: str = _resolve_audit_dir(config=bronze_config)

    runtime_env_label = "AZURE" if check_if_azure_cloud_path(source_data_dir) else "LOCAL"

    logger.info(f"=====| STARTING {runtime_env_label} BRONZE INGESTION |=====")
    logger.info(f"Source data directory: {source_data_dir}")
    logger.info(f"Bronze data directory: {bronze_output_dir}")
    logger.info(f"Audit directory: {audit_dir}")

    bronze_results: list[BronzeIngestionMeta] = []

    for source_name, file_name in DATA_SOURCE_FILES.items():
        source_path = join_uri(source_data_dir, file_name)

        result = ingest_source_data_to_bronze_layer(source_name=source_name, source_path=source_path, output_dir=bronze_output_dir, file_format=bronze_config.file_format)

        bronze_results.append(result)

    write_ingestion_audit(results=bronze_results, audit_dir=audit_dir)

    return bronze_results


def main() -> None:
    config_logging()
    full_source_ingestion_to_bronze()


if __name__ == "__main__":
    main()
