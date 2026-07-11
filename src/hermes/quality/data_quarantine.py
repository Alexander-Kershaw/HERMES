from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, functions

from hermes.config.paths import join_uri
from hermes.config.runtime import HermesRuntimeSettings, retrieve_hermes_runtime_settings
from hermes.utils.paths import hermes_quarantine_dir

"""

Refactor is much the same as in bronze ingestion, silver transformation and contract validators:

- Change Path to string paths

"""


@dataclass(frozen=True)
class HermesQuarantineResult:
    table_name: str
    column_name: str
    rule_name: str
    quarantine_path: str
    row_count: int


def check_if_azure_cloud_path(path: str | Path) -> bool:
    return str(path).startswith("abfss://")


def _resolve_quarantine_base_path(base_quarantine_path: str | Path | None = None) -> str:

    if base_quarantine_path is not None:
        return str(base_quarantine_path)

    runtime_settings: HermesRuntimeSettings = retrieve_hermes_runtime_settings()

    if runtime_settings.quarantine_path:
        return runtime_settings.quarantine_path

    return str(hermes_quarantine_dir())


def make_quarantine_path(table_name: str, column_name: str, rule_name: str, base_quarantine_path: str | Path | None = None) -> str:

    base_quarantine_path = _resolve_quarantine_base_path(base_quarantine_path=base_quarantine_path)

    safe_col_name: str = column_name.replace(".", "_")
    safe_rule_name: str = rule_name.replace(".", "_")

    return join_uri(base_quarantine_path, "silver", table_name, f"{safe_rule_name}_{safe_col_name}")


def write_failed_record_to_quarantine(
    failed_records: DataFrame, table_name: str, column_name: str, rule_name: str, failure_reason: str, base_quarantine_path: str | Path | None = None
) -> HermesQuarantineResult:

    quarantine_output_path: str = make_quarantine_path(table_name=table_name, column_name=column_name, rule_name=rule_name, base_quarantine_path=base_quarantine_path)

    quarantine_df: DataFrame = (
        failed_records.withColumn("_quarantine_table_name", functions.lit(table_name))
        .withColumn("_quarantine_column_name", functions.lit(column_name))
        .withColumn("_quarantine_rule_name", functions.lit(rule_name))
        .withColumn("_quarantine_failure_reason", functions.lit(failure_reason))
        .withColumn("_quarantine_time", functions.current_timestamp())
        .withColumn("_quarantine_date", functions.current_date())
    )

    failure_row_count: int = quarantine_df.count()

    if failure_row_count == 0:
        logger.info(f"No quarantined records for {table_name}.{column_name} with rule {rule_name}")

        return HermesQuarantineResult(table_name=table_name, column_name=column_name, rule_name=rule_name, quarantine_path=quarantine_output_path, row_count=0)

    if not check_if_azure_cloud_path(path=quarantine_output_path):
        Path(quarantine_output_path).parent.mkdir(parents=True, exist_ok=True)

    (quarantine_df.write.format("parquet").mode("overwrite").save(quarantine_output_path))

    logger.warning(
        f"Quarantined records count: {failure_row_count} For table.column: {table_name}.{column_name} with validation rule {rule_name} Quarantine records written to: {quarantine_output_path}"
    )

    quarantine_result = HermesQuarantineResult(table_name=table_name, column_name=column_name, rule_name=rule_name, quarantine_path=quarantine_output_path, row_count=failure_row_count)

    return quarantine_result
