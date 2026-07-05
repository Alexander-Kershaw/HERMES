from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, functions

from hermes.utils.paths import hermes_quarantine_dir


@dataclass(frozen=True)
class HermesQuarantineResult:
    table_name: str
    column_name: str
    rule_name: str
    quarantine_path: Path
    row_count: int


def make_quaratine_path(table_name: str, column_name: str, rule_name: str, base_quaratine_dir: Path | None = None) -> Path:

    base_quaratine_dir = base_quaratine_dir or hermes_quarantine_dir()
    safe_column_name = column_name.replace(".", "_")
    safe_rule_name = rule_name.replace(".", "_")

    quaratine_path = base_quaratine_dir / "silver" / table_name / f"{safe_rule_name}_{safe_column_name}.parquet"

    return quaratine_path


def write_failed_record_to_quarantine(
    failed_records: DataFrame, table_name: str, column_name: str, rule_name: str, failure_reason: str, base_quarantine_dir: Path | None = None
) -> HermesQuarantineResult:

    quaratine_output_path = make_quaratine_path(table_name=table_name, column_name=column_name, rule_name=rule_name, base_quaratine_dir=base_quarantine_dir)

    quaratine_output_path.parent.mkdir(exist_ok=True, parents=True)

    quarantine_df = (
        failed_records.withColumn("_quarantine_table_name", functions.lit(table_name))
        .withColumn("_quarantine_column_name", functions.lit(column_name))
        .withColumn("_quarantine_rule_name", functions.lit(rule_name))
        .withColumn("_quarantine_failure_reason", functions.lit(failure_reason))
        .withColumn("_quarantine_time", functions.current_timestamp())
        .withColumn("_quarantine_date", functions.current_date())
    )

    failure_row_count = quarantine_df.count()

    if failure_row_count == 0:
        logger.info(f"No quarantined records for {table_name}.{column_name} with rule {rule_name}")

        return HermesQuarantineResult(table_name=table_name, column_name=column_name, rule_name=rule_name, quarantine_path=quaratine_output_path, row_count=0)

    (quarantine_df.write.mode("overwrite").parquet(str(quaratine_output_path)))

    logger.warning(
        f"Quarantined records count: {failure_row_count} For table.column: {table_name}.{column_name} with validation rule {rule_name} Quarantine records written to: {quaratine_output_path}"
    )

    quarantine_result = HermesQuarantineResult(table_name=table_name, column_name=column_name, rule_name=rule_name, quarantine_path=quaratine_output_path, row_count=failure_row_count)

    return quarantine_result
