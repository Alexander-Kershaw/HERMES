from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger
from pyspark.sql import DataFrame, SparkSession, functions

from hermes.quality.contracts import HermesDataContract, load_all_yaml_contracts
from hermes.utils.logging import config_logging
from hermes.utils.paths import hermes_data_audit_dir, hermes_silver_dir
from hermes.utils.spark import create_local_spark_session


@dataclass(frozen=True)
class ContractValidationResult:
    table_name: str
    column_name: str
    rule_name: str
    passed: bool
    failed_count: int
    total_count: int
    details: str


def _normalise_contract_rule(rule: str | dict[str, Any]) -> tuple[str, Any]:

    if isinstance(rule, str):
        return rule, None

    if isinstance(rule, dict) and len(rule) == 1:
        rule_name = next(iter(rule))
        return rule_name, rule[rule_name]

    else:
        raise ValueError(f"Unsupported rule format: {rule}")


def _validate_not_null(df: DataFrame, column: str) -> int:
    return df.filter(functions.col(column).isNull()).count()


def _validate_uniqueness(df: DataFrame, column: str) -> int:
    duplicate_rows = (
        df.groupBy(column)
        .count()
        .filter((functions.col("count") > 1) & (functions.col(column).isNotNull()))
        .agg(functions.sum("count").alias("duplicate_row_count"))
        .collect()[0]["duplicate_row_count"]
    )

    return int(duplicate_rows or 0)


def _validate_min_value(df: DataFrame, column: str, min_value: int | float) -> int:
    return df.filter(functions.col(column) < functions.lit(min_value)).count()


def _validate_allowed_values(
    df: DataFrame,
    column: str,
    allowed_values: list[Any],
) -> int:
    return df.filter(functions.col(column).isNotNull() & ~functions.col(column).isin(allowed_values)).count()


def _validate_regex(df: DataFrame, column: str, pattern: str) -> int:
    return df.filter(functions.col(column).isNotNull() & ~functions.col(column).rlike(pattern)).count()


def validate_column_rule(
    df: DataFrame,
    table_name: str,
    column_name: str,
    rule: str | dict[str, Any],
) -> ContractValidationResult:

    rule_name, rule_config = _normalise_contract_rule(rule)
    total_count = df.count()

    if column_name not in df.columns:
        return ContractValidationResult(
            table_name=table_name,
            column_name=column_name,
            rule_name=rule_name,
            passed=False,
            failed_count=total_count,
            total_count=total_count,
            details=f"Column does not exist: {column_name}",
        )

    if rule_name == "not_null":
        failed_count = _validate_not_null(df, column_name)
        details = "Null check"

    elif rule_name == "unique":
        failed_count = _validate_uniqueness(df, column_name)
        details = "Uniqueness check"

    elif rule_name == "min_value":
        failed_count = _validate_min_value(df, column_name, rule_config)
        details = f"Minimum value check: >= {rule_config}"

    elif rule_name == "accepted_values":
        failed_count = _validate_allowed_values(df, column_name, rule_config)
        details = f"Accepted values: {rule_config}"

    elif rule_name == "regex":
        failed_count = _validate_regex(df, column_name, rule_config)
        details = f"Regex pattern: {rule_config}"

    else:
        raise ValueError(f"Unsupported validation rule: {rule_name}")

    return ContractValidationResult(
        table_name=table_name,
        column_name=column_name,
        rule_name=rule_name,
        passed=failed_count == 0,
        failed_count=failed_count,
        total_count=total_count,
        details=details,
    )


def validate_table(df: DataFrame, contract: HermesDataContract) -> list[ContractValidationResult]:

    results = []

    for column_name, column_config in contract.columns.items():
        for rule in column_config.get("rules", []):
            result = validate_column_rule(
                df=df,
                table_name=contract.table,
                column_name=column_name,
                rule=rule,
            )
            results.append(result)

    return results


def write_validation_report(results: list[ContractValidationResult], output_dir: Path | None = None) -> Path:

    output_dir = output_dir or hermes_data_audit_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "silver_validation_report.csv"

    report_df = pd.DataFrame(
        [
            {
                "table_name": result.table_name,
                "column_name": result.column_name,
                "rule_name": result.rule_name,
                "passed": result.passed,
                "failed_count": result.failed_count,
                "total_count": result.total_count,
                "details": result.details,
            }
            for result in results
        ]
    )

    report_df.to_csv(output_path, index=False)
    logger.info(f"Wrote Silver validation report to {output_path}")

    return output_path


def validate_silver_tables(
    spark: SparkSession | None = None,
    silver_base_dir: Path | None = None,
    contract_dir: Path | None = None,
    report_dir: Path | None = None,
) -> list[ContractValidationResult]:

    config_logging()

    spark = spark or create_local_spark_session("HERMES Silver Validation")
    silver_base_dir = silver_base_dir or hermes_silver_dir()
    contract_dir = contract_dir or Path("contracts/silver")

    contracts = load_all_yaml_contracts(contract_dir)
    all_results = []

    for table_name, contract in contracts.items():
        table_path = silver_base_dir / table_name

        if not table_path.exists():
            raise FileNotFoundError(f"Silver table does not exist: {table_path}")

        logger.info(f"Validating Silver table: {table_name}")

        df = spark.read.format("delta").load(str(table_path))
        table_results = validate_table(df, contract)
        all_results.extend(table_results)

    write_validation_report(all_results, report_dir)

    failed_results = [result for result in all_results if not result.passed]
    if failed_results:
        logger.warning(f"Silver validation completed with {len(failed_results)} failed checks")
    else:
        logger.info("Silver validation completed successfully")

    return all_results


def main() -> None:
    validate_silver_tables()


if __name__ == "__main__":
    main()
