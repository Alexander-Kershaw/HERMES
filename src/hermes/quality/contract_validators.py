from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger
from pyspark.sql import DataFrame, SparkSession, functions

from hermes.quality.contracts import HermesDataContract, HermesRelationshipsContract, load_all_yaml_contracts, load_table_relationship_yaml_contract
from hermes.quality.data_quarantine import HermesQuarantineResult, write_failed_record_to_quarantine
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


@dataclass(frozen=True)
class RelationContractValidationResult:
    table_relationship_name: str
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str
    passed: bool
    failed_count: int
    child_count: int
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

        quarantine_table_validation_failures(df=df, data_contract=contract, validation_results=table_results)

    write_validation_report(all_results, report_dir)

    failed_results = [result for result in all_results if not result.passed]
    if failed_results:
        logger.warning(f"Silver validation completed with {len(failed_results)} failed checks")
    else:
        logger.info("Silver validation completed successfully")

    return all_results


def validate_table_relationships(table_relationship: HermesRelationshipsContract, tables: dict[str, DataFrame]) -> RelationContractValidationResult:
    """

    For future table joins there should be corresponding parent and child keys in related tables

    So this function serves to validate that a child/foreign key exists in the parent table

    """

    if table_relationship.child_table not in tables:
        raise ValueError(f"Missing the child table for the relationship: {table_relationship.child_table}")

    if table_relationship.parent_table not in tables:
        raise ValueError(f"Missing parent table for the relationship: {table_relationship.parent_table}")

    child_table_df = tables[table_relationship.child_table]
    parent_table_df = tables[table_relationship.parent_table]

    if table_relationship.child_column not in child_table_df.columns:
        raise ValueError(f"Missing child column: {table_relationship.child_column} in table: {table_relationship.child_table}")

    if table_relationship.parent_column not in parent_table_df.columns:
        raise ValueError(f"Missing parent column: {table_relationship.parent_column} in table: {table_relationship.parent_table}")

    child_keys = child_table_df.select(functions.col(table_relationship.child_column).alias("child_key")).filter(functions.col("child_key").isNotNull()).dropDuplicates()

    parent_keys = parent_table_df.select(functions.col(table_relationship.parent_column).alias("parent_key")).filter(functions.col("parent_key").isNotNull()).dropDuplicates()

    orphan_keys = child_keys.join(parent_keys, child_keys["child_key"] == parent_keys["parent_key"], "left_anti")

    failed_count = orphan_keys.count()
    child_count = child_keys.count()

    relationship_validation_result = RelationContractValidationResult(
        table_relationship_name=table_relationship.table_relationship_name,
        child_table=table_relationship.child_table,
        child_column=table_relationship.child_column,
        parent_table=table_relationship.parent_table,
        parent_column=table_relationship.parent_column,
        passed=failed_count == 0,
        failed_count=failed_count,
        child_count=child_count,
        details=(f"{table_relationship.child_table}.{table_relationship.child_column}column must exist in {table_relationship.parent_column}.{table_relationship.parent_column}"),
    )

    return relationship_validation_result


def write_relationship_validation_report(
    results: list[RelationContractValidationResult],
    output_dir: Path | None = None,
) -> Path:

    output_dir = output_dir or hermes_data_audit_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "silver_relationship_validation_report.csv"

    validation_report_df = pd.DataFrame(
        [
            {
                "table_relationship_name": result.table_relationship_name,
                "child_table": result.child_table,
                "child_column": result.child_column,
                "parent_table": result.parent_table,
                "parent_column": result.parent_column,
                "passed": result.passed,
                "failed_count": result.failed_count,
                "child_count": result.child_count,
                "details": result.details,
            }
            for result in results
        ]
    )

    validation_report_df.to_csv(output_path, index=False)
    logger.info(f"Wrote silver table relationship validation report to {output_path}")

    return output_path


def validate_silver_table_relationships(
    validation_spark_session: SparkSession | None = None, silver_base_dir: Path | None = None, table_relationship_contract_path: Path | None = None, validation_report_dir: Path | None = None
) -> list[RelationContractValidationResult]:

    config_logging()

    spark = validation_spark_session or create_local_spark_session(name="HERMES silver table relationship validation")
    silver_base_dir = silver_base_dir or hermes_silver_dir()
    table_relationship_contract_path = table_relationship_contract_path or Path("contracts/silver/table_relationships/table_relationships.yml")

    silver_table_relationships = load_table_relationship_yaml_contract(table_relationship_contract_path)

    silver_table_names = sorted({relationship.child_table for relationship in silver_table_relationships} | {relationship.parent_table for relationship in silver_table_relationships})

    silver_tables = {}

    for silver_table_name in silver_table_names:
        table_path = silver_base_dir / silver_table_name

        if not table_path.exists():
            raise FileNotFoundError(f"Silver table does not exist in path: {table_path}")

        silver_tables[silver_table_name] = spark.read.format("delta").load(str(table_path))

    silver_validation_results = [validate_table_relationships(relationship, silver_tables) for relationship in silver_table_relationships]

    write_relationship_validation_report(silver_validation_results, validation_report_dir)

    failed_validation_results = [result for result in silver_validation_results if not result.passed]

    if failed_validation_results:
        logger.warning(f"Silver relationship validation has been completed with {len(failed_validation_results)} failed checks")

    return silver_validation_results


def failed_records_for_column_rules(df: DataFrame, column_name: str, rule_name: str, rule_config) -> DataFrame:

    if column_name not in df.columns:
        return df

    if rule_name == "not_null":
        return df.filter(functions.col(column_name).isNull())

    if rule_name == "unique":
        duplicate_keys = df.groupBy(column_name).count().filter((functions.col("count") > 1) & functions.col(column_name).isNotNull()).select(column_name)
        return df.join(duplicate_keys, on=column_name, how="inner")

    if rule_name == "min_value":
        return df.filter(functions.col(column_name) < functions.lit(rule_config))

    if rule_name == "accepted_values":
        return df.filter(functions.col(column_name).isNotNull() & ~functions.col(column_name).isin(rule_config))

    if rule_name == "regex":
        return df.filter(functions.col(column_name).isNotNull() & ~functions(column_name).rlike(rule_config))

    raise ValueError(f"Unsupported validation rule used for quarantine: {rule_name}")


def quarantine_table_validation_failures(
    df: DataFrame, data_contract: HermesDataContract, validation_results: list[ContractValidationResult], quarantine_base_dir: Path | None = None
) -> list[HermesQuarantineResult]:

    quarantine_results = []

    failed_validation_results = [result for result in validation_results if not result.passed]

    if not failed_validation_results:
        return quarantine_results

    for result in failed_validation_results:
        column_config = data_contract.columns.get(result.column_name, {})

        if not isinstance(column_config, dict):
            logger.warning(f"No valid column config found for {data_contract.table}.{result.column_name} Column config expected to be dict type")
            continue

        matching_rule = None
        normalized_rule_config = None

        for rule in column_config.get("rules", []):
            rule_name, rule_config = _normalise_contract_rule(rule=rule)
            if rule_name == result.rule_name:
                matching_rule = rule_name
                normalized_rule_config = rule_config
                break

        if matching_rule is None:
            logger.warning(f"No matching rule config found for {data_contract.table}.{result.column_name}.{result.rule_name}")
            continue

        failed_records = failed_records_for_column_rules(df=df, column_name=result.column_name, rule_name=matching_rule, rule_config=normalized_rule_config)

        quarantine_result = write_failed_record_to_quarantine(
            failed_records=failed_records,
            table_name=data_contract.table,
            column_name=result.column_name,
            rule_name=result.rule_name,
            failure_reason=result.details,
            base_quarantine_dir=quarantine_base_dir,
        )

        quarantine_results.append(quarantine_result)

    return quarantine_results


def main() -> None:
    column_validation_results = validate_silver_tables()
    silver_relationship_results = validate_silver_table_relationships()

    failed_column_val_results = [result for result in column_validation_results if not result.passed]
    failed_relationship_val_results = [result for result in silver_relationship_results if not result.passed]

    if failed_column_val_results or failed_relationship_val_results:
        raise SystemExit(
            "=====| SILVER VALIDATION FAILED |===== "
            f"\nNumber of column validation checks failed: {len(failed_column_val_results)} "
            f"\nNumber of silver table relationship checks failed: {len(failed_relationship_val_results)}"
        )


if __name__ == "__main__":
    main()
