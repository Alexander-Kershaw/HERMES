from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from hermes.quality.contract_validators import ContractValidationResult, failed_records_for_column_rules, validate_table, write_validation_report
from hermes.quality.contracts import HermesDataContract, load_yaml_contract
from hermes.utils.spark import create_local_spark_session


def test_validate_table_passes_for_valid_data(tmp_path: Path) -> None:
    spark = create_local_spark_session(name="test_validate_table_passes_for_valid_data")

    contract_path: Path = tmp_path / "customers.yml"
    contract_path.write_text(
        """
table: customers
primary_key:
  - customer_id
columns:
  customer_id:
    type: string
    rules:
      - not_null
      - unique
      - regex: "^CUSTOMER-[0-9]{6}$"
  loyalty_tier:
    type: string
    rules:
      - accepted_values:
          - bronze
          - silver
          - gold
""",
        encoding="utf-8",
    )

    contract: HermesDataContract = load_yaml_contract(contract_path=contract_path)

    df = spark.createDataFrame(
        [
            {"customer_id": "CUSTOMER-000001", "loyalty_tier": "gold"},
            {"customer_id": "CUSTOMER-000002", "loyalty_tier": "silver"},
        ]
    )

    results: list[ContractValidationResult] = validate_table(df=df, contract=contract)

    assert all(result.passed for result in results)

    spark.stop()


def test_validate_table_fails_for_invalid_data(tmp_path: Path) -> None:
    spark: SparkSession = create_local_spark_session(name="test_validate_table_fails_for_invalid_data")

    contract_path: Path = tmp_path / "customers.yml"
    contract_path.write_text(
        """
table: customers
primary_key:
  - customer_id
columns:
  customer_id:
    type: string
    rules:
      - not_null
      - unique
      - regex: "^CUSTOMER-[0-9]{6}$"
  loyalty_tier:
    type: string
    rules:
      - accepted_values:
          - bronze
          - silver
          - gold
""",
        encoding="utf-8",
    )

    contract: HermesDataContract = load_yaml_contract(contract_path=contract_path)

    df = spark.createDataFrame(
        [
            {"customer_id": "CUSTOMER-000001", "loyalty_tier": "gold"},
            {"customer_id": "BAD_DATA-ID", "loyalty_tier": "diamond"},
            {"customer_id": "CUSTOMER-000001", "loyalty_tier": "silver"},
        ]
    )

    results: list[ContractValidationResult] = validate_table(df=df, contract=contract)

    failed_results: list[ContractValidationResult] = [result for result in results if not result.passed]

    assert failed_results
    assert any(result.rule_name == "unique" for result in failed_results)
    assert any(result.rule_name == "regex" for result in failed_results)
    assert any(result.rule_name == "accepted_values" for result in failed_results)

    spark.stop()


def test_write_validation_report(tmp_path: Path) -> None:
    spark = create_local_spark_session(name="test_write_validation_report")

    contract_path: Path = tmp_path / "customers.yml"
    contract_path.write_text(
        """
table: customers
primary_key:
  - customer_id
columns:
  customer_id:
    type: string
    rules:
      - not_null
""",
        encoding="utf-8",
    )

    contract: HermesDataContract = load_yaml_contract(contract_path=contract_path)

    df = spark.createDataFrame([{"customer_id": "CUSTOMER-000001"}])
    results: list[ContractValidationResult] = validate_table(df=df, contract=contract)

    report_path: str = write_validation_report(results=results, output_dir=str(tmp_path))

    assert Path(report_path).exists()

    spark.stop()


def test_validate_table_fails_invalid_accepted_values(tmp_path: Path) -> None:
    spark = create_local_spark_session(name="test_validate_table_fails_invalid_accepted_values")

    contract_path: Path = tmp_path / "orders.yml"
    contract_path.write_text(
        """
table: orders
primary_key:
  - order_id
columns:
  channel:
    type: string
    rules:
      - accepted_values:
          - online
          - store
          - mobile_app
""",
        encoding="utf-8",
    )

    contract: HermesDataContract = load_yaml_contract(contract_path=contract_path)

    df = spark.createDataFrame(
        [
            {"channel": "online"},
            {"channel": "big_fat_carrier_pigeon"},
        ]
    )

    results: list[ContractValidationResult] = validate_table(df=df, contract=contract)

    assert any(not result.passed for result in results)
    assert any(result.rule_name == "accepted_values" for result in results)

    spark.stop()


def test_failied_records_for_accepted_values() -> None:
    spark = create_local_spark_session(name="testing_failed_records_for_accepted_values")

    df: DataFrame = spark.createDataFrame([{"channel": "online"}, {"channel": "store"}, {"channel": "came to me in a dream"}])

    failed_validation_records = failed_records_for_column_rules(df=df, column_name="channel", rule_name="accepted_values", rule_config=["online", "store", "mobile_app"])

    assert failed_validation_records.count() == 1
    assert failed_validation_records.collect()[0]["channel"] == "came to me in a dream"

    spark.stop()


def test_get_failed_records_for_unique() -> None:
    spark = create_local_spark_session(name="test_get_failed_records_for_unique")

    df: DataFrame = spark.createDataFrame(
        [
            {"order_id": "ORDER-00000001"},
            {"order_id": "ORDER-00000001"},
            {"order_id": "ORDER-00000002"},
        ]
    )

    failed_records = failed_records_for_column_rules(
        df=df,
        column_name="order_id",
        rule_name="unique",
        rule_config=None,
    )

    assert failed_records.count() == 2

    spark.stop()
