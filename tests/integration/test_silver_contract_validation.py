from pathlib import Path

from pyspark.sql import SparkSession

from hermes.quality.contract_validators import validate_table, write_validation_report
from hermes.quality.contracts import load_yaml_contract
from hermes.utils.spark import create_local_spark_session


def test_validate_table_passes_for_valid_data(tmp_path: Path) -> None:
    spark = create_local_spark_session("test_validate_table_passes_for_valid_data")

    contract_path = tmp_path / "customers.yml"
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

    contract = load_yaml_contract(contract_path)

    df = spark.createDataFrame(
        [
            {"customer_id": "CUSTOMER-000001", "loyalty_tier": "gold"},
            {"customer_id": "CUSTOMER-000002", "loyalty_tier": "silver"},
        ]
    )

    results = validate_table(df, contract)

    assert all(result.passed for result in results)

    spark.stop()


def test_validate_table_fails_for_invalid_data(tmp_path: Path) -> None:
    spark: SparkSession = create_local_spark_session("test_validate_table_fails_for_invalid_data")

    contract_path = tmp_path / "customers.yml"
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

    contract = load_yaml_contract(contract_path)

    df = spark.createDataFrame(
        [
            {"customer_id": "CUSTOMER-000001", "loyalty_tier": "gold"},
            {"customer_id": "BAD_DATA-ID", "loyalty_tier": "diamond"},
            {"customer_id": "CUSTOMER-000001", "loyalty_tier": "silver"},
        ]
    )

    results = validate_table(df, contract)

    failed_results = [result for result in results if not result.passed]

    assert failed_results
    assert any(result.rule_name == "unique" for result in failed_results)
    assert any(result.rule_name == "regex" for result in failed_results)
    assert any(result.rule_name == "accepted_values" for result in failed_results)

    spark.stop()


def test_write_validation_report(tmp_path: Path) -> None:
    spark = create_local_spark_session("test_write_validation_report")

    contract_path = tmp_path / "customers.yml"
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

    contract = load_yaml_contract(contract_path)

    df = spark.createDataFrame([{"customer_id": "CUSTOMER-000001"}])
    results = validate_table(df, contract)

    report_path = write_validation_report(results, output_dir=tmp_path)

    assert report_path.exists()

    spark.stop()


def test_validate_table_fails_invalid_accepted_values(tmp_path: Path) -> None:
    spark = create_local_spark_session("test_validate_table_fails_invalid_accepted_values")

    contract_path = tmp_path / "orders.yml"
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

    contract = load_yaml_contract(contract_path)

    df = spark.createDataFrame(
        [
            {"channel": "online"},
            {"channel": "big_fat_carrier_pigeon"},
        ]
    )

    results = validate_table(df, contract)

    assert any(not result.passed for result in results)
    assert any(result.rule_name == "accepted_values" for result in results)

    spark.stop()
