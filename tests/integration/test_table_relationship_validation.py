from pathlib import Path

from hermes.quality.contract_validators import validate_table_relationships
from hermes.quality.contracts import load_table_relationship_yaml_contract
from hermes.utils.spark import create_local_spark_session


def test_load_relationship_contracts(tmp_path: Path) -> None:
    relationship_path = tmp_path / "relationships.yml"

    relationship_path.write_text(
        """
relationships:
  - table_relationship_name: orders_customer_fk
    child_table: orders
    child_column: customer_id
    parent_table: customers
    parent_column: customer_id
""",
        encoding="utf-8",
    )

    relationships = load_table_relationship_yaml_contract(relationship_path)

    assert len(relationships) == 1
    assert relationships[0].table_relationship_name == "orders_customer_fk"
    assert relationships[0].child_table == "orders"
    assert relationships[0].parent_table == "customers"


def test_validate_relationship_passes_for_valid_keys() -> None:
    spark = create_local_spark_session("test_validate_relationship_passes_for_valid_keys")

    orders = spark.createDataFrame(
        [
            {"order_id": "ORDER-00000001", "customer_id": "CUSTOMER-000001"},
            {"order_id": "ORDER-00000002", "customer_id": "CUSTOMER-000002"},
        ]
    )

    customers = spark.createDataFrame(
        [
            {"customer_id": "CUSTOMER-000001"},
            {"customer_id": "CUSTOMER-000002"},
        ]
    )

    relationship = load_relationship_contracts_from_text(
        """
relationships:
  - table_relationship_name: orders_customer_fk
    child_table: orders
    child_column: customer_id
    parent_table: customers
    parent_column: customer_id
"""
    )[0]

    result = validate_table_relationships(
        table_relationship=relationship,
        tables={
            "orders": orders,
            "customers": customers,
        },
    )

    assert result.passed
    assert result.failed_count == 0

    spark.stop()


def test_validate_relationship_fails_for_orphan_keys() -> None:
    spark = create_local_spark_session("test_validate_relationship_fails_for_orphan_keys")

    orders = spark.createDataFrame(
        [
            {"order_id": "ORDER-00000001", "customer_id": "CUSTOMER-000001"},
            {"order_id": "ORDER-00000002", "customer_id": "CUSTOMER-999999"},
        ]
    )

    customers = spark.createDataFrame(
        [
            {"customer_id": "CUSTOMER-000001"},
        ]
    )

    relationship = load_relationship_contracts_from_text(
        """
relationships:
  - table_relationship_name: orders_customer_fk
    child_table: orders
    child_column: customer_id
    parent_table: customers
    parent_column: customer_id
"""
    )[0]

    result = validate_table_relationships(
        table_relationship=relationship,
        tables={
            "orders": orders,
            "customers": customers,
        },
    )

    assert not result.passed
    assert result.failed_count == 1

    spark.stop()


def load_relationship_contracts_from_text(text: str):
    tmp_path = Path("data/tmp_test_relationships.yml")
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path.write_text(text, encoding="utf-8")
    try:
        return load_table_relationship_yaml_contract(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)
