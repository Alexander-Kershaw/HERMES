from pathlib import Path

from hermes.quality.contracts import (
    HermesDataContract,
    HermesRelationshipsContract,
    default_relationship_contract_path,
    default_silver_contracts_dir,
    load_all_yaml_contracts,
    load_table_relationship_yaml_contract,
)


def test_default_silver_contracts_dir_exists() -> None:
    contracts_dir: Path = default_silver_contracts_dir()

    assert contracts_dir.exists()
    assert contracts_dir.is_dir()


def test_packaged_silver_contracts_load() -> None:
    contracts: dict[str, HermesDataContract] = load_all_yaml_contracts(contracts_dir=default_silver_contracts_dir())

    assert "customers" in contracts
    assert "orders" in contracts
    assert "order_items" in contracts


def test_default_relationship_contract_path_exists() -> None:
    relationship_path: Path = default_relationship_contract_path()

    assert relationship_path.exists()
    assert relationship_path.name == "table_relationships.yml"


def test_packaged_relationship_contract_loads() -> None:
    relationships: list[HermesRelationshipsContract] = load_table_relationship_yaml_contract(relation_contract_path=default_relationship_contract_path())

    assert relationships
