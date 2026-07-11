from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class HermesDataContract:
    """

    Dataclass object for a singular data contract for one table

    """

    table: str
    primary_key: list[str]
    columns: dict[str, Any]
    contract_path: Path


@dataclass(frozen=True)
class HermesRelationshipsContract:
    """

    Relationship data contract between two tables in the silver layer

    """

    table_relationship_name: str
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str


def load_yaml_contract(contract_path: Path) -> HermesDataContract:
    """

    Loads a single yaml data contract and returns it as a data contract
    object (HermesDataContract)

    """

    if not contract_path.exists():
        raise FileNotFoundError(f"YAML data contract not found at: {contract_path}")

    with contract_path.open("r", encoding="utf-8") as yaml_contract:
        loaded_yaml_contract = yaml.safe_load(stream=yaml_contract)

    required_keys: set[str] = {"table", "columns"}
    missing_keys: set[str] = required_keys - set(loaded_yaml_contract)

    if missing_keys:
        raise ValueError(f"Invalid data contract at {contract_path}. Missing keys: {sorted(missing_keys)}")

    data_contract = HermesDataContract(
        table=loaded_yaml_contract["table"], primary_key=loaded_yaml_contract.get("primary_key", []), columns=loaded_yaml_contract.get("columns", {}), contract_path=contract_path
    )

    return data_contract


def load_all_yaml_contracts(contracts_dir: Path) -> dict[str, HermesDataContract]:
    """

    Loads all the YAML contracts in a directory

    """

    all_data_contracts: dict = {}

    for single_yaml_contract_path in sorted(contracts_dir.glob("*yml")):
        loaded_yaml_contract = yaml.safe_load(stream=single_yaml_contract_path.read_text(encoding="utf-8"))

        # If table not in yaml contract then continue to next table
        if "table" not in loaded_yaml_contract:
            continue

        contract: HermesDataContract = load_yaml_contract(contract_path=single_yaml_contract_path)
        all_data_contracts[contract.table] = contract

    return all_data_contracts


def load_table_relationship_yaml_contract(relation_contract_path: Path) -> list[HermesRelationshipsContract]:
    if not relation_contract_path.exists():
        raise FileNotFoundError(f"Table relationship contract yaml file not found: {relation_contract_path}")

    with relation_contract_path.open("r", encoding="utf-8") as relation_contract_file:
        relation_contract = yaml.safe_load(relation_contract_file)

    table_relationships = relation_contract.get("relationships", [])

    table_relationship_obj: list[HermesRelationshipsContract] = [
        HermesRelationshipsContract(
            table_relationship_name=table_relationship["name"],
            child_table=table_relationship["child_table"],
            child_column=table_relationship["child_column"],
            parent_table=table_relationship["parent_table"],
            parent_column=table_relationship["parent_column"],
        )
        for table_relationship in table_relationships
    ]

    return table_relationship_obj
