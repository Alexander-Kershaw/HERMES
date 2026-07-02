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


def load_yaml_contract(contract_path: Path) -> HermesDataContract:
    """

    Loads a single yaml data contract and returns it as a data contract
    object (HermesDataContract)

    """

    if not contract_path.exists():
        raise FileNotFoundError(f"YAML data contract not found at: {contract_path}")

    with contract_path.open("r", encoding="utf-8") as yaml_contract:
        loaded_yaml_contract = yaml.safe_load(yaml_contract)

    data_contract = HermesDataContract(
        table=loaded_yaml_contract["table"], primary_key=loaded_yaml_contract.get("primary_key", []), columns=loaded_yaml_contract.get("columns", {}), contract_path=contract_path
    )

    return data_contract


def load_all_yaml_contracts(contracts_dir: Path) -> dict[str, HermesDataContract]:
    """

    Loads all the YAML contracts in a directory

    """

    all_data_contracts = {}

    for single_yaml_contract_path in sorted(contracts_dir.glob("*yml")):
        contract = load_yaml_contract(single_yaml_contract_path)
        all_data_contracts[contract.table] = contract

    return all_data_contracts
