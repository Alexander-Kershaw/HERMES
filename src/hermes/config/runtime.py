import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class HermesRuntimeEnvironment(StrEnum):
    LOCAL_ENV = "local"
    AZURE_ENV = "azure"


@dataclass(frozen=True)
class HermesRuntimeSettings:
    environment: HermesRuntimeEnvironment
    raw_source_data_path: str
    bronze_path: str
    silver_path: str
    gold_path: str
    quarantine_path: str
    audit_path: str
    storage_account: str | None = None


def _local_runtime_settings() -> HermesRuntimeSettings:
    return HermesRuntimeSettings(
        environment=HermesRuntimeEnvironment.LOCAL_ENV,
        raw_source_data_path=str(Path("data/sample/raw")),
        bronze_path=str(Path("data/lakehouse/bronze")),
        silver_path=str(Path("data/lakehouse/silver")),
        gold_path=str(Path("data/lakehouse/gold")),
        quarantine_path=str(Path("data/quarantine")),
        audit_path=str(Path("data/audit")),
    )


def _azure_runtime_settings() -> HermesRuntimeSettings:

    azure_storage_account: str = os.getenv(key="HERMES_STORAGE_ACCOUNT")

    if not azure_storage_account:
        raise ValueError("HERMES_STORAGE_ACCOUNT must be set when HERMES_RUNTIME_ENV=azure")

    account_fqdn: str = f"{azure_storage_account}.dfs.core.windows.net"

    return HermesRuntimeSettings(
        environment=HermesRuntimeEnvironment.AZURE_ENV,
        storage_account=azure_storage_account,
        raw_source_data_path=f"abfss://landing@{account_fqdn}/hermes/raw",
        bronze_path=f"abfss://bronze@{account_fqdn}/hermes/bronze",
        silver_path=f"abfss://silver@{account_fqdn}/hermes/silver",
        gold_path=f"abfss://gold@{account_fqdn}/hermes/gold",
        quarantine_path=f"abfss://quarantine@{account_fqdn}/hermes/quarantine",
        audit_path=f"abfss://audit@{account_fqdn}/hermes/audit",
    )


def retrieve_hermes_runtime_settings() -> HermesRuntimeSettings:

    ENV = os.getenv("HERMES_RUNTIME_ENV", HermesRuntimeEnvironment.LOCAL_ENV.value).lower()

    try:
        environment = HermesRuntimeEnvironment(ENV)
    except ValueError as exc:
        valid_env_values = ", ".join(env.value for env in HermesRuntimeEnvironment)
        raise ValueError(f"Invald HERMES_RUNTIME_ENV={ENV!r}. Expected one of: {valid_env_values}") from exc

    if environment == HermesRuntimeEnvironment.LOCAL_ENV:
        return _local_runtime_settings()

    if environment == HermesRuntimeEnvironment.AZURE_ENV:
        return _azure_runtime_settings()

    raise ValueError(f"Unsupported runtime environment: {environment}")
