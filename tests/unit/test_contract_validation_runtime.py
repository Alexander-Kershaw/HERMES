from pathlib import Path

from hermes.quality.contract_validators import (
    _resolve_audit_base_path,
    _resolve_silver_base_path,
    resolve_silver_table_path,
)
from hermes.utils.paths import hermes_data_audit_dir, hermes_silver_dir


def test_resolve_silver_base_path_uses_local_default(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_RUNTIME_ENV", "local")
    monkeypatch.delenv("HERMES_STORAGE_ACCOUNT", raising=False)

    assert _resolve_silver_base_path() == str(hermes_silver_dir())


def test_resolve_audit_base_path_uses_local_default(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_RUNTIME_ENV", "local")
    monkeypatch.delenv("HERMES_STORAGE_ACCOUNT", raising=False)

    assert _resolve_audit_base_path() == str(hermes_data_audit_dir())


def test_resolve_silver_base_path_uses_azure_runtime(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_RUNTIME_ENV", "azure")
    monkeypatch.setenv("HERMES_STORAGE_ACCOUNT", "sthermesdevexample")

    assert _resolve_silver_base_path() == ("abfss://silver@sthermesdevexample.dfs.core.windows.net/hermes/silver")


def test_resolve_audit_base_path_uses_azure_runtime(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_RUNTIME_ENV", "azure")
    monkeypatch.setenv("HERMES_STORAGE_ACCOUNT", "sthermesdevexample")

    assert _resolve_audit_base_path() == ("abfss://audit@sthermesdevexample.dfs.core.windows.net/hermes/audit")


def test_resolve_silver_table_path_local() -> None:
    assert resolve_silver_table_path(silver_base_path="data/lakehouse/silver", table_name="customers") == "data/lakehouse/silver/customers"


def test_resolve_silver_table_path_azure() -> None:
    assert (
        resolve_silver_table_path(
            silver_base_path="abfss://silver@sthermesdevexample.dfs.core.windows.net/hermes/silver",
            table_name="customers",
        )
        == "abfss://silver@sthermesdevexample.dfs.core.windows.net/hermes/silver/customers"
    )


def test_resolve_silver_base_path_accepts_override(tmp_path: Path) -> None:
    assert _resolve_silver_base_path(silver_base_path=tmp_path / "silver") == str(tmp_path / "silver")


def test_resolve_audit_base_path_accepts_override(tmp_path: Path) -> None:
    assert _resolve_audit_base_path(audit_base_path=tmp_path / "audit") == str(tmp_path / "audit")
