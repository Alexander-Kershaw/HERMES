from pathlib import Path

from hermes.quality.data_quarantine import (
    HermesQuarantineResult,
    make_quarantine_path,
)


def test_make_quarantine_path_local(tmp_path: Path) -> None:
    path: str = make_quarantine_path(
        table_name="customers",
        column_name="email",
        rule_name="not_null",
        base_quarantine_path=tmp_path / "quarantine",
    )

    assert path == str(tmp_path / "quarantine" / "silver" / "customers" / "not_null_email")


def test_make_quarantine_path_sanitises_rule_and_column() -> None:
    path: str = make_quarantine_path(
        table_name="customers",
        column_name="customer.email",
        rule_name="regex.email",
        base_quarantine_path="data/quarantine",
    )

    assert path == "data/quarantine/silver/customers/regex_email_customer_email"


def test_make_quarantine_path_azure() -> None:
    path: str = make_quarantine_path(
        table_name="customers",
        column_name="email",
        rule_name="not_null",
        base_quarantine_path=("abfss://quarantine@sthermesdevexample.dfs.core.windows.net/hermes/quarantine"),
    )

    assert path == ("abfss://quarantine@sthermesdevexample.dfs.core.windows.net/hermes/quarantine/silver/customers/not_null_email")


def test_make_quarantine_path_uses_azure_runtime(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_RUNTIME_ENV", "azure")
    monkeypatch.setenv("HERMES_STORAGE_ACCOUNT", "sthermesdevexample")

    path: str = make_quarantine_path(
        table_name="orders",
        column_name="channel",
        rule_name="accepted_values",
    )

    assert path == ("abfss://quarantine@sthermesdevexample.dfs.core.windows.net/hermes/quarantine/silver/orders/accepted_values_channel")


def test_quarantine_result_uses_string_path() -> None:
    result = HermesQuarantineResult(
        table_name="customers",
        column_name="email",
        rule_name="not_null",
        quarantine_path="data/quarantine/silver/customers/not_null_email",
        row_count=1,
    )

    assert isinstance(result.quarantine_path, str)
