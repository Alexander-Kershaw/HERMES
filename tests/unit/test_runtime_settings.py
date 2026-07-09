import pytest

from hermes.config.runtime import HermesRuntimeEnvironment, retrieve_hermes_runtime_settings


def test_local_runtime_settings_are_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(name="HERMES_RUNTIME_ENV", raising=False)

    settings: HermesRuntimeEnvironment = retrieve_hermes_runtime_settings()

    assert settings.environment == HermesRuntimeEnvironment.LOCAL_ENV
    assert settings.raw_source_data_path == "data/sample/raw"
    assert settings.bronze_path == "data/lakehouse/bronze"
    assert settings.silver_path == "data/lakehouse/silver"


def test_azure_runtime_settings_require_storage_account(monkeypatch: pytest.MonkeyPatch) -> None:

    monkeypatch.setenv(name="HERMES_RUNTIME_ENV", value="azure")
    monkeypatch.delenv(name="HERMES_STORAGE_ACCOUNT", raising=False)

    with pytest.raises(expected_exception=ValueError, match="HERMES_STORAGE_ACCOUNT"):
        retrieve_hermes_runtime_settings()


def test_azure_runtime_settings_build_abfss_paths(monkeypatch: pytest.MonkeyPatch) -> None:

    monkeypatch.setenv(name="HERMES_RUNTIME_ENV", value="azure")
    monkeypatch.setenv(name="HERMES_STORAGE_ACCOUNT", value="sthermesdevexample")

    settings: HermesRuntimeEnvironment = retrieve_hermes_runtime_settings()

    assert settings.environment == HermesRuntimeEnvironment.AZURE_ENV
    assert settings.storage_account == "sthermesdevexample"
    assert settings.raw_source_data_path == "abfss://landing@sthermesdevexample.dfs.core.windows.net/hermes/raw"
    assert settings.silver_path == "abfss://silver@sthermesdevexample.dfs.core.windows.net/hermes/silver"
