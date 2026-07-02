from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

"""

This file serves to provide a centralized location for all the paths
and configuration settings used in the project.

"""

HERMES_ROOT_DIR = Path(__file__).resolve().parents[3]


class HermesSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent.parent / ".env", env_file_encoding="utf-8", extra="ignore")

    hermes_env: str = "local"

    hermes_data_dir: Path = HERMES_ROOT_DIR / "data"
    hermes_sample_data_dir: Path = HERMES_ROOT_DIR / "data" / "sample"
    hermes_lakehouse_dir: Path = HERMES_ROOT_DIR / "data" / "lakehouse"
    hermes_quarantine_dir: Path = HERMES_ROOT_DIR / "data" / "quarantine"
    hermes_data_audit_dir: Path = HERMES_ROOT_DIR / "data" / "audit"

    random_seed: int = 77


hermes_settings = HermesSettings()

print(hermes_settings.model_dump())
