from pathlib import Path

from hermes.config.hermes_setting import hermes_settings


def ensure_directory_exists(path: Path) -> Path:
    # Just ensures a directory exists and creates if needed
    path.mkdir(parents=True, exist_ok=True)
    return path

def sample_data_dir() -> Path:
    # Return sample data directory and creates if needed
    return ensure_directory_exists(hermes_settings.hermes_sample_data_dir)

def raw_sample_data_dir() -> Path:
    # Returns raw sample source data directory and creates if needed
    return ensure_directory_exists(hermes_settings.hermes_sample_data_dir / "raw")

def processed_sample_data_dir() -> Path:
    # Returns processed sample source data directory and creates if needed
    return ensure_directory_exists(hermes_settings.hermes_sample_data_dir / "processed")

def hermes_lakehouse_dir() -> Path:
    return ensure_directory_exists(hermes_settings.hermes_lakehouse_dir)

def hermes_bronze_dir() -> Path:
    return ensure_directory_exists(hermes_settings.hermes_lakehouse_dir / "bronze")

def hermes_silver_dir() -> Path:
    return ensure_directory_exists(hermes_settings.hermes_lakehouse_dir / "silver")

def hermes_gold_dir() -> Path:
    return ensure_directory_exists(hermes_settings.hermes_lakehouse_dir / "gold")

def hermes_data_audit_dir() -> Path:
    return ensure_directory_exists(hermes_settings.hermes_data_audit_dir)

def hermes_quarantine_dir() -> Path:
    return ensure_directory_exists(hermes_settings.hermes_quarantine_dir)





