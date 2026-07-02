from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from loguru import logger

from hermes.utils.logging import config_logging
from hermes.utils.paths import (
    hermes_data_audit_dir,
    hermes_bronze_dir,
    raw_sample_data_dir
)

@dataclass(frozen=True)
class BronzeIngestionMeta:

    source_name: str
    source_path: Path
    bronze_path: Path
    row_count: int
    column_count: int
    ingestion_datetime: datetime

@dataclass(frozen=True)
class BronzeIngestionConfig:

    source_dir: Path | None = None
    output_dir: Path | None = None
    file_format: str = "parquet" 


DATA_SOURCE_FILES = {
    "customers": "customers.csv",
    "stores": "stores.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "inventory_snapshots": "inventory_snapshots.csv",
    "promotions": "promotions.csv"
}


def _resolve_source_dir(config: BronzeIngestionConfig) -> Path:
    return config.source_dir or raw_sample_data_dir()

def _resolve_output_dir(config: BronzeIngestionConfig) -> Path:
    return config.output_dir or hermes_bronze_dir()

def _read_source_csv_file(source_data_path: Path) -> pd.DataFrame:
    if not source_data_path.exists():
        raise FileNotFoundError(
            f"<red> SOURCE DATA FILES DOES NOT EXIST: {source_data_path} </red>"
            )
    
    return pd.read_csv(source_data_path)


def _attach_bronze_metadata(
        source_df: pd.DataFrame,
        source_name: str,
        source_path: Path,
        ingestion_datetime: datetime
) -> pd.DataFrame:
    
    bronze_df = source_df.copy()

    bronze_df["_bronze_source_name"] = source_name
    bronze_df["_bronze_source_file"] = source_path.name
    bronze_df["_bronze_source_path"] = str(source_path)
    bronze_df["_bronze_ingested_at"] = ingestion_datetime.isoformat()
    bronze_df["_bronze_ingestion_date"] = ingestion_datetime.date().isoformat()

    return bronze_df


def _write_bronze_table(
        df: pd.DataFrame,
        output_dir: Path,
        source_name: str,
        file_format: str,
) -> Path:
    
    bronze_table_dir = output_dir / source_name
    bronze_table_dir.mkdir(parents=True, exist_ok=True)

    if file_format == "parquet":
        output_path = bronze_table_dir / f"{source_name}.parquet"
        df.to_parquet(output_path, index=False)
        return output_path
    
    if file_format == "csv":
        output_path = bronze_table_dir / f"{source_name}.parquet"
        df.to_csv(output_path, index=False)
        return output_path
    
    raise ValueError(f"<red> UNSUPPORTED BRONZE FILE FORMAT: {file_format} </red>")


def ingest_source_to_bronze_layer(
        source_name: str,
        source_path: Path,
        output_dir: Path,
        file_format: str = "parquet",
) -> BronzeIngestionMeta:
    
    ingestion_datetime = datetime.now(UTC)

    source_data_df = _read_source_csv_file(source_path)
    bronze_data_df = _attach_bronze_metadata(
        source_data_df, source_name, source_path, ingestion_datetime
    )

    bronze_data_path = _write_bronze_table(
        bronze_data_df, output_dir, source_name, file_format
    )

    bronze_ingestion_result = BronzeIngestionMeta(
        source_name=source_name,
        source_path=source_path,
        bronze_path=bronze_data_path,
        row_count=len(bronze_data_df),
        column_count=len(bronze_data_df.columns),
        ingestion_datetime=ingestion_datetime
    )

    logger.info(
        f"INGESTED {source_name}:"
        f"{bronze_ingestion_result.row_count:,} rows, {bronze_ingestion_result.column_count:,}" 
        "columns ->>> {bronze_data_path}"
    )

    return bronze_ingestion_result


def write_ingestion_audit(results: list[BronzeIngestionMeta]) -> Path:

    output_dir = hermes_data_audit_dir()
    output_path = output_dir / "bronze_ingestion_audit.csv"

    bronze_audit_df = pd.DataFrame(
        [
            {
                "source_name": result.source_name,
                "source_path": str(result.source_path),
                "bronze_path": str(result.bronze_path),
                "row_count": result.row_count,
                "column_count": result.column_count,
                "ingestion_datetime": result.ingestion_datetime.isoformat()
            }
            for result in results
        ]
    )

    bronze_audit_df.to_csv(output_path, index=False)
    logger.info(f"WROTE BRONZE INGESTION AUDIT TO {output_path}")


def full_source_ingestion_to_bronze(
        config: BronzeIngestionConfig | None = None
) -> list[BronzeIngestionMeta]:
    
    bronze_config = config or BronzeIngestionConfig()

    source_data_dir = _resolve_source_dir(config=bronze_config)
    bronze_output_dir = _resolve_output_dir(config=bronze_config)

    logger.info(f"=====| STARTING LOCAL BRONZE INGESTION |=====")
    logger.info(f"Source data directory: {source_data_dir}")
    logger.info(f"Bronze data directory: {bronze_output_dir}")

    bronze_results = []

    for source_name, file_name in DATA_SOURCE_FILES.items():
        source_path = source_data_dir / file_name
        result = ingest_source_to_bronze_layer(
            source_name=source_name,
            source_path=source_path,
            output_dir=bronze_output_dir,
            file_format=bronze_config.file_format
        )

        bronze_results.append(result)

    write_ingestion_audit(bronze_results)

    return bronze_results


def main() -> None:
    config_logging()
    full_source_ingestion_to_bronze()

if __name__ == "__main__":
    main()





