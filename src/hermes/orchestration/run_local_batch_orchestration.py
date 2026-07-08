import subprocess
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from hermes.data_generation.generate_batch_sources import DataGenerationConfig, generate_all_synth_retail_data
from hermes.ingestion.bronze_ingestion import full_source_ingestion_to_bronze
from hermes.quality.contract_validators import validate_silver_table_relationships, validate_silver_tables
from hermes.transforms.bronze_silver_transform import transform_all_bronze_to_silver
from hermes.utils.logging import config_logging
from hermes.utils.paths import hermes_data_audit_dir, hermes_quarantine_dir


@dataclass(frozen=True)
class BatchOrchestrationStageResult:
    pipeline_stage_name: str
    completion_status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    details: str


def _run_batch_pipeline_stage(pipeline_stage_name: str, stage_function) -> BatchOrchestrationStageResult:
    started_at: datetime = datetime.now(tz=UTC)

    try:
        logger.info(f"Starting orchestrated batch pipeline stage: {pipeline_stage_name}")
        stage_result: Any = stage_function()
        completion_status = "success"
        details = str(object=stage_result)
        logger.info(f"Completed batch pipeline stage: {pipeline_stage_name}")

    except Exception as exc:
        completion_status = "failed"
        details: str = repr(exc)
        logger.exception(f"Batch pipeline stage failed: {pipeline_stage_name}")

    finally:
        finished_at: datetime = datetime.now(tz=UTC)

    return BatchOrchestrationStageResult(
        pipeline_stage_name=pipeline_stage_name,
        completion_status=completion_status,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=(finished_at - started_at).total_seconds(),
        details=details,
    )


def _run_dbt_hermes_parse() -> str:
    """

    Runs dbt parse as an additional structural validation step.

    The full dbt execution requires the intended Databricks connectivity, this is local
    orchetration so this is why it uses dbt parse rather than dbt run.

    """

    dbt_project_dir: Path = Path("dbt_hermes")

    dbt_parse_completed: subprocess.CompletedProcess[str] = subprocess.run(args=["dbt", "parse"], cwd=dbt_project_dir, check=True, capture_output=True, text=True)

    return dbt_parse_completed.stdout


def _summarize_quarantine() -> str:
    quarantine_base_dir: Path = hermes_quarantine_dir()

    if not quarantine_base_dir.exists():
        return "Quarantine directory not found."

    quarantine_incidents_paths: list[Path] = sorted(quarantine_base_dir.glob(pattern="silver/*/*.parquet"))

    if not quarantine_incidents_paths:
        return "No quarantine incidents found."

    return f"Found {len(quarantine_incidents_paths)} quarantine output path(s)."


def write_orchestrated_batch_run_audit(pipeline_stage_results: list[BatchOrchestrationStageResult]) -> Path:
    output_dir: Path = hermes_data_audit_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    pipeline_run_timestamp: str = datetime.now(tz=UTC).strftime(format="%Y%m%dT%H%M%SZ")
    audit_output_path: Path = output_dir / f"batch_run_audit_{pipeline_run_timestamp}.csv"

    pd.DataFrame(data=[asdict(obj=stage_result) for stage_result in pipeline_stage_results]).to_csv(path_or_buf=audit_output_path, index=False)

    logger.info(f"Wrote batch pipeline run audit to: {audit_output_path}")

    return audit_output_path


def run_batch_pipeline_locally(include_dbt_parse: bool = True) -> list[BatchOrchestrationStageResult]:
    config_logging()

    batch_pipeline_run_results: list = []

    batch_pipeline_run_results.append(_run_batch_pipeline_stage(pipeline_stage_name="generate_batch_source_data", stage_function=lambda: generate_all_synth_retail_data(config=DataGenerationConfig())))

    batch_pipeline_run_results.append(_run_batch_pipeline_stage(pipeline_stage_name="Bronze ingestion", stage_function=full_source_ingestion_to_bronze))

    batch_pipeline_run_results.append(_run_batch_pipeline_stage(pipeline_stage_name="Silver transformation", stage_function=transform_all_bronze_to_silver))

    batch_pipeline_run_results.append(_run_batch_pipeline_stage(pipeline_stage_name="Silver validation: Column rules", stage_function=validate_silver_tables))

    batch_pipeline_run_results.append(_run_batch_pipeline_stage(pipeline_stage_name="Silver validation: Table relationships", stage_function=validate_silver_table_relationships))

    batch_pipeline_run_results.append(_run_batch_pipeline_stage(pipeline_stage_name="Quarantine record summay", stage_function=_summarize_quarantine))

    if include_dbt_parse:
        batch_pipeline_run_results.append(_run_batch_pipeline_stage(pipeline_stage_name="dbt_parse", stage_function=_run_dbt_hermes_parse))

    write_orchestrated_batch_run_audit(pipeline_stage_results=batch_pipeline_run_results)

    logger.info("=====| HERMES local batch pipeline completed successfully |=====")

    return batch_pipeline_run_results


def main() -> None:
    run_batch_pipeline_locally()


if __name__ == "__main__":
    main()
