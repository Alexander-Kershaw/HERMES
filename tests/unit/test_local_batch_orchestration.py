from hermes.orchestration.run_local_batch_orchestration import BatchOrchestrationStageResult


def test_batch_stage_result_shape() -> None:
    stage_result = BatchOrchestrationStageResult(
        pipeline_stage_name="test_stage",
        completion_status="success",
        started_at="2026-01-01T00:00:00+00:00",
        finished_at="2026-01-01T00:00:01+00:00",
        duration_seconds=1.0,
        details="ok",
    )

    assert stage_result.pipeline_stage_name == "test_stage"
    assert stage_result.completion_status == "success"
    assert stage_result.duration_seconds == 1.0
