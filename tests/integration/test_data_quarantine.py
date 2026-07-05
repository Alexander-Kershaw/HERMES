from pathlib import Path

from hermes.quality.data_quarantine import write_failed_record_to_quarantine
from hermes.utils.spark import create_local_spark_session


def test_write_failed_records_to_quarantine(tmp_path: Path) -> None:
    spark = create_local_spark_session("test_write_failed_records_to_quarantine")

    failed_records = spark.createDataFrame(
        [
            {
                "order_id": "ORDER-00000001",
                "channel": "my mate Kyle",
            }
        ]
    )

    result = write_failed_record_to_quarantine(
        failed_records=failed_records,
        table_name="orders",
        column_name="channel",
        rule_name="accepted_values",
        failure_reason="Invalid channel",
        base_quarantine_dir=tmp_path,
    )

    assert result.row_count == 1
    assert result.quarantine_path.exists()

    quarantined_df = spark.read.parquet(str(result.quarantine_path))

    assert quarantined_df.count() == 1
    assert "_quarantine_table_name" in quarantined_df.columns
    assert "_quarantine_column_name" in quarantined_df.columns
    assert "_quarantine_rule_name" in quarantined_df.columns
    assert "_quarantine_failure_reason" in quarantined_df.columns
    assert "_quarantine_time" in quarantined_df.columns
    assert "_quarantine_date" in quarantined_df.columns

    row = quarantined_df.collect()[0]

    assert row["_quarantine_table_name"] == "orders"
    assert row["_quarantine_column_name"] == "channel"
    assert row["_quarantine_rule_name"] == "accepted_values"

    spark.stop()
