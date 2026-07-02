from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

"""

This is a single controlled local spark entry point, I have this
to save rewriting spark session builders everywhere

Note: this will end up being replaced by Databricks spark session 

"""


def create_local_spark_session(name: str = "HERMES", data_warehouse_dir: Path | str = "data/spark-warehouse") -> SparkSession:

    spark_session_builder = (
        SparkSession.builder.appName(name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(data_warehouse_dir))
        .config("spark.sql.extentions", "io.delta.sql.DeltaSparkSessionExtention")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "True")
    )

    return configure_spark_with_delta_pip(spark_session_builder).getOrCreate()
