from hermes.utils.paths import hermes_silver_dir
from hermes.utils.spark import create_local_spark_session

spark = create_local_spark_session(name="Inspect Silver")

inspection_df = spark.read.format("delta").load(str(hermes_silver_dir() / "orders"))

inspection_df.printSchema()

inspection_df.show(50, truncate=False)

print("rows:", inspection_df.count())

spark.stop()
