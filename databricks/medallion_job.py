"""Databricks/PySpark version of the local contract-driven pipeline.

Expected widget parameters: input_path, output_root.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("sensor_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("fill_percent", DoubleType(), False),
        StructField("temperature_c", DoubleType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
    ]
)

input_path = dbutils.widgets.get("input_path")  # noqa: F821
output_root = dbutils.widgets.get("output_root")  # noqa: F821

bronze = spark.read.option("header", True).schema(schema).csv(input_path)  # noqa: F821
bronze.write.format("delta").mode("append").save(f"{output_root}/bronze/events")

is_valid = (
    F.col("event_id").isNotNull()
    & F.col("sensor_id").isNotNull()
    & F.to_timestamp("event_time").isNotNull()
    & F.col("fill_percent").between(0, 100)
    & F.col("temperature_c").between(-30, 100)
    & F.col("latitude").between(-90, 90)
    & F.col("longitude").between(-180, 180)
)

silver = (
    bronze.filter(is_valid)
    .dropDuplicates(["event_id"])
    .withColumn("latitude", F.round("latitude", 2))
    .withColumn("longitude", F.round("longitude", 2))
)
quarantine = bronze.filter(~is_valid).withColumn("rejection_reason", F.lit("contract violation"))
silver.write.format("delta").mode("append").save(f"{output_root}/silver/events")
quarantine.write.format("delta").mode("append").save(f"{output_root}/quarantine/events")

gold = (
    silver.withColumn("date", F.to_date("event_time"))
    .groupBy("date", "sensor_id")
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("fill_percent"), 2).alias("average_fill_percent"),
        F.max("fill_percent").alias("max_fill_percent"),
        F.round(F.avg("temperature_c"), 2).alias("average_temperature_c"),
        F.sum(F.when(F.col("fill_percent") >= 85, 1).otherwise(0)).alias("collection_alerts"),
    )
)
gold.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(
    f"{output_root}/gold/daily_metrics"
)

