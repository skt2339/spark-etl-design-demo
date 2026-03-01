from pyspark.sql.types import *
from pyspark.sql import functions as F


def get_schema():
    # Explicit schema avoids costly inference on 60GB CSV
    return StructType([
        StructField("party_key", StringType(), True),
        StructField("source_updated_at", StringType(), True),
        StructField("name", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("country", StringType(), True),
        StructField("is_deleted", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ])


def ingest_csv(spark, input_path, output_path, run_date):
    df = (
        spark.read
             .option("header", "true")
             .schema(get_schema())  # schema defined upfront
             .csv(input_path)
    )

    # Normalize datatypes after load
    df = (
        df.withColumn("source_updated_at", F.to_timestamp("source_updated_at"))
          .withColumn("ingested_at", F.to_timestamp("ingested_at"))
          .withColumn("dob", F.to_date("dob"))
          .withColumn("is_deleted", F.col("is_deleted").cast("boolean"))
          .withColumn("run_date", F.lit(run_date))
    )

    # Repartition to control output file size (avoid small files)
    df = df.repartition(200, "run_date")

    # Partition by run_date for incremental processing
    df.write.mode("overwrite").partitionBy("run_date").parquet(output_path)
