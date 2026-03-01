from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_latest_record(df):
    # Convert timestamp fields once so ordering is consistent
    df = (
        df.withColumn("source_updated_at_ts", F.to_timestamp("source_updated_at"))
          .withColumn("ingested_at_ts", F.to_timestamp("ingested_at"))
          .withColumn("is_deleted", F.coalesce(F.col("is_deleted").cast("boolean"), F.lit(False)))
    )

    # Window ensures we deterministically pick the latest record per party_key
    window_spec = Window.partitionBy("party_key").orderBy(
        F.col("source_updated_at_ts").desc(),
        F.col("ingested_at_ts").desc()
    )

    # Assign row number within each party group
    ranked_df = df.withColumn("rn", F.row_number().over(window_spec))

    # Keep only the top-ranked record per party_key
    latest_df = ranked_df.filter(F.col("rn") == 1).drop("rn")

    return latest_df


def handle_soft_deletes(df, keep_deleted=True):
    # Business choice: either keep tombstones or drop logically deleted rows
    if keep_deleted:
        return df
    return df.filter(~F.col("is_deleted"))


def write_curated(df, output_path):
    # Overwrite ensures idempotency for re-runs of same dataset
    df.write.mode("overwrite").parquet(output_path)
