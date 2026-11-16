"""Prediction files, loaded exactly as landed, with lineage from the file path."""

import dlt
from pyspark.sql import functions as F

LANDING = "/Volumes/predictions/raw/landing"

# The contract: the columns we expect, all as text in bronze. Silver does the typing,
# because the raw values need cleaning first (e.g. prediction arrives as "[3.52]").
PREDICTION_SCHEMA = "model STRING, metric STRING, prediction STRING, player STRING, game_date STRING"

@dlt.table(
    name="predictions.bronze.player_predictions",
    comment="Prediction rows exactly as landed, plus game date and arrival time from the file path.",
)
def player_predictions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.partitionColumns", "game_date")      # from the game_date=... folder
        .option("rescuedDataColumn", "_rescued_data")             # anything that doesn't fit the contract
        .schema(PREDICTION_SCHEMA)
        .load(f"{LANDING}/predictions/")
        .withColumn("_file_path", F.col("_metadata.file_path"))
        .withColumn("_file_name", F.col("_metadata.file_name"))
        .withColumn(
            "_arrived_at",                                         # S3 LastModified, from the file name
            F.to_timestamp(
                F.regexp_extract("_metadata.file_name", r"__(\d{8}T\d{6}Z)\.csv$", 1),
                "yyyyMMdd'T'HHmmss'Z'",
            ),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )