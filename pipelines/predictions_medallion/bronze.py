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

# The actuals contract: every field the API sends, typed. Mismatches go to _rescued_data.
BOX_SCORE_SCHEMA = """
    starters STRING, minutes_played INT,
    field_goals INT, fga INT, field_goal_percentage DOUBLE,
    three_point_field_goals INT, three_point_field_goal_attempts INT, three_point_field_goal_percentage DOUBLE,
    free_throws INT, free_throw_attempts INT, free_throw_percentage DOUBLE,
    offensive_rebounds INT, defensive_rebounds INT, total_rebounds INT,
    assists INT, steals INT, blocks INT, turnovers INT, personal_fouls INT, points INT,
    month INT, day INT, year INT,
    opposing_team STRING, team_eog_score INT, op_eog_score INT, away STRING, player_team STRING
"""


@dlt.table(
    name="predictions.bronze.player_box_scores",
    comment="Box score rows from each season snapshot, exactly as received, plus snapshot time.",
)
def player_box_scores():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")                       # each file is one JSON array, not one object per line
        .option("rescuedDataColumn", "_rescued_data")
        .schema(BOX_SCORE_SCHEMA)
        .load(f"{LANDING}/actuals/")
        .withColumn("_file_name", F.col("_metadata.file_name"))
        .withColumn(
            "_snapshot_at",                                 # when the DAG fetched this snapshot
            F.to_timestamp(
                F.regexp_extract("_metadata.file_name", r"__(\d{8}T\d{6}Z)\.json$", 1),
                "yyyyMMdd'T'HHmmss'Z'",
            ),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )