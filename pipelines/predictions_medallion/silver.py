import re
import unicodedata
from pyspark.sql import functions as F

SILVER_START_DATE = spark.conf.get("silver_start_date", "2026-08-21")

TARGETS = ["points", "assists", "total_rebounds"]
TARGET_ALIASES = {"rebounds": "total_rebounds"}       # older files say "rebounds" (to confirm)
KNOWN_TARGET_NAMES = TARGETS + list(TARGET_ALIASES)
BASELINE_PLAYER = "Reserves"                           # each model's average, not a real player
MOJIBAKE = "[ÃÄÅÂ]"                                    # signs of UTF-8 text read as Latin-1
MODEL_NAME = "^[a-z]+[0-9]*$"                          # emerald3, amethyst1, sapphire, ...
PREDICTION_KEY = ["game_date", "player_key", "model_name", "target"]


@F.udf("string")
def player_key(name):
    """A comparable key for a name: no accents, punctuation, or suffixes; lowercase."""
    if name is None:
        return None
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    cleaned = re.sub(r"[^a-z0-9 ]", "", ascii_name.lower())
    cleaned = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


# ---------------------------------------------------------------------------
# Staging: stream bronze in scope, clean and label every row
# ---------------------------------------------------------------------------

@dlt.view(name="prediction_rows_staged")
def prediction_rows_staged():
    rows = (
        spark.readStream.table("predictions.bronze.player_predictions")
        .where(F.col("game_date") >= SILVER_START_DATE)
        .withColumn("_digits", F.regexp_replace("prediction", r"[\[\]\s]", ""))   # "[3.52]" -> "3.52"
    )

    # The column holding a known target IS the target column, whatever the header says.
    model_is_target = F.col("model").isin(KNOWN_TARGET_NAMES)
    raw_target = F.when(model_is_target, F.col("model")).otherwise(F.col("metric"))
    target = raw_target
    for old, new in TARGET_ALIASES.items():
        target = F.when(raw_target == old, F.lit(new)).otherwise(target)

    # Repair names stored as UTF-8 but read as Latin-1 (DonÄiÄ -> Dončić). Only touch damaged ones.
    player_name = F.when(
        F.col("player").rlike(MOJIBAKE),
        F.decode(F.encode(F.col("player"), "ISO-8859-1"), "UTF-8"),
    ).otherwise(F.col("player"))

    staged = rows.select(
        F.to_date("game_date").alias("game_date"),
        F.trim(player_name).alias("player_name"),
        F.when(model_is_target, F.col("metric")).otherwise(F.col("model")).alias("model_name"),
        target.alias("target"),
        F.expr("try_cast(_digits AS DOUBLE)").alias("predicted_value"),
        model_is_target.alias("was_swapped"),
        "_arrived_at",
        "_file_name",
        "model", "metric", "prediction", "player",      # raw values, for inspecting quarantined rows
    )

    reason = (
        F.when(F.col("player_name").isNull(), "missing player")
        .when(F.col("model_name").isNull() & F.col("target").isNull(), "empty row")
        .when(F.col("model_name").isNull(), "missing model")
        .when(F.col("target").isNull() | ~F.col("target").isin(TARGETS), "unknown target")
        .when(~F.col("model_name").rlike(MODEL_NAME), "unrecognized model name")
        .when(F.col("predicted_value").isNull(), "unparseable prediction")
    )

    return (
        staged.withColumn("player_key", player_key("player_name"))
        .withColumn("quarantine_reason", reason)
        # The same player can appear in several blocks of one file. Keep one copy per file.
        .dropDuplicates(PREDICTION_KEY + ["_file_name"])
    )


# ---------------------------------------------------------------------------
# Quarantine: append-only, with the reason
# ---------------------------------------------------------------------------

@dlt.table(
    name="predictions.silver.prediction_quarantine",
    comment="Prediction rows that failed silver rules, with the reason.",
)
def prediction_quarantine():
    return dlt.read_stream("prediction_rows_staged").where(F.col("quarantine_reason").isNotNull())


# ---------------------------------------------------------------------------
# Baselines: each model's "Reserves" value per day and target (SCD Type 1)
# ---------------------------------------------------------------------------

@dlt.view(name="baseline_rows")
def baseline_rows():
    return (
        dlt.read_stream("prediction_rows_staged")
        .where(F.col("quarantine_reason").isNull() & (F.col("player_name") == BASELINE_PLAYER))
        .select("game_date", "model_name", "target",
                F.col("predicted_value").alias("baseline_value"), "_arrived_at")
    )


dlt.create_streaming_table(
    name="predictions.silver.model_baselines",
    comment="Each model's default prediction (the 'Reserves' row) per game date and target.",
)

dlt.apply_changes(
    target="predictions.silver.model_baselines",
    source="baseline_rows",
    keys=["game_date", "model_name", "target"],
    sequence_by=F.col("_arrived_at"),
    stored_as_scd_type=1,
)


# ---------------------------------------------------------------------------
# Predictions: one row per key; history only when the value changes (SCD Type 2)
# ---------------------------------------------------------------------------

@dlt.view(name="prediction_rows")
def prediction_rows():
    return (
        dlt.read_stream("prediction_rows_staged")
        .where(F.col("quarantine_reason").isNull() & (F.col("player_name") != BASELINE_PLAYER))
        .withColumn("model_family", F.regexp_extract("model_name", r"^([a-z]+)", 1))
        .withColumn("model_variant", F.nullif(F.regexp_extract("model_name", r"([0-9]+)$", 1), F.lit("")))
        .select(*PREDICTION_KEY, "player_name", "model_family", "model_variant",
                "predicted_value", "was_swapped", "_arrived_at", "_file_name")
    )


dlt.create_streaming_table(
    name="predictions.silver.player_predictions",
    comment=(
        "One row per game date, player, model, and target. __START_AT is when the prediction was "
        "first seen; a second version of a key means a later copy disagreed with the first."
    ),
)

dlt.apply_changes(
    target="predictions.silver.player_predictions",
    source="prediction_rows",
    keys=PREDICTION_KEY,
    sequence_by=F.col("_arrived_at"),
    stored_as_scd_type=2,
    track_history_column_list=["predicted_value"],   # new version only if the prediction itself changes
)