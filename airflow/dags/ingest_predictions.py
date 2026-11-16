"""Land new or changed prediction files from S3 into the Databricks landing volume.

Each run lists daily_* in S3, compares every file's ETag against stored state,
and copies only changed files, unmodified, into the volume. State is saved last,
so a failed run re-lands files instead of skipping them. The first run, with no
state yet, is the initial load of all history.
"""

from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable, dag, task
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

STATE_PATH = "_state/prediction_files.json"
KEY_PATTERN = re.compile(r"^daily_(\d{4})/players_(\d{2})_(\d{2})\.csv$")


def volume_path(relative: str) -> str:
    return f"{os.environ['LANDING_VOLUME']}/{relative}"


def game_date_for(key: str) -> str | None:
    """daily_2026/players_11_08.csv -> 2026-11-08. None if the name doesn't match."""
    match = KEY_PATTERN.match(key)
    if not match:
        return None
    year, month, day = match.groups()
    return f"{year}-{month}-{day}"


def read_state(w: WorkspaceClient) -> dict:
    try:
        return json.loads(w.files.download(volume_path(STATE_PATH)).contents.read())
    except NotFound:
        return {}  # first run: nothing landed yet


@dag(
    schedule="*/30 * * * *",                 # every 30 minutes
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,                       # runs never overlap, so state is never written twice at once
    tags=["predictions"],
)
def ingest_predictions():

    @task
    def find_changed_files() -> list[dict]:
        bucket = Variable.get("predictions_bucket")
        s3 = S3Hook(aws_conn_id="aws_predictions").get_conn()
        state = read_state(WorkspaceClient())

        changed = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="daily_"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if game_date_for(key) is None:
                    print(f"Skipping file with an unexpected name: {key}")
                    continue
                if state.get(key, {}).get("etag") == obj["ETag"]:
                    continue  # unchanged since it was last landed
                changed.append(
                    {
                        "key": key,
                        "etag": obj["ETag"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                    }
                )

        print(f"{len(changed)} new or changed file(s)")
        return changed

    @task(max_active_tis_per_dag=4)          # at most 4 files downloading at once
    def land_file(obj: dict) -> dict:
        bucket = Variable.get("predictions_bucket")
        s3 = S3Hook(aws_conn_id="aws_predictions").get_conn()

        # IfMatch: download exactly the version we listed. If the file was rewritten
        # in between, S3 refuses, the task fails, and the next run picks up the new version.
        body = s3.get_object(Bucket=bucket, Key=obj["key"], IfMatch=obj["etag"])["Body"].read()

        game_date = game_date_for(obj["key"])
        arrived = datetime.fromisoformat(obj["last_modified"]).strftime("%Y%m%dT%H%M%SZ")
        file_name = obj["key"].split("/")[-1].removesuffix(".csv")
        landed_path = f"predictions/game_date={game_date}/{file_name}__{arrived}.csv"

        WorkspaceClient().files.upload(volume_path(landed_path), io.BytesIO(body), overwrite=True)
        print(f"Landed {obj['key']} ({len(body):,} bytes) -> {landed_path}")
        return {**obj, "landed_path": landed_path}

    @task
    def save_state(landed: list[dict]) -> None:
        w = WorkspaceClient()
        state = read_state(w)
        for entry in landed:
            state[entry["key"]] = entry
        body = json.dumps(state, indent=2, sort_keys=True).encode()
        w.files.upload(volume_path(STATE_PATH), io.BytesIO(body), overwrite=True)
        print(f"Saved state for {len(landed)} file(s)")

    landed = land_file.expand(obj=find_changed_files())
    save_state(landed)


ingest_predictions()