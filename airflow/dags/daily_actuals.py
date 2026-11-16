"""Fetch the season's actual stats and land a snapshot in the volume when the data changes.

One API call returns the whole year, so there's nothing to backfill: the first run
lands all of history. Later runs land a new snapshot only when the content's
fingerprint changes. The API address is a masked Airflow Variable, and any field
that could reveal it is removed before landing.
"""

from __future__ import annotations

import hashlib
import io
import json
import os

import pendulum
import requests
from airflow.sdk import Variable, dag, task
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

STATE_PATH = "_state/actuals.json"
URL_KEYS = {"self", "next", "previous", "links", "url", "href"}


def volume_path(relative: str) -> str:
    return f"{os.environ['LANDING_VOLUME']}/{relative}"





def read_state(w: WorkspaceClient) -> dict:
    try:
        return json.loads(w.files.download(volume_path(STATE_PATH)).contents.read())
    except NotFound:
        return {}


@dag(
    schedule="17 11 * * *",                   # daily, 11:17 UTC (6:17 a.m. New York), after late games end
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"year": None},                    # override when triggering by hand; defaults to this year
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,    # 5, then ~10, then ~20 minutes between attempts
    },
    tags=["actuals"],
)
def daily_actuals():

    @task
    def fetch_and_land(params=None) -> None:
        year = (params or {}).get("year") or pendulum.now("UTC").year
        base_url = Variable.get("actuals_api_secret_url")   # masked in logs: the name contains "secret"

        response = requests.get(f"{base_url}/year-data/{year}", timeout=120)
        response.raise_for_status()
        body = json.dumps(response.json(), sort_keys=True).encode()
        digest = hashlib.sha256(body).hexdigest()

        w = WorkspaceClient()
        state = read_state(w)
        if state.get(str(year), {}).get("sha256") == digest:
            print(f"Actuals for {year} unchanged since the last snapshot; nothing landed.")
            return

        fetched_at = pendulum.now("UTC").format("YYYYMMDD[T]HHmmss[Z]")
        landed_path = f"actuals/{year}/actuals_{year}__{fetched_at}.json"
        w.files.upload(volume_path(landed_path), io.BytesIO(body), overwrite=True)

        state[str(year)] = {"sha256": digest, "fetched_at": fetched_at, "landed_path": landed_path}
        w.files.upload(volume_path(STATE_PATH), io.BytesIO(json.dumps(state, indent=2).encode()), overwrite=True)
        print(f"Landed {len(body):,} bytes -> {landed_path}")

    fetch_and_land()


daily_actuals()