"""NBA player prop lines (points, rebounds, assists), snapshotted several times a day.

Each run lands one CSV per game, one row per book, stat, player and side, in
the same flattened format as the standalone script:
    odds/game_date=YYYY-MM-DD/{event_id}__{snapshot_at}.csv
All games in a run share one snapshot_at, so a snapshot can be compared as a whole.

Every snapshot is kept, so silver can track how each line moves from the
morning open to just before tip-off.

API keys: up to three, in Airflow Variables whose names contain "secret" so
Airflow masks them in the UI and logs. Each run checks every key's remaining
credits (free) and uses them in order, moving to the next key when one drops
below MIN_CREDITS or is rejected. Only key *positions* (1, 2, 3) ever go into
XCom or the logs, never the keys themselves.
"""

import csv
import io
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pendulum
import requests
from airflow.sdk import Variable, dag, task
from databricks.sdk import WorkspaceClient

SPORT = "basketball_nba"
MARKETS = "player_points,player_rebounds,player_assists"
STAT_MAP = {
    "player_points": "points",
    "player_rebounds": "total_rebounds",
    "player_assists": "assists",
}
BOOKMAKERS = "draftkings,fanduel"  # up to 10 bookmakers cost the same as one region
CREDITS_PER_GAME = 3               # 3 markets x 1 bookmaker group
MIN_CREDITS = 15                   # switch to the next key below this
LOOKAHEAD_HOURS = 18
KEY_VARIABLES = ["odds_api_key_secret", "odds_api_key_2_secret", "odds_api_key_3_secret"]
EASTERN = ZoneInfo("America/New_York")
LANDING = os.environ.get("LANDING_VOLUME", "/Volumes/predictions/raw/landing")

# game_date isn't a column: Auto Loader reads it from the game_date= folder.
FIELDS = [
    "event_id", "commence_time", "away_team", "home_team",
    "bookmaker", "market", "stat", "player_name", "side", "line", "price",
    "last_update", "snapshot_at",
]


class EventNotFound(Exception):
    """The game finished or was cancelled before its odds were fetched."""


def load_keys() -> list[str | None]:
    """Keys by position; unset ones stay None so positions never shift."""
    return [Variable.get(name, default=None) or None for name in KEY_VARIABLES]


def send(path: str, params: dict, api_key: str) -> requests.Response:
    base_url = Variable.get("odds_api_secret_url")
    try:
        return requests.get(
            f"{base_url}{path}", params={**params, "apiKey": api_key}, timeout=60
        )
    except requests.RequestException as exc:
        # Never print the original exception: its message can contain the URL, key included.
        raise RuntimeError(f"Odds API request to {path} failed: {type(exc).__name__}") from None


def remaining_credits(response: requests.Response) -> int | None:
    value = response.headers.get("x-requests-remaining")
    return int(float(value)) if value is not None else None


class KeyPool:
    """Uses keys in the given order, moving on when one runs low or is rejected."""

    def __init__(self, key_order: list[int]):
        self.keys = load_keys()
        self.order = list(key_order)

    def get(self, path: str, params: dict):
        while self.order:
            position = self.order[0]
            response = send(path, params, self.keys[position])
            remaining = remaining_credits(response)
            print(
                f"key #{position + 1}: HTTP {response.status_code}, "
                f"cost {response.headers.get('x-requests-last')}, remaining {remaining}"
            )

            if response.status_code in (401, 403):  # out of credits, or a bad key
                print(f"key #{position + 1} rejected, switching to the next key")
                self.order.pop(0)
                continue
            if response.status_code == 404:
                raise EventNotFound(path)
            if not response.ok:  # 429 and 5xx: let Airflow retry with backoff
                raise RuntimeError(f"Odds API request to {path} returned HTTP {response.status_code}")

            if remaining is not None and remaining < MIN_CREDITS and len(self.order) > 1:
                print(f"key #{position + 1} is low ({remaining}), later calls use the next key")
                self.order.pop(0)
            return response.json()

        raise RuntimeError("Every odds API key is exhausted or rejected")


def eastern_game_date(commence_time: str) -> str:
    """A 7:40 PM ET tip-off is 00:40 the next day in UTC, so convert first."""
    tip_off = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
    return tip_off.astimezone(EASTERN).date().isoformat()


@dag(
    schedule="23 11,15,18 * * *",  # Eastern: morning open, afternoon, close to tip-off
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    tags=["odds", "ingestion"],
)
def daily_prop_lines():
    @task
    def check_keys() -> list[int]:
        """Read every key's remaining credits from the free /sports endpoint."""
        usable = []
        for position, key in enumerate(load_keys()):
            if not key:
                continue
            response = send("/sports", {}, key)
            remaining = remaining_credits(response)
            print(f"key #{position + 1}: HTTP {response.status_code}, remaining {remaining}")
            if response.ok and (remaining is None or remaining >= MIN_CREDITS):
                usable.append(position)
        if not usable:
            raise RuntimeError("No odds API key has enough credits left")
        return usable  # positions only; the keys never enter XCom

    @task
    def list_upcoming_games(key_order: list[int]) -> list[dict]:
        """The events endpoint doesn't count against the usage quota."""
        now = datetime.now(timezone.utc).replace(microsecond=0)
        events = KeyPool(key_order).get(
            f"/sports/{SPORT}/events",
            {
                "dateFormat": "iso",
                "commenceTimeFrom": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "commenceTimeTo": (now + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        snapshot_at = now.strftime("%Y%m%dT%H%M%SZ")
        print(f"{len(events)} upcoming games, about {len(events) * CREDITS_PER_GAME} credits")
        return [
            {"event_id": e["id"], "matchup": f"{e['away_team']} @ {e['home_team']}", "snapshot_at": snapshot_at}
            for e in events
        ]

    @task(max_active_tis_per_dag=4)
    def land_game_lines(game: dict, key_order: list[int]) -> None:
        try:
            odds = KeyPool(key_order).get(
                f"/sports/{SPORT}/events/{game['event_id']}/odds",
                {"bookmakers": BOOKMAKERS, "markets": MARKETS, "oddsFormat": "american", "dateFormat": "iso"},
            )
        except EventNotFound:
            print(f"{game['matchup']}: no longer available, skipping")
            return

        rows = []
        for book in odds.get("bookmakers", []):
            for market in book.get("markets", []):
                for outcome in market.get("outcomes", []):
                    rows.append({
                        "game_date": eastern_game_date(odds["commence_time"]),
                        "event_id": odds.get("id"),
                        "commence_time": odds.get("commence_time"),
                        "away_team": odds.get("away_team"),
                        "home_team": odds.get("home_team"),
                        "bookmaker": book.get("key"),
                        "market": market.get("key"),
                        "stat": STAT_MAP.get(market.get("key"), market.get("key")),
                        "player_name": outcome.get("description"),
                        "side": outcome.get("name"),
                        "line": outcome.get("point"),
                        "price": outcome.get("price"),
                        "last_update": market.get("last_update"),
                        "snapshot_at": game["snapshot_at"],
                    })

        if not rows:
            print(f"{game['matchup']}: no prop lines posted yet, skipping")
            return

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        path = f"{LANDING}/odds/game_date={rows[0]['game_date']}/{game['event_id']}__{game['snapshot_at']}.csv"
        WorkspaceClient().files.upload(path, io.BytesIO(buffer.getvalue().encode()), overwrite=True)
        print(f"{game['matchup']}: landed {len(rows)} lines to {path}")

    key_order = check_keys()
    land_game_lines.partial(key_order=key_order).expand(game=list_upcoming_games(key_order))


daily_prop_lines()