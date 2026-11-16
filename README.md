# Prediction Lakehouse
 
A data platform that measures how well a set of NBA player-prediction models actually perform. Predictions arrive as CSV files in **S3** throughout the day; actual box scores come from a stats API. **Airflow** lands both in **Databricks**, a **Lakeflow Declarative Pipeline** cleans them into bronze and silver, and **dbt** builds the gold layer: every prediction scored against what happened, a model leaderboard, drift tracking, and pipeline health checks.
 
The source data is real and messy, and the producer is treated as a vendor that can't be changed, so every quality issue is handled, measured, and made visible in the pipeline rather than fixed upstream.
 
## The result
 
`predictions.gold.model_leaderboard`, over predictions for players who played:
 
| Stat | Best model | MAE | Skill vs. baseline |
|---|---|---:|---:|
| points | [model] | [x.xx] | [x.xx] |
| assists | [model] | [x.xx] | [x.xx] |
| total_rebounds | [model] | [x.xx] | [x.xx] |
 
**MAE** is the average miss, in points, assists, or rebounds. **Skill vs. baseline** compares each model's error with simply predicting that model's own average: 0.20 means 20% less error than the baseline, and 0 or below means the model adds nothing.
 
## Architecture
 
```
S3 (prediction CSVs, rewritten as they grow)          Stats API (one call = a full calendar year)
        │                                                        │
Airflow: ingest_predictions (every 30 min)            Airflow: daily_actuals (daily)
  list daily_*, compare ETags to stored state            fetch the year, fingerprint it (SHA-256),
  land only new or changed files, unmodified             land a snapshot only when it changed
        │                                                        │
        └──────────────► raw: /Volumes/predictions/raw/landing ◄─┘
                                     │  Auto Loader, explicit schemas
                                     ▼
          bronze   player_predictions, player_box_scores        rows as received + lineage
                                     │  Lakeflow Declarative Pipeline (streaming tables, Auto CDC)
                                     ▼
          silver   player_predictions (SCD2), model_baselines,  cleaned, typed, deduplicated,
                   player_box_scores (SCD1), prediction_quarantine   long format
                                     │  dbt
                                     ▼
    intermediate   int_current_predictions, int_actuals_long
                                     │  dbt
                                     ▼
            gold   fct_prediction_outcomes, model_leaderboard,  the only layer analysts can read
                   model_daily_performance, pipeline_match_rate
```
 
## Repository layout
 
| Path | What it does |
|---|---|
| `terraform/main.tf` | AWS: a read-only IAM role for one bucket prefix, and an Airflow user that can only assume it |
| `terraform/databricks.tf` | Databricks: schemas per layer, the landing volume, gold-only grants |
| `airflow/dags/ingest_predictions.py` | Incremental landing of prediction files from S3 |
| `airflow/dags/daily_actuals.py` | Daily snapshot of the season's box scores from the API |
| `airflow/dags/list_prediction_files.py`, `check_databricks.py` | Setup and connectivity checks |
| `pipelines/predictions_medallion/bronze.py` | Bronze tables with explicit schema contracts |
| `pipelines/predictions_medallion/silver.py` | Cleaning rules, quarantine, baselines, CDC tables |
| `dbt/` | Intermediate and gold models, and tests |
| `analysis/profile_bronze.sql` | Profiling queries used to size up the raw data before writing silver |
| `DEVLOG.md` | Problems hit and how they were solved |
 
## The data, and what's wrong with it
 
**Predictions** are CSVs, one per game date (`players_MM_DD.csv`), rewritten throughout the day as rows are added. Each row is one player, one model, one stat. Models grow over time (16 names so far, in families like `emerald1`, `emerald3`, `emerald5`); the three stats are fixed.
 
Profiling bronze before writing any cleaning rules found:
 
| Issue | Example | How silver handles it |
|---|---|---|
| `model` and `metric` swapped, from two causes: whole files with a different header, and blocks inside normal files | `total_rebounds,emerald3,...` | Whichever column holds a known stat **is** the stat. Counted per row as `was_swapped` |
| Predictions stored as text lists | `[3.527728796005249]` | Brackets stripped, `try_cast` to a number |
| Empty rows | `,,,AK Okereke` | Quarantined, with a reason |
| A placeholder "player" | `Reserves` | Stored as each model's baseline, not as a player |
| Default predictions | Many players get exactly the `Reserves` value | Flagged `is_default` and evaluated separately |
| UTF-8 names read as Latin-1, in **both** sources | `Luka DonÄiÄ`, `VÃ­t KrejÄÃ­` | Repaired only when the damage pattern appears |
| No player IDs in either source | | A normalized `player_key`: no accents, punctuation, or suffixes |
| The same player in several blocks of a file | | Deduplicated per file; values always agree |
 
**Actuals** are a JSON array of box score rows. The player's name is in a field called `starters`, the date is split into `year`, `month`, and `day`, away opponents carry a trailing comma (`"MEM,"`), and players who sat out appear with all zeros.
 
## Design decisions
 
### Ingestion (Airflow)
 
- **Long format from the start.** One row per player, game, model, and stat. A new model is just more rows; no table or query changes.
- **ETags decide what changed; timestamps record when.** S3's ETag changes exactly when contents change. `LastModified` can tie within a second and changes on identical re-uploads, so it's kept for ordering and naming instead.
- **Files are landed unmodified,** one new file per version, with the game date in the folder and the arrival time in the name: `predictions/game_date=2026-11-08/players_11_08__20261108T234107Z.csv`.
- **The first run is the initial load.** With no stored state, every file is new, so history loads through the same code as incremental runs.
- **State is saved last,** so a failed run re-lands files rather than skipping them. Duplicates are harmless because silver dedupes.
- **One run at a time** (`max_active_runs=1`), so the state file is never written twice at once.
- **`IfMatch` on download** guarantees the version that was listed is the version landed.
- **Actuals are fingerprinted with SHA-256,** since the API offers no timestamps. Unchanged days land nothing.
- **Downtime only delays data.** Change detection is based on stored state, so if the scheduler is off, the next run catches up.
### Bronze
 
- **Explicit schemas, not inference.** An empty folder produces an empty table instead of failing the pipeline, and anything that doesn't fit lands in `_rescued_data`.
- **Everything is kept as received.** Values are cleaned in silver.
- **Lineage from the file path:** game date from the folder, arrival time from the file name.
### Silver
 
- **Scope:** games from 2026-08-21 onward (`SILVER_START_DATE`). Bronze keeps full history.
- **Streaming tables with Auto CDC.** Each run processes only new bronze rows.
- **Predictions use SCD Type 2, tracking only `predicted_value`.** Predictions are static for a game date, so each key normally has one version, and `__START_AT` records when it was first seen. A second version would mean a later copy disagreed with the first, which a test checks never happens.
- **Box scores use SCD Type 1.** Stats legitimately get corrected, and only the latest numbers matter.
- **Bad rows are quarantined with a reason,** not dropped and not fatal.
### Gold (dbt)
 
- **Every prediction gets one outcome status:** `evaluated`, `did_not_play`, `not_played_yet`, or `no_box_score`. Only `evaluated` counts toward accuracy, so a player who sat out isn't scored as a model error.
- **Each model is compared with its own baseline** (`skill_vs_baseline`), and defaults are measured separately (`default_share`, `mae_excluding_defaults`).
- **Rolling accuracy is weighted by volume:** total error over total predictions in the window.
- **Match rate is tracked as a health metric.** Names are the only key, so a dropping match rate is the early sign of a matching problem.
### Security
 
- **Least privilege in AWS.** Airflow's access key can only assume one role, and that role can only list and read `daily_*` in one bucket. Verified by testing that the bucket root, writes, and the raw key are all denied.
- **Credentials never enter the repo.** They live in gitignored `.env` files and Airflow connections. The access key was created in the console, not by Terraform, so it isn't stored in Terraform state.
- **Analysts can read gold only.**
## Tests
 
| Test | Guarantees |
|---|---|
| `assert_predictions_never_change` | No prediction ever disagreed with an earlier copy |
| `assert_player_once_per_date` | The join to actuals can't duplicate predictions |
| `assert_outcomes_cover_every_prediction` | Gold has exactly one outcome per prediction: none lost, none duplicated |
| `warn_low_match_rate` (warning) | Flags recent days where under 90% of predictions found a box score |
| Source tests | Keys present; stats limited to the known three |
 
The match-rate warning has already paid for itself. On the first build, **every test passed while the match rate was 0%**: actuals had landed after gold was built, and gold's tables still held the earlier build. Counting rows layer by layer (26,337 bronze → 5,248 silver → 15,744 unpivoted → 0 evaluated) found the break, and a rebuild produced 15,060 evaluated predictions. Tests that check whether results **make sense** catch what tests on code alone miss.
 
## Setup
 
### Prerequisites
 
- AWS account and CLI (admin access, for Terraform only)
- Databricks workspace with Unity Catalog and a SQL warehouse (built on **Free Edition**)
- Docker Desktop and the Astro CLI
- Terraform 1.13+
- Python 3.9+ and `dbt-databricks`
### 1. Infrastructure
 
Create a catalog named `predictions` in the Databricks UI (Free Edition can't create catalogs through the API). Then:
 
```bash
cd terraform
export TF_VAR_bucket_name=<your-bucket>
terraform init
terraform apply
```
 
Create an access key for the `airflow-predictions` user in the AWS console.
 
### 2. Airflow
 
In `airflow/.env`:
 
```
AIRFLOW_CONN_AWS_PREDICTIONS='{"conn_type": "aws", "login": "<access key>", "password": "<secret key>", "extra": {"role_arn": "<reader_role_arn output>", "region_name": "us-east-2"}}'
AIRFLOW_VAR_PREDICTIONS_BUCKET=<your-bucket>
AIRFLOW_VAR_ACTUALS_API_SECRET_URL=https://<api-host>/ip
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_TOKEN=<token>
LANDING_VOLUME=/Volumes/predictions/raw/landing
```
 
```bash
cd airflow
astro dev start
```
 
Open http://localhost:8080, then trigger `ingest_predictions` (its first run loads all history) and `daily_actuals`.
 
### 3. Pipeline
 
Create a Lakeflow Declarative Pipeline with `pipelines/predictions_medallion/` as its source, default catalog `predictions`, and run it.
 
### 4. dbt
 
In a root `.env`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_HTTP_PATH`. Then:
 
```bash
source .venv/bin/activate
set -a && source .env && set +a
cd dbt
dbt build --profiles-dir .
```
 
## Status and next steps
 
**Done:** ingestion of both sources, bronze, silver, gold, and tests.
 
**Next:**
 
- [ ] **Orchestrate the daily refresh** in Airflow: land actuals, run the pipeline and wait for it, then `dbt build`. Today the last two run by hand, which is exactly how gold went stale once.
- [ ] **CI on pull requests:** DAG import checks, unit tests for the cleaning rules, and `dbt parse`.
- [ ] **Fetch the previous year during January,** so December 31 games and late corrections aren't missed.
- [ ] **A name-correction mapping** for players whose names differ between the two sources.
- [ ] **Run Airflow somewhere always-on** (a small VM or hosted Astro), so it doesn't depend on a laptop.
- [ ] **Define the pipeline as code** with a Databricks Asset Bundle, so its settings live in the repo.
 