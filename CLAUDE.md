# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

End-to-end analytics engineering pipeline: FRED API → Python ingestion → Snowflake → dbt (staging/intermediate/marts) → Streamlit dashboard, orchestrated daily by Airflow. The domain layer models U.S. recession risk (Sahm Rule, yield-curve inversion, composite signal scoring). See `README.md` for the economic rationale behind each signal and the "at scale" roadmap.

## Commands

All Python commands assume `source venv/bin/activate` and credentials in `.env` (copy from `.env.example`).

```bash
# Ingestion (FRED numeric series → RAW)
python dags/scripts/ingest_fred.py                 # full load from 2000-01-01
python dags/scripts/ingest_fred.py --incremental   # only new obs per series
python dags/scripts/ingest_fred.py --log-level DEBUG
```

```bash
# Python tests (14, no credentials or network needed, <1s)
pytest
pytest tests/test_ingest_fred.py::TestCleanObservationValue::test_parses_zero   # single test
```

```bash
# dbt — must run from dbt_fred/ with env vars exported
set -a && source .env && set +a
cd dbt_fred
dbt deps --profiles-dir .
dbt run --profiles-dir .                                   # all layers
dbt run --profiles-dir . --select staging                  # one layer (same selectors the DAG uses)
dbt run --profiles-dir . --select +int_recession_signals   # model and its upstream deps
dbt test --profiles-dir .
dbt test --profiles-dir . --select int_recession_signals
dbt test --profiles-dir . --store-failures                 # failed rows land in dbt_test__audit schema
```

```bash
# Airflow (local Docker, UI at localhost:8080, admin/admin)
docker compose up airflow-init
docker compose up -d
```

```bash
streamlit run streamlit/app.py       # dashboard at localhost:8501
```

### dbt profile gotcha

`dbt_fred/profiles.yml` is checked into the project dir (not `~/.dbt/`) because Airflow's containers read it from there. Always pass `--profiles-dir .` on local runs, as every command above does — without it dbt silently falls back to `~/.dbt/profiles.yml`, which may point at a different account or database. The DAG passes `--profiles-dir /opt/airflow/dbt_fred`, so with the flag local and Airflow runs share one profile. That profile reads `SNOWFLAKE_ACCOUNT/USER/PASSWORD` from env but **hardcodes** database `FRED_PIPELINE`, warehouse `FRED_WH`, role `ACCOUNTADMIN` — unlike `ingest_fred.py`, which honors the `SNOWFLAKE_DATABASE`/`WAREHOUSE`/`ROLE` env vars.

## Architecture

### Layers and where logic belongs

| Layer | Path | Materialization | Responsibility |
|---|---|---|---|
| RAW | `snowflake/setup.sql` | tables | Landing zone; written only by `ingest_fred.py` via MERGE |
| staging | `dbt_fred/models/staging/` | view | Dedup, type cast, NULL filter, surrogate keys |
| intermediate | `dbt_fred/models/intermediate/` | view | Pivot, derived metrics, recession signals |
| marts | `dbt_fred/models/marts/` | table | Denormalized, dashboard-facing |

Dependency chain: `stg_fred_observations` → `int_economic_indicators_pivoted` → `int_indicators_with_changes` → `int_recession_signals` → `mart_economic_dashboard` / `mart_indicator_summary`. Streamlit reads only `MARTS.*` plus `RAW.INGESTION_LOG`.

`macros/generate_schema_name.sql` overrides dbt's default so models land in bare `STAGING` / `INTERMEDIATE` / `MARTS` schemas rather than `STAGING_STAGING` etc. Don't remove it without renaming every downstream reference.

### Frequency alignment is the core modeling trick

`int_economic_indicators_pivoted` normalizes three frequencies onto one monthly grain, which every downstream window function depends on:
- Monthly series (UNRATE, CPIAUCSL, FEDFUNDS, UMCSENT) → `max(case when series_id = ...)` pivot
- Quarterly (GDP) → forward-filled across a `generator()` date spine with `last_value(... ignore nulls)`
- Daily (T10Y2Y) → month-end value via `row_number()`

Because downstream models use `lag(..., 12)` and `rows between N preceding` over `observation_month`, the output must stay exactly one row per month with no gaps. A change here breaks the Sahm Rule and all YoY metrics.

### Recession signals are threshold-parameterized

`int_recession_signals` reads thresholds from `vars` in `dbt_project.yml` (`sahm_rule_threshold`, `high_inflation_threshold`, `low_sentiment_threshold`, `fed_tightening_threshold`). Tune signals there, not in SQL. The signal count is computed once in a `signal_count` CTE and reused by the `risk_level` CTE — keep that structure rather than duplicating the sum.

### Adding a new indicator

1. Add an entry to `TARGET_SERIES` in `dags/scripts/ingest_fred.py` (id / name / frequency) — ingestion itself needs no code change.
2. Add the series ID to the `accepted_values` test in `dbt_fred/models/staging/_stg_models.yml`.
3. Handle its frequency in the right CTE of `int_economic_indicators_pivoted` and add the column through `int_indicators_with_changes` → marts.

### Ingestion contract

`ingest_fred.py` isolates errors per series: one failing series is logged to `RAW.INGESTION_LOG` and the loop continues, but `run_pipeline()` returns a summary whose non-zero `failed` makes both the CLI exit 1 and the Airflow `run_fred_ingestion` callable raise. Loads are MERGE-based, so re-running is idempotent. Pure helpers (`clean_observation_value`, `parse_args`) and HTTP functions are separated so tests need no Snowflake; `tests/conftest.py` adds `dags/scripts` to `sys.path` and monkeypatches `time.sleep` so tenacity retries don't actually wait.

### Airflow DAG

`fred_financial_pipeline`, daily 06:00 UTC, `catchup=False`, `max_active_runs=1`. Five tasks: `ingest_fred_data` (PythonOperator, 3 retries) → `dbt_run_staging` → `dbt_run_intermediate` → `dbt_run_marts` → `dbt_test_all` (BashOperators). Each has an `execution_timeout`; failures route to `alert_on_failure`, currently log-only and the intended hook point for Slack/PagerDuty. The DAG's `doc_md` carries an on-call runbook — update it when task behavior changes.

Note: `docker-compose.yml` pins `apache/airflow:2.10.4-python3.11` and installs dbt via `pip install` at container start (no custom image), even though the README advertises Airflow 3.x.

## Tests

- **pytest** (`tests/`) covers only the ingestion script — value cleaning, mocked FRED HTTP, retry behavior, CLI parsing. No Snowflake coverage.
- **dbt tests**: schema tests in `_stg_models.yml` / `_int_models.yml` / `_mart_models.yml`, source freshness on `RAW.FRED_OBSERVATIONS`, plus two singular tests in `dbt_fred/tests/` — `assert_sahm_rule_value_in_range` (catches broken rolling-window logic) and `assert_staging_to_intermediate_row_count` (catches pivot fanout or over-filtering). Both must return zero rows.

## Work in progress (untracked)

`dags/scripts/ingest_fed_documents.py` and `snowflake/setup_v2.sql` are an unfinished "v2" branch of the project: scraping FOMC statements, FOMC minutes, and Beige Books from federalreserve.gov into a new `DOCUMENTS` schema (`RAW_FED_DOCUMENTS`, `DOCUMENT_INGESTION_LOG`), deduplicated by content hash. It uses `beautifulsoup4` (not yet in `requirements.txt`), relies on hardcoded `KNOWN_FOMC_MEETINGS` / `KNOWN_BEIGE_BOOK_MONTHS` lists covering 2022–2024, has no tests, and is not wired into the DAG. Run modes: `--mode smoke` (3 sample docs) or `--mode backfill --start-year --end-year`.

## Conventions

- SQL models are written as named CTEs ending in a single `select * from <final_cte>`, with a block comment at the top explaining what the model does and *why*. Follow that style — the comments are a deliberate part of this portfolio repo.
- Snowflake returns uppercase column names; Streamlit lowercases them (`df.columns = [c.lower() ...]`) right after each `read_sql`.
- The dashboard must degrade to `N/A` rather than error when the latest month has NULLs (UMCSENT publishes with a lag).

## Working rules (from the project owner)

- Work on one GitHub Issue at a time, on its own branch. Known problems are
  tracked as Issues: don't fix anything outside the current Issue.
- Always plan first and wait for my approval before editing files.
- Do not change the existing marts or the Streamlit dashboard unless the
  current Issue requires it. New models are added alongside them.
- Leave the unfinished v2 document work (`ingest_fed_documents.py`,
  `setup_v2.sql`) untouched unless I ask.
- Never put credentials in code; they live in `.env`.
- Snowflake: use the existing extra-small warehouse; keep queries small.
- For local dbt runs, use `--profiles-dir .` so local and Airflow runs use
  the same profile.
- Before saying a task is done, run `dbt build` and `pytest` and show me
  the results, except for docs-only changes, where `dbt parse` (or `dbt ls`),
  `pytest` and CI are enough.
- After each change, explain what changed, why, and how I can verify it
  myself.
- Indicators are rates or index levels: never sum them across months.
