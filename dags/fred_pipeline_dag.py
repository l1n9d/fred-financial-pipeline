"""
dags/fred_pipeline_dag.py

Airflow DAG orchestrating the full FRED pipeline:
1. Ingest data from FRED API into Snowflake (Python)
2. Run dbt staging models
3. Run dbt intermediate models
4. Run dbt mart models
5. Run all dbt tests

Schedule: Daily at 6:00 AM UTC
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ============================================
# Configuration
# ============================================

DBT_PROJECT_DIR = "/opt/airflow/dbt_fred"
DBT_CMD_PREFIX = f"cd {DBT_PROJECT_DIR} && dbt"


# ============================================
# Failure Callback
# ============================================

def alert_on_failure(context: dict[str, Any]) -> None:
    """
    Called by Airflow when any task in the DAG fails.

    Logs structured failure details to stdout (captured by Airflow's task
    log infrastructure). In production, extend this to post alerts to
    Slack or PagerDuty — e.g.:

        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        hook = SlackWebhookHook(slack_webhook_conn_id='slack_alerts')
        hook.send(text=message)
    """
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    dag_run = context.get("dag_run")

    message = (
        f"FRED Pipeline task FAILED\n"
        f"  DAG:       {task_instance.dag_id}\n"
        f"  Task:      {task_instance.task_id}\n"
        f"  Run:       {dag_run.run_id if dag_run else 'unknown'}\n"
        f"  Try:       {task_instance.try_number}/{task_instance.max_tries}\n"
        f"  Exception: {exception}\n"
        f"  Log URL:   {task_instance.log_url}"
    )
    logger.error(message)


# ============================================
# Python Callable for Ingestion Task
# ============================================

def run_fred_ingestion(**context: Any) -> None:
    """
    Entry point for the ingestion task. Runs the ingestion script in
    incremental mode (only fetches data since last load).

    Raises RuntimeError if any series failed to load — this surfaces the
    failure in the Airflow UI rather than burying it in INGESTION_LOG.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/dags/scripts")
    from ingest_fred import run_pipeline

    summary = run_pipeline(incremental=True)

    if summary["failed"] > 0:
        raise RuntimeError(
            f"Ingestion completed with {summary['failed']}/{summary['total']} "
            "series failed — check RAW.INGESTION_LOG for details."
        )


# ============================================
# DAG Documentation (rendered in Airflow UI)
# ============================================

DAG_DOC_MD = """
# FRED Financial Indicators Pipeline

Daily pipeline that ingests U.S. economic indicators from the Federal Reserve
(FRED API), transforms them through a dbt staging → intermediate → marts
architecture in Snowflake, and powers a Streamlit recession-risk dashboard.

## Task Flow

```
ingest_fred_data
        ↓
dbt_run_staging       (2 models: cleaned observations, metadata)
        ↓
dbt_run_intermediate  (3 models: pivoted indicators, derived metrics,
                       recession signals)
        ↓
dbt_run_marts         (2 models: dashboard table, indicator summary)
        ↓
dbt_test_all          (21 tests across all layers)
```

## Key Design Decisions

- **Incremental ingestion** — fetches only data since last observation date
  per series, making daily runs fast and cheap.
- **Idempotent** — MERGE-based upserts mean manual re-runs are safe.
- **Error isolation** — one failed FRED series does not stop other series,
  but the ingest task fails overall if any series failed (surfaces in UI).
- **Execution timeouts** — each task has a hard time limit to prevent
  runaway runs consuming Snowflake credits.
- **Failure callbacks** — task failures trigger `alert_on_failure` for
  logging; extensible to Slack/PagerDuty in production.

## On-Call Runbook

- **Ingestion failure** — check `RAW.INGESTION_LOG` in Snowflake for the
  failing series and error message. Usually a transient FRED API issue.
  Retry manually: `python dags/scripts/ingest_fred.py --incremental`.
- **dbt run failure** — check task logs. Most common cause is schema drift
  from FRED metadata updates; rebuild sources with `dbt run --select staging`.
- **dbt test failure** — run `dbt test --store-failures` to inspect failed
  rows in Snowflake under the `dbt_test__audit` schema.
- **Execution timeout** — the Snowflake warehouse may have been suspended
  and cold-started; verify `FRED_WH` is running and consider increasing
  the timeout if cold starts are common.
"""


# ============================================
# DAG Default Args
# ============================================

default_args = {
    "owner": "lingling",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id="fred_financial_pipeline",
    default_args=default_args,
    description="Ingest FRED economic data, transform with dbt, run quality tests",
    doc_md=DAG_DOC_MD,
    schedule="0 6 * * *",              # Daily at 6:00 AM UTC
    start_date=datetime(2026, 3, 28),
    catchup=False,                     # Historical data already loaded
    max_active_runs=1,                 # No overlapping runs
    dagrun_timeout=timedelta(minutes=45),
    tags=["fred", "dbt", "snowflake", "financial"],
) as dag:

    # Task 1: Ingest data from FRED API → Snowflake RAW
    ingest = PythonOperator(
        task_id="ingest_fred_data",
        python_callable=run_fred_ingestion,
        execution_timeout=timedelta(minutes=10),
        retries=3,                     # API tasks are flakier than dbt; retry more
        doc_md="""
        ### Ingest FRED Data

        Fetches 6 economic indicators from the FRED API and loads them into
        `RAW.FRED_OBSERVATIONS` and `RAW.FRED_SERIES_METADATA` via MERGE
        upserts. Runs in incremental mode (only new observations since last
        load per series).

        The underlying script has its own retry logic for transient API
        errors via tenacity. This task-level retry handles longer outages
        or Snowflake connection issues.
        """,
    )

    # Task 2: Run dbt staging models
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"{DBT_CMD_PREFIX} run --select staging "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # Task 3: Run dbt intermediate models
    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=(
            f"{DBT_CMD_PREFIX} run --select intermediate "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # Task 4: Run dbt mart models
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"{DBT_CMD_PREFIX} run --select marts "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # Task 5: Run all dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"{DBT_CMD_PREFIX} test --profiles-dir {DBT_PROJECT_DIR}",
        execution_timeout=timedelta(minutes=10),
        doc_md="""
        ### dbt Quality Tests

        Runs all 21 dbt tests across staging/intermediate/marts:
        - **Uniqueness** — surrogate keys on observations and metadata
        - **Not-null** — required fields across all layers
        - **Accepted values** — `series_id` restricted to known indicators

        Failures here indicate upstream data quality issues and should block
        dashboard publishing. Use `dbt test --store-failures` to inspect
        failed rows in Snowflake.
        """,
    )

    # Task Dependencies: ingest → staging → intermediate → marts → test
    ingest >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test
