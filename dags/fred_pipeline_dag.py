"""
dags/fred_pipeline_dag.py

Airflow DAG that orchestrates the full FRED pipeline:
1. Ingest data from FRED API into Snowflake (Python)
2. Run dbt staging models
3. Run dbt intermediate models
4. Run dbt mart models
5. Run dbt tests

Schedule: Daily at 6:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# -------------------------------------------
# DAG Configuration
# -------------------------------------------
default_args = {
    "owner": "lingling",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Path to dbt project inside the Docker container
DBT_PROJECT_DIR = "/opt/airflow/dbt_fred"
DBT_CMD_PREFIX = f"cd {DBT_PROJECT_DIR} && dbt"

# -------------------------------------------
# Ingestion Function
# Called by the PythonOperator
# -------------------------------------------
def run_fred_ingestion(**kwargs):
    """
    Import and run the ingestion script.
    Uses incremental mode — only fetches data since last load.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/dags/scripts")
    from ingest_fred import run_pipeline

    run_pipeline(incremental=True)


# -------------------------------------------
# DAG Definition
# -------------------------------------------
with DAG(
    dag_id="fred_financial_pipeline",
    default_args=default_args,
    description="Ingest FRED economic data, transform with dbt, run quality tests",
    schedule="0 6 * * *",       # Daily at 6:00 AM UTC
    start_date=datetime(2026, 3, 28),
    catchup=False,              # Don't backfill — we loaded historical data already
    tags=["fred", "dbt", "snowflake", "financial"],
) as dag:

    # Task 1: Ingest data from FRED API → Snowflake RAW
    ingest = PythonOperator(
        task_id="ingest_fred_data",
        python_callable=run_fred_ingestion,
    )

    # Task 2: Run dbt staging models
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD_PREFIX} run --select staging --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 3: Run dbt intermediate models
    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"{DBT_CMD_PREFIX} run --select intermediate --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 4: Run dbt mart models
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD_PREFIX} run --select marts --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 5: Run all dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"{DBT_CMD_PREFIX} test --profiles-dir {DBT_PROJECT_DIR}",
    )

    # -------------------------------------------
    # Task Dependencies (execution order)
    # -------------------------------------------
    # ingest → staging → intermediate → marts → test
    ingest >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test
