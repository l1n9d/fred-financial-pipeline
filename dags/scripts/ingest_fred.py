"""
ingest_fred.py — Pull FRED economic indicators into Snowflake

This script:
1. Reads target series from a config list
2. For each series, fetches observations + metadata from FRED API
3. Loads data into Snowflake RAW tables (MERGE to avoid duplicates)
4. Logs every run to INGESTION_LOG for monitoring

Usage:
    python ingest_fred.py              # full historical load (from 2000-01-01)
    python ingest_fred.py --incremental # only fetch new data since last load
"""

import os
import uuid
import time
import requests
import pandas as pd
import snowflake.connector
from datetime import datetime, timezone
from dotenv import load_dotenv

# ============================================
# Configuration
# ============================================

load_dotenv()  # loads from .env file

# FRED API
FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE_URL = "https://api.stlouisfed.org/fred"

# Snowflake connection
SF_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "database":  "FRED_PIPELINE",
    "warehouse": "FRED_WH",
    "role":      "ACCOUNTADMIN",
}

# Target series to ingest
TARGET_SERIES = [
    {"id": "UNRATE",   "name": "Unemployment Rate",        "frequency": "monthly"},
    {"id": "CPIAUCSL", "name": "Consumer Price Index",      "frequency": "monthly"},
    {"id": "FEDFUNDS", "name": "Federal Funds Rate",        "frequency": "monthly"},
    {"id": "GDP",      "name": "Gross Domestic Product",    "frequency": "quarterly"},
    {"id": "T10Y2Y",   "name": "10Y-2Y Treasury Spread",   "frequency": "daily"},
    {"id": "UMCSENT",  "name": "Consumer Sentiment Index",  "frequency": "monthly"},
]

DEFAULT_START_DATE = "2000-01-01"


# ============================================
# FRED API Functions
# ============================================

def fetch_observations(series_id: str, start_date: str = DEFAULT_START_DATE) -> list[dict]:
    """
    Fetch observations for a single FRED series.
    Returns a list of dicts: [{"date": "2024-01-01", "value": "3.7"}, ...]
    """
    url = f"{FRED_BASE_URL}/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "sort_order": "asc",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    observations = response.json().get("observations", [])
    return observations


def fetch_series_metadata(series_id: str) -> dict:
    """
    Fetch metadata (title, frequency, units, etc.) for a FRED series.
    """
    url = f"{FRED_BASE_URL}/series"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    series_list = response.json().get("seriess", [])
    if series_list:
        return series_list[0]
    return {}


# ============================================
# Snowflake Functions
# ============================================

def get_connection():
    """Create and return a Snowflake connection."""
    conn = snowflake.connector.connect(**SF_CONFIG)
    return conn


def get_last_observation_date(conn, series_id: str) -> str:
    """
    Get the most recent observation_date for a series in Snowflake.
    Used for incremental loads — only fetch data after this date.
    Returns date string like '2024-06-01', or DEFAULT_START_DATE if no data.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT MAX(observation_date)
        FROM RAW.FRED_OBSERVATIONS
        WHERE series_id = %s
        """,
        (series_id,),
    )
    result = cursor.fetchone()[0]
    cursor.close()

    if result is None:
        return DEFAULT_START_DATE
    return result.strftime("%Y-%m-%d")


def load_observations(conn, series_id: str, observations: list[dict]) -> int:
    """
    Load observations into Snowflake using MERGE (upsert).
    This makes the script idempotent — safe to re-run without duplicates.
    Returns the number of rows merged.
    """
    if not observations:
        return 0

    cursor = conn.cursor()

    # Clean the data: FRED returns '.' for missing values
    rows = []
    for obs in observations:
        value = obs["value"]
        if value == ".":
            value = None  # will become NULL in Snowflake
        else:
            value = float(value)
        rows.append((
            series_id,
            obs["date"],
            value,
            f"{FRED_BASE_URL}/series/observations?series_id={series_id}",
        ))

    # Create a temp table, bulk insert, then MERGE into target
    cursor.execute("CREATE OR REPLACE TEMPORARY TABLE RAW.TEMP_OBSERVATIONS LIKE RAW.FRED_OBSERVATIONS")

    # Bulk insert into temp table
    cursor.executemany(
        """
        INSERT INTO RAW.TEMP_OBSERVATIONS (series_id, observation_date, value, source_url)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )

    # MERGE: update existing rows, insert new ones
    cursor.execute(
        """
        MERGE INTO RAW.FRED_OBSERVATIONS AS target
        USING RAW.TEMP_OBSERVATIONS AS source
        ON target.series_id = source.series_id
           AND target.observation_date = source.observation_date
        WHEN MATCHED THEN
            UPDATE SET
                value = source.value,
                ingested_at = CURRENT_TIMESTAMP(),
                source_url = source.source_url
        WHEN NOT MATCHED THEN
            INSERT (series_id, observation_date, value, ingested_at, source_url)
            VALUES (source.series_id, source.observation_date, source.value,
                    CURRENT_TIMESTAMP(), source.source_url)
        """
    )

    row_count = cursor.rowcount
    cursor.execute("DROP TABLE IF EXISTS RAW.TEMP_OBSERVATIONS")
    cursor.close()

    return row_count


def load_metadata(conn, series_id: str, metadata: dict) -> None:
    """
    Load or update series metadata in Snowflake.
    Uses DELETE + INSERT (simple upsert for small table).
    """
    if not metadata:
        return

    cursor = conn.cursor()

    cursor.execute(
        "DELETE FROM RAW.FRED_SERIES_METADATA WHERE series_id = %s",
        (series_id,),
    )

    cursor.execute(
        """
        INSERT INTO RAW.FRED_SERIES_METADATA
            (series_id, title, frequency, units, seasonal_adj, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            series_id,
            metadata.get("title"),
            metadata.get("frequency"),
            metadata.get("units"),
            metadata.get("seasonal_adjustment"),
            metadata.get("last_updated"),
        ),
    )

    cursor.close()


def log_ingestion(conn, run_id: str, series_id: str,
                  records_fetched: int, status: str,
                  started_at: datetime, error_message: str = None) -> None:
    """Write a row to the ingestion log for monitoring."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO RAW.INGESTION_LOG
            (run_id, series_id, records_fetched, status, started_at, completed_at, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            series_id,
            records_fetched,
            status,
            started_at,
            datetime.now(timezone.utc),
            error_message,
        ),
    )
    cursor.close()


# ============================================
# Main Pipeline
# ============================================

def run_pipeline(incremental: bool = False):
    """
    Main entry point. Loops through all target series,
    fetches data from FRED, loads into Snowflake.

    Args:
        incremental: If True, only fetch data since last loaded date.
                     If False, fetch full history from DEFAULT_START_DATE.
    """
    run_id = str(uuid.uuid4())
    print(f"\n{'='*60}")
    print(f"FRED Pipeline Run: {run_id}")
    print(f"Mode: {'INCREMENTAL' if incremental else 'FULL LOAD'}")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    conn = get_connection()
    conn.cursor().execute("USE SCHEMA RAW")

    total_series = len(TARGET_SERIES)
    success_count = 0
    fail_count = 0

    for i, series in enumerate(TARGET_SERIES, 1):
        series_id = series["id"]
        series_start = datetime.now(timezone.utc)

        print(f"[{i}/{total_series}] Processing {series_id} ({series['name']})...")

        try:
            # Determine start date
            if incremental:
                start_date = get_last_observation_date(conn, series_id)
                print(f"  Incremental: fetching data after {start_date}")
            else:
                start_date = DEFAULT_START_DATE
                print(f"  Full load: fetching data from {start_date}")

            # Fetch from FRED API
            observations = fetch_observations(series_id, start_date)
            print(f"  API returned {len(observations)} observations")

            # Load observations into Snowflake
            rows_merged = load_observations(conn, series_id, observations)
            print(f"  Merged {rows_merged} rows into FRED_OBSERVATIONS")

            # Load metadata
            metadata = fetch_series_metadata(series_id)
            load_metadata(conn, series_id, metadata)
            print(f"  Metadata updated")

            # Log success
            log_ingestion(conn, run_id, series_id, len(observations), "SUCCESS", series_start)
            success_count += 1
            print(f"  ✓ Done!\n")

        except Exception as e:
            # Log failure — but continue with other series
            error_msg = str(e)[:2000]
            log_ingestion(conn, run_id, series_id, 0, "FAILED", series_start, error_msg)
            fail_count += 1
            print(f"  ✗ FAILED: {error_msg}\n")

        # Rate limiting: small pause between API calls
        time.sleep(0.5)

    conn.commit()
    conn.close()

    # Summary
    print(f"{'='*60}")
    print(f"Pipeline Complete!")
    print(f"  Succeeded: {success_count}/{total_series}")
    print(f"  Failed:    {fail_count}/{total_series}")
    print(f"  Run ID:    {run_id}")
    print(f"{'='*60}\n")


# ============================================
# Entry Point
# ============================================

if __name__ == "__main__":
    import sys

    incremental = "--incremental" in sys.argv
    run_pipeline(incremental=incremental)