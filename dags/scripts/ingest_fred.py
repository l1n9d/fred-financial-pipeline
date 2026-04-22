"""
ingest_fred.py — Pull FRED economic indicators into Snowflake.

This script:
1. Reads target series from a config list
2. For each series, fetches observations + metadata from FRED API
3. Loads data into Snowflake RAW tables (MERGE to avoid duplicates)
4. Logs every run to INGESTION_LOG for monitoring

Usage:
    python ingest_fred.py                    # full historical load (from 2000-01-01)
    python ingest_fred.py --incremental      # only fetch new data since last load
    python ingest_fred.py --log-level DEBUG  # verbose logging for troubleshooting
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
import snowflake.connector
from dotenv import load_dotenv
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ============================================
# Configuration
# ============================================

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE_URL = "https://api.stlouisfed.org/fred"

SF_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "database":  os.getenv("SNOWFLAKE_DATABASE", "FRED_PIPELINE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "FRED_WH"),
    "role":      os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
}

# Target series to ingest
TARGET_SERIES: list[dict[str, str]] = [
    {"id": "UNRATE",   "name": "Unemployment Rate",        "frequency": "monthly"},
    {"id": "CPIAUCSL", "name": "Consumer Price Index",     "frequency": "monthly"},
    {"id": "FEDFUNDS", "name": "Federal Funds Rate",       "frequency": "monthly"},
    {"id": "GDP",      "name": "Gross Domestic Product",   "frequency": "quarterly"},
    {"id": "T10Y2Y",   "name": "10Y-2Y Treasury Spread",   "frequency": "daily"},
    {"id": "UMCSENT",  "name": "Consumer Sentiment Index", "frequency": "monthly"},
]

DEFAULT_START_DATE = "2000-01-01"
API_TIMEOUT_SECONDS = 30
INTER_REQUEST_DELAY_SECONDS = 0.5

logger = logging.getLogger(__name__)


# ============================================
# Custom Exceptions
# ============================================

class FREDAPIError(Exception):
    """Raised when the FRED API returns an unexpected or malformed response."""


# ============================================
# Pure helpers (no I/O — easily unit-testable)
# ============================================

def clean_observation_value(raw_value: str) -> float | None:
    """
    Convert a raw FRED observation value string to a float or None.

    FRED uses the literal string '.' to represent missing values. All other
    values are numeric strings and convert cleanly to float.

    >>> clean_observation_value("3.7")
    3.7
    >>> clean_observation_value(".") is None
    True
    """
    if raw_value == ".":
        return None
    return float(raw_value)


# ============================================
# FRED API Functions
# ============================================

@retry(
    retry=retry_if_exception_type(
        (requests.HTTPError, requests.Timeout, requests.ConnectionError)
    ),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def fetch_observations(
    series_id: str,
    start_date: str = DEFAULT_START_DATE,
) -> list[dict[str, Any]]:
    """
    Fetch observations for a single FRED series.

    Retries up to 3 times on transient network errors with exponential backoff.
    Raises FREDAPIError if the response is malformed.
    """
    url = f"{FRED_BASE_URL}/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "sort_order": "asc",
    }

    response = requests.get(url, params=params, timeout=API_TIMEOUT_SECONDS)
    response.raise_for_status()

    payload = response.json()
    if "observations" not in payload:
        raise FREDAPIError(
            f"Unexpected FRED response for {series_id}: missing 'observations' key. "
            f"Response keys: {list(payload.keys())}"
        )

    return payload["observations"]


@retry(
    retry=retry_if_exception_type(
        (requests.HTTPError, requests.Timeout, requests.ConnectionError)
    ),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def fetch_series_metadata(series_id: str) -> dict[str, Any]:
    """Fetch metadata (title, frequency, units, etc.) for a FRED series."""
    url = f"{FRED_BASE_URL}/series"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
    }

    response = requests.get(url, params=params, timeout=API_TIMEOUT_SECONDS)
    response.raise_for_status()

    series_list = response.json().get("seriess", [])
    return series_list[0] if series_list else {}


# ============================================
# Snowflake Functions
# ============================================

def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and return a Snowflake connection. Validates config first."""
    missing = [k for k, v in SF_CONFIG.items() if v is None]
    if missing:
        raise ValueError(
            f"Missing Snowflake config for: {missing}. "
            "Check your .env file or environment variables."
        )
    return snowflake.connector.connect(**SF_CONFIG)


def get_last_observation_date(conn, series_id: str) -> str:
    """
    Return the most recent observation_date for a series in Snowflake,
    or DEFAULT_START_DATE if the series has no data yet.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT MAX(observation_date)
            FROM RAW.FRED_OBSERVATIONS
            WHERE series_id = %s
            """,
            (series_id,),
        )
        result = cursor.fetchone()[0]
    finally:
        cursor.close()

    if result is None:
        return DEFAULT_START_DATE
    return result.strftime("%Y-%m-%d")


def load_observations(
    conn,
    series_id: str,
    observations: list[dict[str, Any]],
) -> int:
    """
    Load observations into Snowflake via MERGE (upsert).
    Idempotent — safe to re-run without creating duplicates.
    Returns the number of rows merged.
    """
    if not observations:
        return 0

    rows = [
        (
            series_id,
            obs["date"],
            clean_observation_value(obs["value"]),
            f"{FRED_BASE_URL}/series/observations?series_id={series_id}",
        )
        for obs in observations
    ]

    cursor = conn.cursor()
    try:
        cursor.execute(
            "CREATE OR REPLACE TEMPORARY TABLE RAW.TEMP_OBSERVATIONS "
            "LIKE RAW.FRED_OBSERVATIONS"
        )

        cursor.executemany(
            """
            INSERT INTO RAW.TEMP_OBSERVATIONS
                (series_id, observation_date, value, source_url)
            VALUES (%s, %s, %s, %s)
            """,
            rows,
        )

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
    finally:
        cursor.close()

    return row_count


def load_metadata(conn, series_id: str, metadata: dict[str, Any]) -> None:
    """Upsert series metadata via DELETE + INSERT (simple for small table)."""
    if not metadata:
        return

    cursor = conn.cursor()
    try:
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
    finally:
        cursor.close()


def log_ingestion(
    conn,
    run_id: str,
    series_id: str,
    records_fetched: int,
    status: str,
    started_at: datetime,
    error_message: str | None = None,
) -> None:
    """Write a row to the ingestion log for monitoring."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO RAW.INGESTION_LOG
                (run_id, series_id, records_fetched, status,
                 started_at, completed_at, error_message)
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
    finally:
        cursor.close()


# ============================================
# Main Pipeline
# ============================================

def run_pipeline(incremental: bool = False) -> dict[str, int]:
    """
    Loop through all target series, fetch data from FRED, load into Snowflake.

    Args:
        incremental: If True, only fetch data since last loaded date per series.
                     If False, fetch full history from DEFAULT_START_DATE.

    Returns:
        Summary dict with keys: succeeded, failed, total.
    """
    run_id = str(uuid.uuid4())
    mode = "INCREMENTAL" if incremental else "FULL LOAD"

    logger.info("=" * 60)
    logger.info("FRED Pipeline Run: %s", run_id)
    logger.info("Mode: %s", mode)
    logger.info("Started: %s", datetime.now(timezone.utc).isoformat())
    logger.info("=" * 60)

    conn = get_connection()
    conn.cursor().execute("USE SCHEMA RAW")

    total_series = len(TARGET_SERIES)
    success_count = 0
    fail_count = 0

    try:
        for i, series in enumerate(TARGET_SERIES, 1):
            series_id = series["id"]
            series_start = datetime.now(timezone.utc)

            logger.info(
                "[%d/%d] Processing %s (%s)",
                i, total_series, series_id, series["name"],
            )

            try:
                start_date = (
                    get_last_observation_date(conn, series_id)
                    if incremental
                    else DEFAULT_START_DATE
                )
                logger.info("  Fetching data from %s", start_date)

                observations = fetch_observations(series_id, start_date)
                logger.info("  API returned %d observations", len(observations))

                rows_merged = load_observations(conn, series_id, observations)
                logger.info("  Merged %d rows into FRED_OBSERVATIONS", rows_merged)

                metadata = fetch_series_metadata(series_id)
                load_metadata(conn, series_id, metadata)
                logger.debug("  Metadata updated for %s", series_id)

                log_ingestion(
                    conn, run_id, series_id,
                    len(observations), "SUCCESS", series_start,
                )
                success_count += 1
                logger.info("  ✓ %s complete", series_id)

            except Exception as e:  # noqa: BLE001 — we log and continue per-series
                error_msg = str(e)[:2000]
                log_ingestion(
                    conn, run_id, series_id, 0,
                    "FAILED", series_start, error_msg,
                )
                fail_count += 1
                logger.error("  ✗ %s FAILED: %s", series_id, error_msg)

            time.sleep(INTER_REQUEST_DELAY_SECONDS)

        conn.commit()
    finally:
        conn.close()

    logger.info("=" * 60)
    logger.info(
        "Pipeline Complete — Succeeded: %d/%d, Failed: %d/%d",
        success_count, total_series, fail_count, total_series,
    )
    logger.info("Run ID: %s", run_id)
    logger.info("=" * 60)

    return {"succeeded": success_count, "failed": fail_count, "total": total_series}


# ============================================
# CLI Entry Point
# ============================================

def configure_logging(level: str = "INFO") -> None:
    """Configure root logging for standalone script execution."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Ingest FRED economic indicators into Snowflake.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Fetch only data since the most recent observation in Snowflake "
             "(default: full load from 2000-01-01).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns exit code suitable for shell use."""
    args = parse_args(argv)
    configure_logging(args.log_level)

    try:
        summary = run_pipeline(incremental=args.incremental)
    except Exception:
        logger.exception("Pipeline failed with unhandled exception")
        return 1

    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
