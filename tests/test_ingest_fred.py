"""
tests/test_ingest_fred.py — Unit tests for the FRED ingestion script.

Covers:
- Pure data transformation (value cleaning)
- FRED API response parsing (mocked HTTP)
- Retry behavior on transient errors
- CLI argument parsing
- Custom exception on malformed API responses
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from ingest_fred import (
    FREDAPIError,
    clean_observation_value,
    fetch_observations,
    parse_args,
)


# ============================================
# clean_observation_value — pure function tests
# ============================================

class TestCleanObservationValue:
    """The pure function that converts FRED's string values to numeric."""

    def test_returns_none_for_fred_missing_marker(self):
        # FRED uses '.' to represent missing values
        assert clean_observation_value(".") is None

    def test_parses_positive_float(self):
        assert clean_observation_value("3.7") == 3.7

    def test_parses_zero(self):
        assert clean_observation_value("0") == 0.0

    def test_parses_negative(self):
        # Treasury spread can go negative
        assert clean_observation_value("-1.5") == -1.5

    def test_parses_integer_string(self):
        assert clean_observation_value("42") == 42.0

    def test_raises_value_error_on_garbage_input(self):
        with pytest.raises(ValueError):
            clean_observation_value("not_a_number")


# ============================================
# fetch_observations — mocked HTTP tests
# ============================================

class TestFetchObservations:
    """Tests FRED API interaction with mocked HTTP responses."""

    @patch("ingest_fred.requests.get")
    def test_returns_observations_list(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "observations": [
                {"date": "2024-01-01", "value": "3.7"},
                {"date": "2024-02-01", "value": "3.9"},
            ]
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_observations("UNRATE", "2024-01-01")

        assert len(result) == 2
        assert result[0]["date"] == "2024-01-01"
        assert result[0]["value"] == "3.7"

    @patch("ingest_fred.requests.get")
    def test_passes_correct_query_params(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"observations": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetch_observations("UNRATE", "2020-01-01")

        call_kwargs = mock_get.call_args.kwargs
        assert call_kwargs["params"]["series_id"] == "UNRATE"
        assert call_kwargs["params"]["observation_start"] == "2020-01-01"
        assert call_kwargs["params"]["file_type"] == "json"
        assert call_kwargs["params"]["sort_order"] == "asc"

    @patch("ingest_fred.requests.get")
    def test_raises_fred_api_error_on_malformed_response(self, mock_get):
        # Simulate a FRED error response (e.g., invalid API key or bad series)
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "error_code": 400,
            "error_message": "Bad Request",
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with pytest.raises(FREDAPIError, match="missing 'observations'"):
            fetch_observations("INVALID_SERIES")

    @patch("ingest_fred.requests.get")
    def test_retries_on_transient_http_error_then_succeeds(self, mock_get):
        """Tenacity should retry on HTTPError up to 3 times total."""
        failing = MagicMock()
        failing.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

        success = MagicMock()
        success.json.return_value = {
            "observations": [{"date": "2024-01-01", "value": "3.7"}]
        }
        success.raise_for_status.return_value = None

        # Two failures, then success on 3rd attempt
        mock_get.side_effect = [failing, failing, success]

        result = fetch_observations("UNRATE", "2024-01-01")

        assert len(result) == 1
        assert mock_get.call_count == 3


# ============================================
# parse_args — CLI tests
# ============================================

class TestParseArgs:
    """Tests CLI argument parsing for the script entry point."""

    def test_default_is_full_load(self):
        args = parse_args([])
        assert args.incremental is False
        assert args.log_level == "INFO"

    def test_incremental_flag_enables_incremental_mode(self):
        args = parse_args(["--incremental"])
        assert args.incremental is True

    def test_log_level_can_be_overridden(self):
        args = parse_args(["--log-level", "DEBUG"])
        assert args.log_level == "DEBUG"

    def test_invalid_log_level_rejected(self):
        # argparse exits with SystemExit on invalid choices
        with pytest.raises(SystemExit):
            parse_args(["--log-level", "NOT_A_LEVEL"])
