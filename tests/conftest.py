"""
tests/conftest.py — shared pytest configuration.

Makes the ingestion script importable by tests and speeds up tenacity
retry delays so the retry tests don't actually wait.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

# Add dags/scripts to sys.path so tests can import the ingestion module
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_DIR = _PROJECT_ROOT / "dags" / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch):
    """Skip tenacity's exponential backoff sleeps during tests."""
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
