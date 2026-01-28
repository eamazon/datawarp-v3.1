"""Pytest configuration for DataWarp v3.1 test suite."""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests (may require network access)"
    )
