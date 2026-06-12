"""Smoke test: package imports and reports its version."""

from rr_analytics import __version__


def test_version() -> None:
    assert __version__ == "0.1.0"
