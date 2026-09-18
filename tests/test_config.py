"""Unit tests for database configuration handling."""

import pytest

from elt import config

ENV = {
    "POSTGRES_USER": "someone",
    "POSTGRES_PASSWORD": "s3cret",
    "POSTGRES_DB": "warehouse",
    "DB_HOST": "db.internal",
    "DB_PORT": "6543",
}


def _set_env(monkeypatch, **overrides):
    values = {**ENV, **overrides}
    for key, value in values.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)


def test_builds_the_connection_string_from_the_environment(monkeypatch):
    _set_env(monkeypatch)

    assert config.get_db_connection() == (
        "postgresql+psycopg2://someone:s3cret@db.internal:6543/warehouse"
    )


def test_refuses_to_build_a_connection_without_a_password(monkeypatch):
    _set_env(monkeypatch, POSTGRES_PASSWORD=None)

    with pytest.raises(RuntimeError) as excinfo:
        config.get_db_connection()

    assert "POSTGRES_PASSWORD" in str(excinfo.value)


def test_hides_the_password_in_the_loggable_connection_string(monkeypatch):
    _set_env(monkeypatch)

    safe = config.get_safe_db_connection()

    assert "s3cret" not in safe
    assert "db.internal:6543/warehouse" in safe
