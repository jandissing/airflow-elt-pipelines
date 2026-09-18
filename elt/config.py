"""Configuration for the ELT pipelines.

Connection settings come from the environment (see ``.env.example``). The
password has no default on purpose: a missing credential fails loudly instead of
silently falling back to a value hardcoded in the repository.
"""

import os

# Non-secret defaults match the docker-compose service names, so a local
# checkout works with nothing but a POSTGRES_PASSWORD.
DEFAULT_DB_USER = "airflow_user"
DEFAULT_DB_HOST = "postgres"
DEFAULT_DB_PORT = "5432"
DEFAULT_DB_NAME = "airflow_db"

OUTPUT_DIR = os.getenv("ELT_OUTPUT_DIR", "/opt/airflow/output")
INPUT_DIR = os.getenv("ELT_INPUT_DIR", "/opt/airflow/input")

# Sales pipeline (DB -> transform -> DB/Excel)
RAW_TABLE = "raw_sales_data"
TRANSFORMED_TABLE = "transformed_sales"

# CSV ingest pipeline (CSV files -> aggregate by region -> DB)
REGION_SUMMARY_TABLE = "region_sales_summary"


def _db_parts():
    return (
        os.getenv("POSTGRES_USER", DEFAULT_DB_USER),
        os.getenv("DB_HOST", DEFAULT_DB_HOST),
        os.getenv("DB_PORT", DEFAULT_DB_PORT),
        os.getenv("POSTGRES_DB", DEFAULT_DB_NAME),
    )


def get_db_connection() -> str:
    """Build the SQLAlchemy URL for the warehouse database.

    Raises:
        RuntimeError: if POSTGRES_PASSWORD is not set in the environment.
    """
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError(
            "POSTGRES_PASSWORD is not set. Copy .env.example to .env and set it, "
            "then restart the Airflow services."
        )

    user, host, port, name = _db_parts()
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"


def get_safe_db_connection() -> str:
    """Same URL as :func:`get_db_connection`, with the password masked for logs."""
    user, host, port, name = _db_parts()
    return f"postgresql+psycopg2://{user}:***@{host}:{port}/{name}"
