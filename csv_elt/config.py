"""Configuration for the CSV → Database ELT pipeline.

Reuses the shared database connection from elt.config and adds constants
specific to the reverse (CSV ingest) flow.
"""

from elt.config import DB_CONNECTION  # noqa: F401  (re-exported for convenience)

INPUT_DIR = "/opt/airflow/input"

REGION_SUMMARY_TABLE = "region_sales_summary"
