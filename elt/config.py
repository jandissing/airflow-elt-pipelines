"""Configuration for ELT pipeline"""

import os

DB_USER = os.getenv("POSTGRES_USER", "airflow_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow_pass_2024")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "airflow_db")

DB_CONNECTION = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

OUTPUT_DIR = "/opt/airflow/output"
INPUT_DIR = "/opt/airflow/input"

# Sales pipeline (DB -> transform -> DB/Excel)
RAW_TABLE = "raw_sales_data"
TRANSFORMED_TABLE = "transformed_sales"

# CSV ingest pipeline (CSV files -> aggregate by region -> DB)
REGION_SUMMARY_TABLE = "region_sales_summary"
