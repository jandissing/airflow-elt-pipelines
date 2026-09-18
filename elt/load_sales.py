"""Data loading to PostgreSQL"""

import logging
from io import StringIO

import pandas as pd
from sqlalchemy import create_engine, text

from elt.config import TRANSFORMED_TABLE, get_db_connection

logger = logging.getLogger(__name__)


def normalize_sale_date(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce ``sale_date`` back to a real date after the XCom JSON round-trip.

    JSON turns the column into ISO strings (or epoch millis), which Postgres
    rejects for a DATE column. Frames without the column pass through unchanged.
    """
    if "sale_date" not in df.columns:
        return df

    normalized = df.copy()
    normalized["sale_date"] = pd.to_datetime(normalized["sale_date"]).dt.date
    return normalized


def load_to_database(**context):
    """Airflow task: replace ``transformed_sales`` with the transformed rows.

    Returns:
        int: Number of rows loaded
    """
    try:
        logger.info("=" * 60)
        logger.info("📤 LOAD TASK STARTED")
        logger.info(f"📍 Target table: {TRANSFORMED_TABLE}")

        task_instance = context["task_instance"]
        logger.info("📥 Pulling transformed_data from XCom (transform task)")
        transformed_data_json = task_instance.xcom_pull(
            task_ids="transform", key="transformed_data"
        )

        if not transformed_data_json:
            raise ValueError("No transformed data found in XCom from transform task")
        logger.info("✓ Transformed data retrieved")

        df = normalize_sale_date(pd.read_json(StringIO(transformed_data_json)))
        logger.info(f"📊 Loaded {len(df)} rows (sale_date normalized to date type)")

        logger.info("🔗 Connecting to database...")
        engine = create_engine(get_db_connection())
        logger.info("✓ Database connection established")

        logger.info(f"🗑️ Clearing existing data from {TRANSFORMED_TABLE}...")
        with engine.begin() as connection:
            connection.execute(text(f"DELETE FROM {TRANSFORMED_TABLE}"))
        logger.info("✓ Old data cleared")

        logger.info(f"💾 Inserting {len(df)} new rows...")
        df.to_sql(TRANSFORMED_TABLE, engine, if_exists="append", index=False)

        row_count = len(df)
        logger.info(f"✓ Inserted {row_count} rows")

        engine.dispose()
        logger.info("✓ Database connection closed")

        logger.info(f"✅ LOAD TASK COMPLETED - {row_count} rows loaded")
        logger.info("=" * 60)
        return row_count

    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ LOAD TASK FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        raise
