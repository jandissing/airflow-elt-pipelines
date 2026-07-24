"""Load the per-region summary into PostgreSQL."""

import logging

import pandas as pd
from sqlalchemy import create_engine, text

from csv_elt.config import DB_CONNECTION, REGION_SUMMARY_TABLE

logger = logging.getLogger(__name__)


def load_region_summary(**context):
    """
    Load the region summary into REGION_SUMMARY_TABLE (truncate + append).

    Returns:
        int: Number of rows loaded
    """
    try:
        logger.info("=" * 60)
        logger.info("📤 LOAD (REGION SUMMARY) TASK STARTED")
        logger.info(f"📍 Target table: {REGION_SUMMARY_TABLE}")

        task_instance = context["task_instance"]
        logger.info("📥 Pulling region_summary from XCom (transform task)")
        summary_json = task_instance.xcom_pull(
            task_ids="aggregate_by_region", key="region_summary"
        )

        if not summary_json:
            raise ValueError("No region summary found in XCom from transform task")
        logger.info("✓ Region summary retrieved")

        df = pd.read_json(summary_json)
        logger.info(f"📊 Loaded {len(df)} region rows")

        logger.info("🔗 Connecting to database...")
        engine = create_engine(DB_CONNECTION)
        logger.info("✓ Database connection established")

        logger.info(f"🗑️ Clearing existing data from {REGION_SUMMARY_TABLE}...")
        with engine.begin() as connection:
            connection.execute(text(f"DELETE FROM {REGION_SUMMARY_TABLE}"))
        logger.info("✓ Old data cleared")

        logger.info(f"💾 Inserting {len(df)} region rows...")
        df.to_sql(REGION_SUMMARY_TABLE, engine, if_exists="append", index=False)

        row_count = len(df)
        logger.info(f"✓ Inserted {row_count} rows")

        engine.dispose()
        logger.info("✓ Database connection closed")

        logger.info(f"✅ LOAD TASK COMPLETED - {row_count} region rows loaded")
        logger.info("=" * 60)
        return row_count

    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ LOAD (REGION SUMMARY) TASK FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        raise
