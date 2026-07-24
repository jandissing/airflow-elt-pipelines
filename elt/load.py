"""Data loading to PostgreSQL"""

import logging

import pandas as pd
from sqlalchemy import create_engine, text

from elt.config import DB_CONNECTION, TRANSFORMED_TABLE

logger = logging.getLogger(__name__)


def load_to_database(**context):
    """
    Load transformed data into transformed_sales table.

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

        df = pd.read_json(transformed_data_json)
        logger.info(f"📊 Loaded {len(df)} rows")

        logger.info(f"🔗 Connecting to database...")
        engine = create_engine(DB_CONNECTION)
        logger.info("✓ Database connection established")

        logger.info(f"🗑️ Clearing existing data from {TRANSFORMED_TABLE}...")
        with engine.begin() as connection:
            connection.execute(text(f"DELETE FROM {TRANSFORMED_TABLE}"))
        logger.info(f"✓ Old data cleared")

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
