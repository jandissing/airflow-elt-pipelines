"""Data extraction from PostgreSQL"""

import logging

import pandas as pd
from sqlalchemy import create_engine

from elt.config import DB_CONNECTION, RAW_TABLE

logger = logging.getLogger(__name__)


def extract_raw_data(**context):
    """
    Extract raw sales data from PostgreSQL.

    Returns:
        int: Number of rows extracted
    """
    try:
        logger.info("=" * 60)
        logger.info(f"🔍 EXTRACT TASK STARTED")
        logger.info(f"📍 Table: {RAW_TABLE}")
        logger.info(f"🔗 Connection: {DB_CONNECTION[:50]}...")

        engine = create_engine(DB_CONNECTION)
        logger.info("✓ Database connection established")

        query = f"SELECT * FROM {RAW_TABLE} ORDER BY sale_date"
        logger.info(f"📊 Executing query: {query}")

        df = pd.read_sql(query, engine)
        engine.dispose()
        logger.info("✓ Query executed successfully")

        row_count = len(df)
        logger.info(f"✓ Extracted {row_count} rows")
        logger.info(f"📋 Columns: {list(df.columns)}")
        logger.info(f"💾 DataFrame shape: {df.shape}")

        raw_data_json = df.to_json(orient="records")
        context["task_instance"].xcom_push(key="raw_data", value=raw_data_json)
        logger.info("✓ Data pushed to XCom")

        logger.info(f"✅ EXTRACT TASK COMPLETED - {row_count} rows")
        logger.info("=" * 60)
        return row_count

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ EXTRACT TASK FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        raise
