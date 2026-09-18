"""Data extraction from PostgreSQL"""

import logging

import pandas as pd
from sqlalchemy import create_engine

from elt.config import RAW_TABLE, get_db_connection, get_safe_db_connection

logger = logging.getLogger(__name__)


def extract_raw_data(**context):
    """Airflow task: read the raw sales table and publish the rows to XCom.

    Returns:
        int: Number of rows extracted
    """
    try:
        logger.info("=" * 60)
        logger.info("🔍 EXTRACT TASK STARTED")
        logger.info(f"📍 Table: {RAW_TABLE}")
        logger.info(f"🔗 Connection: {get_safe_db_connection()}")

        engine = create_engine(get_db_connection())
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

        raw_data_json = df.to_json(orient="records", date_format="iso")
        context["task_instance"].xcom_push(key="raw_data", value=raw_data_json)
        logger.info("✓ Data pushed to XCom")

        logger.info(f"✅ EXTRACT TASK COMPLETED - {row_count} rows")
        logger.info("=" * 60)
        return row_count

    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ EXTRACT TASK FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        raise
