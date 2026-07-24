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
        logger.info(f"Starting data extraction from {RAW_TABLE} table")

        engine = create_engine(DB_CONNECTION)
        query = f"SELECT * FROM {RAW_TABLE} ORDER BY sale_date"

        df = pd.read_sql(query, engine)
        engine.dispose()

        row_count = len(df)
        logger.info(f"Successfully extracted {row_count} rows from {RAW_TABLE}")

        raw_data_json = df.to_json(orient="records")
        context["task_instance"].xcom_push(key="raw_data", value=raw_data_json)

        return row_count

    except Exception as e:
        logger.error(f"Error during data extraction: {str(e)}", exc_info=True)
        raise
