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
        logger.info("Starting data load to database")

        task_instance = context["task_instance"]
        transformed_data_json = task_instance.xcom_pull(
            task_ids="transform", key="transformed_data"
        )

        if not transformed_data_json:
            raise ValueError("No transformed data found in XCom")

        df = pd.read_json(transformed_data_json)

        engine = create_engine(DB_CONNECTION)

        with engine.begin() as connection:
            connection.execute(text(f"DELETE FROM {TRANSFORMED_TABLE}"))
            logger.info(f"Cleared existing data from {TRANSFORMED_TABLE} table")

        df.to_sql(TRANSFORMED_TABLE, engine, if_exists="append", index=False)

        row_count = len(df)
        logger.info(f"Successfully loaded {row_count} rows into {TRANSFORMED_TABLE} table")

        engine.dispose()

        return row_count

    except Exception as e:
        logger.error(f"Error during data load: {str(e)}", exc_info=True)
        raise
