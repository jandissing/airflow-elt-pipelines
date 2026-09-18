"""Aggregate sales rows into a per-region summary."""

import logging
from io import StringIO

import pandas as pd

logger = logging.getLogger(__name__)


def summarize_by_region(df: pd.DataFrame) -> pd.DataFrame:
    """Group sales rows by region and compute summary metrics.

    Produces one row per region with:
      - total_quantity: sum of quantity
      - total_revenue:  sum of quantity * unit_price
      - avg_unit_price: mean unit_price
      - num_orders:     number of source rows

    The input frame is not modified.
    """
    with_revenue = df.assign(line_revenue=df["quantity"] * df["unit_price"])

    summary = (
        with_revenue.groupby("region")
        .agg(
            total_quantity=("quantity", "sum"),
            total_revenue=("line_revenue", "sum"),
            avg_unit_price=("unit_price", "mean"),
            num_orders=("region", "size"),
        )
        .reset_index()
    )

    # Round monetary/avg columns for a clean summary table
    summary["total_revenue"] = summary["total_revenue"].round(2)
    summary["avg_unit_price"] = summary["avg_unit_price"].round(2)
    return summary


def aggregate_by_region(**context):
    """Airflow task: read the extracted rows from XCom, publish a region summary.

    Returns:
        int: Number of regions in the summary
    """
    try:
        logger.info("=" * 60)
        logger.info("🔄 TRANSFORM (AGGREGATE BY REGION) TASK STARTED")

        task_instance = context["task_instance"]
        logger.info("📥 Pulling csv_raw_data from XCom (extract task)")
        raw_data_json = task_instance.xcom_pull(
            task_ids="extract_csv", key="csv_raw_data"
        )

        if not raw_data_json:
            raise ValueError("No CSV data found in XCom from extract task")
        logger.info("✓ Raw data retrieved")

        df = pd.read_json(StringIO(raw_data_json))
        logger.info(f"📊 Loaded {len(df)} rows across {df['region'].nunique()} regions")

        logger.info("📦 Grouping by region and aggregating")
        summary = summarize_by_region(df)

        region_count = len(summary)
        logger.info(f"✓ Produced {region_count} region summary rows")
        for _, row in summary.iterrows():
            logger.info(
                f"   • {row['region']}: qty={row['total_quantity']}, "
                f"revenue=${row['total_revenue']:,.2f}, "
                f"avg_price=${row['avg_unit_price']:,.2f}, "
                f"orders={row['num_orders']}"
            )

        summary_json = summary.to_json(orient="records")
        task_instance.xcom_push(key="region_summary", value=summary_json)
        logger.info("✓ Region summary pushed to XCom")

        logger.info(f"✅ TRANSFORM TASK COMPLETED - {region_count} regions")
        logger.info("=" * 60)
        return region_count

    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ TRANSFORM (AGGREGATE BY REGION) TASK FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        raise
