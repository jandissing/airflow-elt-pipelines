"""Unit tests for the CSV pipeline's region aggregation."""

import pandas as pd

from elt.transform_csv import summarize_by_region


def _sales(rows):
    return pd.DataFrame(rows, columns=["region", "quantity", "unit_price"])


def test_sums_quantity_and_revenue_per_region():
    df = _sales(
        [
            ("South", 2, 10.0),
            ("South", 3, 20.0),
            ("North", 1, 5.0),
        ]
    )

    summary = summarize_by_region(df).set_index("region")

    assert summary.loc["South", "total_quantity"] == 5
    assert summary.loc["South", "total_revenue"] == 80.0
    assert summary.loc["North", "total_revenue"] == 5.0


def test_counts_source_rows_as_orders():
    df = _sales([("South", 1, 1.0), ("South", 1, 1.0), ("North", 1, 1.0)])

    summary = summarize_by_region(df).set_index("region")

    assert summary.loc["South", "num_orders"] == 2
    assert summary.loc["North", "num_orders"] == 1


def test_averages_unit_price_per_region():
    df = _sales([("West", 1, 10.0), ("West", 4, 15.0)])

    summary = summarize_by_region(df).set_index("region")

    assert summary.loc["West", "avg_unit_price"] == 12.5


def test_rounds_money_columns_to_cents():
    df = _sales([("West", 3, 3.333)])

    summary = summarize_by_region(df).set_index("region")

    assert summary.loc["West", "total_revenue"] == 10.0


def test_returns_one_row_per_region():
    df = _sales([("A", 1, 1.0), ("B", 1, 1.0), ("A", 1, 1.0)])

    assert len(summarize_by_region(df)) == 2


def test_does_not_mutate_the_input_frame():
    df = _sales([("South", 2, 10.0)])
    original_columns = list(df.columns)

    summarize_by_region(df)

    assert list(df.columns) == original_columns
