"""Unit tests for the sales pipeline's row-level transformation."""

import pandas as pd
import pytest

from elt.transform_sales import transform_sales_frame

RAW_COLUMNS = [
    "id",
    "customer_name",
    "product_name",
    "quantity",
    "unit_price",
    "sale_date",
    "region",
]


def _raw(rows):
    return pd.DataFrame(rows, columns=RAW_COLUMNS)


def test_computes_total_amount_from_quantity_and_unit_price():
    df = _raw([(1, "Ana", "Monitor", 3, 1200.0, "2024-01-18", "North")])

    result = transform_sales_frame(df)

    assert result.loc[0, "total_amount"] == 3600.0


def test_drops_columns_not_needed_downstream():
    df = _raw([(1, "Ana", "Monitor", 1, 10.0, "2024-01-18", "North")])

    result = transform_sales_frame(df)

    assert list(result.columns) == [
        "customer_name",
        "product_name",
        "total_amount",
        "sale_date",
        "region",
    ]


def test_keeps_every_input_row():
    df = _raw(
        [
            (1, "Ana", "Monitor", 1, 10.0, "2024-01-18", "North"),
            (2, "Bo", "Mouse", 2, 5.0, "2024-01-19", "South"),
        ]
    )

    assert len(transform_sales_frame(df)) == 2


def test_rejects_a_frame_missing_a_required_column():
    df = pd.DataFrame([{"customer_name": "Ana", "quantity": 1, "unit_price": 10.0}])

    with pytest.raises(KeyError):
        transform_sales_frame(df)


def test_does_not_mutate_the_input_frame():
    df = _raw([(1, "Ana", "Monitor", 1, 10.0, "2024-01-18", "North")])
    original_columns = list(df.columns)

    transform_sales_frame(df)

    assert list(df.columns) == original_columns
