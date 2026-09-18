"""Unit tests for the type coercion the load step applies."""

import datetime

import pandas as pd

from elt.load_sales import normalize_sale_date


def test_converts_iso_strings_to_dates():
    df = pd.DataFrame({"sale_date": ["2024-01-18T00:00:00.000"]})

    result = normalize_sale_date(df)

    assert result.loc[0, "sale_date"] == datetime.date(2024, 1, 18)


def test_converts_epoch_milliseconds_to_dates():
    df = pd.DataFrame({"sale_date": [pd.Timestamp("2024-01-18")]})

    result = normalize_sale_date(df)

    assert result.loc[0, "sale_date"] == datetime.date(2024, 1, 18)


def test_leaves_a_frame_without_the_column_untouched():
    df = pd.DataFrame({"region": ["North"]})

    result = normalize_sale_date(df)

    assert list(result.columns) == ["region"]
