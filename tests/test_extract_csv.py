"""Unit tests for reading the CSV input directory."""

import pytest

from elt.extract_csv import read_csv_directory

HEADER = "customer_name,product_name,quantity,unit_price,sale_date,region\n"
ROW = "Ana,Monitor,1,1200.00,2024-01-18,North\n"


def _write_csv(directory, name, body):
    path = directory / name
    path.write_text(body)
    return path


def test_concatenates_rows_from_all_files(tmp_path):
    _write_csv(tmp_path, "a.csv", HEADER + ROW)
    _write_csv(tmp_path, "b.csv", HEADER + ROW + ROW)

    df = read_csv_directory(str(tmp_path))

    assert len(df) == 3


def test_ignores_non_csv_files(tmp_path):
    _write_csv(tmp_path, "sales.csv", HEADER + ROW)
    _write_csv(tmp_path, "notes.txt", "not data")

    df = read_csv_directory(str(tmp_path))

    assert len(df) == 1


def test_raises_when_the_directory_holds_no_csv_files(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_csv_directory(str(tmp_path))


def test_names_the_missing_columns_when_the_schema_is_wrong(tmp_path):
    _write_csv(tmp_path, "bad.csv", "customer_name,region\nAna,North\n")

    with pytest.raises(ValueError) as excinfo:
        read_csv_directory(str(tmp_path))

    assert "quantity" in str(excinfo.value)
    assert "unit_price" in str(excinfo.value)
