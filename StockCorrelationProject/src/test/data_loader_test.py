import datetime
import time

import polars as pl
import pytest

from src.config.settings import ZIP_FILE_PATH
from src.main.data_loader import load_from_zip, parse_date


@pytest.fixture
def mock_zip_file_path(tmp_path):
    # Create a temporary zip file for testing
    zip_file = ZIP_FILE_PATH
    return zip_file

@pytest.fixture
def sample_lazyframe():
    # Create a sample LazyFrame with various date formats
    data = {
        "Date": [
            "01/31/2023",  # MM/DD/YYYY
            "31/01/2023",  # DD/MM/YYYY
            "2023/01/31",  # YYYY/MM/DD
            "01-31-2023",  # MM-DD-YYYY
            "31-01-2023",  # DD-MM-YYYY
            "2023-01-31",  # YYYY-MM-DD
        ]
    }
    return pl.LazyFrame(data)


def test_load_from_zip_invalid_zip_file():
    with pytest.raises(FileNotFoundError, match="Zip file not found:"):
        load_from_zip("non_existent.zip")


def test_load_from_zip_success(mock_zip_file_path):
    result = load_from_zip(mock_zip_file_path)
    assert isinstance(result, pl.LazyFrame)
    assert result.collect_schema() == pl.Schema({'Ticker': pl.String, 'Date': pl.Date, 'Price': pl.Float64})

def test_load_from_zip_time(mock_zip_file_path):
    start = time.perf_counter()
    load_from_zip(mock_zip_file_path).collect()
    duration = time.perf_counter() - start
    assert duration < 3

def test_parse_date(sample_lazyframe):
    result = parse_date(sample_lazyframe).collect()
    expected_dates = [
        datetime.date(2023, 1, 31),
        datetime.date(2023, 1, 31),
        datetime.date(2023, 1, 31),
        datetime.date(2023, 1, 31),
        datetime.date(2023, 1, 31),
        datetime.date(2023, 1, 31),
    ]
    assert result["Date"].to_list() == expected_dates


def test_parse_date_invalid_format():
    data = {"Date": ["invalid_date", "2023/31/01"]}
    lf = pl.LazyFrame(data)
    result = parse_date(lf).collect()
    assert result["Date"].to_list() == [None, None]
