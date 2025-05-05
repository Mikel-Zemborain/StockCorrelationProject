import os

import pytest
import polars as pl
from polars import LazyFrame
from unittest.mock import patch, MagicMock

from src.config.settings import ZIP_FILE_PATH
from src.main.returns_engine import ReturnEngine


@pytest.fixture
def mock_cache_path(tmp_path):
    return tmp_path / "returns.parquet"


@pytest.fixture
def mock_valid_lazyframe():
    return pl.DataFrame({
        "Ticker": ["AAPL", "AAPL", "AAPL"],
        "Date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "Return": [0.01, 0.02, -0.01]
    }).lazy()

@pytest.fixture
def mock_valid_price_lazyframe():
    return pl.DataFrame({
        "Ticker": ["AAPL", "AAPL", "AAPL"],
        "Date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "Price": [150.0, 152.0, 148.0]
    }).lazy()

@pytest.fixture
def mock_invalid_lazyframe():
    return pl.DataFrame({"InvalidColumn": [1]}).lazy()


def test_get_returns_from_cache_valid_schema(mock_cache_path, mock_valid_lazyframe):
    with patch("src.main.returns_engine.parquet.scan_parquet", return_value=mock_valid_lazyframe), \
         patch("src.main.returns_engine.ReturnEngine._validate_schema", return_value=True), \
         patch("pathlib.Path.exists", return_value=True):
        engine = ReturnEngine()
        engine.cache_dir = mock_cache_path.parent
        result = engine.get_returns()
        assert isinstance(result, LazyFrame)

def test_get_returns_no_cache(mock_cache_path, mock_valid_lazyframe):
    with patch("pathlib.Path.exists", return_value=False):
        engine = ReturnEngine()
        engine.cache_dir = mock_cache_path.parent
        result = engine.get_returns()
        assert isinstance(result, LazyFrame)

def test_get_returns_runtime_error(mock_cache_path):
    with patch("pathlib.Path.exists", return_value=False), \
         patch("src.main.returns_engine.load_from_zip", side_effect=Exception("Unexpected error")):
        # Mock the initialization of ReturnEngine to avoid dependency issues
        with patch("src.main.returns_engine.ReturnEngine.__init__", return_value=None):
            engine = ReturnEngine()
            engine.cache_dir = mock_cache_path.parent
            engine.zip_file_path = "/mock/path/to/zip_file.zip"  # Mock the zip file path
            with pytest.raises(RuntimeError, match="error occurred in ReturnEngine: Unexpected error"):
                engine.get_returns()