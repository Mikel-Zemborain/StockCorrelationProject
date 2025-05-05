import datetime

import pytest
import polars as pl
from polars import LazyFrame
from src.main.correlation_engine import CorrelationEngine
from src.config.settings import CORRELATION_SCHEMA

@pytest.fixture
def sample_returns():
    data = {
        "Date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "AAPL": [100, 101, 102],
        "MSFT": [200, 202, 204],
        "GOOG": [300, 303, 306],
    }
    return pl.DataFrame(data).lazy()

@pytest.fixture
def correlation_engine(sample_returns):
    return CorrelationEngine(returns=sample_returns)

def test_get_correlations_invalid_tickers(correlation_engine):
    with pytest.raises(ValueError, match="At least two tickers are required to compute correlations."):
        correlation_engine.get_correlations({"AAPL"})

def test_get_correlations_valid_tickers(correlation_engine):
    result = correlation_engine.get_correlations({"AAPL", "MSFT"})
    assert isinstance(result, LazyFrame)

def test_get_lazy_correlations_empty_tickers(correlation_engine):
    result = correlation_engine.get_lazy_correlations(set())
    assert result == []

def test_get_lazy_correlations_valid_tickers(correlation_engine):
    result = correlation_engine.get_lazy_correlations({"AAPL", "MSFT"})
    assert len(result) == 1
    assert isinstance(result[0], LazyFrame)

def test_get_correlation_for_pair_cache_hit(mocker, correlation_engine):
    mocker.patch(
        "src.main.correlation_engine.CorrelationEngine.get_correlation_from_cache",
        return_value = pl.DataFrame({"Date": [], "Correlation": []}).lazy())
    result = correlation_engine.get_correlation_for_pair("AAPL", "MSFT")
    assert isinstance(result, LazyFrame)

def test_get_correlation_for_pair_cache_miss(mocker, correlation_engine):
    mocker.patch("src.main.correlation_engine.CorrelationEngine.get_correlation_from_cache", return_value=None)
    mocker.patch("src.main.correlation_engine.CorrelationEngine.calculate_correlation", return_value=pl.DataFrame({"Date": [], "Correlation": []}).lazy())
    result = correlation_engine.get_correlation_for_pair("AAPL", "MSFT")
    assert isinstance(result, LazyFrame)

def test_get_correlation_from_cache_valid_schema(mocker, correlation_engine):
    mocker.patch("src.main.correlation_engine.parquet.scan_parquet", return_value=pl.DataFrame({"Date": [], "Correlation": []}).lazy())
    mocker.patch("src.main.correlation_engine.CorrelationEngine._validate_schema", return_value=True)
    result = correlation_engine.get_correlation_from_cache("AAPL-MSFT")
    assert isinstance(result, LazyFrame)

def test_get_correlation_from_cache_invalid_schema(mocker, correlation_engine):
    mocker.patch("src.main.correlation_engine.parquet.scan_parquet", return_value=pl.DataFrame({"Date": [], "Correlation": []}).lazy())
    mocker.patch("src.main.correlation_engine.CorrelationEngine._validate_schema", return_value=False)
    result = correlation_engine.get_correlation_from_cache("AAPL-MSFT")
    assert result is None

def test_calculate_correlation(correlation_engine):
    result = correlation_engine.calculate_correlation("AAPL", "MSFT", "AAPL-MSFT")
    assert isinstance(result, LazyFrame)
    assert set(result.collect().columns) == set(CORRELATION_SCHEMA.keys())

def test_validate_schema_valid(correlation_engine):
    valid_lf = pl.DataFrame({"Date": [datetime.date(2025,2,1)], "Correlation": [float(100.001)]}).lazy()
    assert correlation_engine._validate_schema(valid_lf) is True

def test_validate_schema_invalid_column_type(correlation_engine, sample_returns):
    invalid_lf = pl.DataFrame({"Date": [datetime.date(2025,2,1)], "Correlation": ["100"]}).lazy()
    assert correlation_engine._validate_schema(invalid_lf) is False

def test_validate_schema_invalid_column_name(correlation_engine, sample_returns):
    invalid_lf = pl.DataFrame({"Date": [datetime.date(2025, 2, 1)], "NotACollumn": [float(100.001)]}).lazy()
    assert correlation_engine._validate_schema(invalid_lf) is False