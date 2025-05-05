from typing import Optional

import polars as pl
from polars import LazyFrame
from polars.io import parquet

from src.config.settings import ROLLING_WINDOW_SIZE, CORRELATION_CACHE_PATH, CORRELATION_SCHEMA


class CorrelationEngine:
    def __init__(self,returns: LazyFrame, window_size: int = ROLLING_WINDOW_SIZE):
        self.returns = returns
        self.window_size = window_size
        self.cache_dir = CORRELATION_CACHE_PATH
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_correlations(self, tickers: set[str]) -> LazyFrame:
        """
        Computes the pairwise correlations for the given list of tickers
        :param tickers: List of ticker symbols to compute correlations for.
        :return: A Polars DataFrame containing the pairwise correlations.
        """
        if len(tickers) < 2:
            raise ValueError("At least two tickers are required to compute correlations.")
        try:
            return pl.concat(
                self.get_lazy_correlations(tickers),
                parallel=True
            )
        except pl.exceptions.PolarsError as e:
            raise RuntimeError(f"An error occurred while computing correlations: {e}")

    def get_lazy_correlations(self, unique_tickers: set[str]) -> list[LazyFrame]:
        """
        Generates lazy frames for all unique combinations of ticker pairs. Each pair's correlation
        is represented as a Polars LazyFrame.
        :param unique_tickers: A list of ticker symbols to compute pairwise correlations for.
        :return: A list of Polars LazyFrames, each containing the correlation for a ticker pair.
        """
        results = [
            self.get_correlation_for_pair(ticker1, ticker2)
            for i, ticker1 in enumerate(unique_tickers)
            for j, ticker2 in enumerate(unique_tickers)
            if i < j
        ]
        return results

    def get_correlation_for_pair(self, ticker1: str, ticker2: str) -> LazyFrame:
        """
        First attempts to retrieve the correlation from the Paquret cache. If the correlation
        is not cached, it calculates the correlation, stores it in the cache, and returns the result as a LazyFrame.
        :param ticker1: The first ticker symbol.
        :param ticker2: The second ticker symbol.
        :return: A Polars LazyFrame containing the correlation for the ticker pair.
        """
        correlation_name = '-'.join(sorted([ticker1, ticker2]))
        cache_path = self.cache_dir / f"{correlation_name}.parquet"
        cached_result = self.get_correlation_from_cache(correlation_name)
        # Attempt to retrieve from cache
        if cached_result is not None:
            return cached_result
        else:
            # Calculate correlation if not in cache
            result = self.calculate_correlation(ticker1, ticker2)
            result.collect().write_parquet(cache_path)
            return result

    def get_correlation_from_cache(self, correlation_name: str) -> Optional[LazyFrame]:
        """
        Checks if a cached parquet file exists for the specified ticker pair. If the file
        exists and has a valid schema, it returns the cached result. Otherwise, it returns None.
        :param correlation_name: The name of the ticker pair (e.g., "AAPL-MSFT").
        :return: A Polars LazyFrame containing the cached correlation, or None if not found.
        """
        cache_path = self.cache_dir / f"{correlation_name}.parquet"
        if cache_path.exists():
            cached_result = parquet.scan_parquet(cache_path)
            if self._validate_schema(cached_result):
                return cached_result
        return None

    def calculate_correlation(self, ticker1: str, ticker2: str) -> LazyFrame:
        """
        Calculates the rolling correlation between the two tickers over the specified
        window size and returns the result as a Polars LazyFrame.
        :param ticker1: The first ticker symbol.
        :param ticker2: The second ticker symbol.
        :return: A Polars LazyFrame containing the rolling correlation for the ticker pair.
        """
        return self.returns.with_columns(
            Correlation=pl.rolling_corr(ticker1, ticker2, window_size = self.window_size)
        ).select(CORRELATION_SCHEMA.keys())

    def _validate_schema(self, lf: LazyFrame) -> bool:
        """
        Validates the schema of the LazyFrame to ensure it contains the required columns.
        :param lf: The LazyFrame to validate.
        :return: True if the schema is valid, False otherwise.
        """
        expected_schema = CORRELATION_SCHEMA
        actual_schema = lf.collect_schema()

        return all(
            column in actual_schema and actual_schema[column] == dtype
            for column, dtype in expected_schema.items()
        )

