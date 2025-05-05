import polars as pl
from polars import LazyFrame
from polars.io import parquet

from src.config.settings import ZIP_FILE_PATH, RETURNS_CACHE_PATH, RETURNS_SCHEMA
from src.main.data_loader import load_from_zip


class ReturnEngine:
    def __init__(self, zip_file_path: str = ZIP_FILE_PATH):
        self.zip_file_path = zip_file_path
        self.cache_dir = RETURNS_CACHE_PATH
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.returns = self.get_returns()

    def get_returns(self) -> LazyFrame:
        """
        Returns a Polars LazyFrame containing the calculated returns.
        :return: A Polars LazyFrame with the calculated returns.
        """
        cache_path = self.cache_dir / "returns.parquet"

        try:
            # If cached returns exist, load them
            if cache_path.exists():
                cached_result = parquet.scan_parquet(cache_path)
                if self._validate_schema(cached_result):
                    return cached_result

            # If cache does not exist or contains invalid returns, calculate returns
            result_lazy = load_from_zip(self.zip_file_path).pipe(self._calculate_returns)
            result = result_lazy.collect().pivot(index="Date", on="Ticker", values="Return")
            result.write_parquet(cache_path)
            return result.lazy()

        except FileNotFoundError:
            raise ValueError(f"File not found: {self.zip_file_path}")
        except Exception as e:
            raise RuntimeError(f"error occurred in ReturnEngine: {e}")


    def ticker_list(self) -> list[str]:
        """
        Returns a list of unique tickers from the loaded data.
        :return: A list of unique ticker symbols.
        """
        return self.returns.select("Ticker").unique().collect().to_series().to_list()

    def _calculate_returns(self, lf: LazyFrame) -> LazyFrame:
        """
        Calculates the percentage change in the 'Price' column for each ticker and returns a LazyFrame with the calculated returns.
        :param lf: a LazyFrame to calculate returns from.
        :return: A LazyFrame with the calculated returns.
        """
        return lf.sort(["Ticker", "Date"]).with_columns(
                Return = pl.col("Price").pct_change().over("Ticker")
            ).select(list(RETURNS_SCHEMA.keys()))

    def _validate_schema(self, lf: LazyFrame) -> bool:
        """
        Validates the schema of the LazyFrame to ensure it contains the required columns.
        :param lf: The LazyFrame to validate.
        :return: True if the schema is valid, False otherwise.
        """
        expected_schema = RETURNS_SCHEMA
        actual_schema = lf.collect_schema()

        return all(
            column in actual_schema and actual_schema[column] == dtype
            for column, dtype in expected_schema.items()
        )