import os
from zipfile import ZipFile, BadZipFile, Path

import polars as pl
from polars import LazyFrame

from src.config.settings import CSV_SCHEMA


def load_from_zip(zip_file_path: str) -> LazyFrame:
    """
    Uses ZipFile and Polars to efficiently read CSV files within the zip file without extracting them.
    :return: A Polars LazyFrame with the following structure:
        - Ticker (str): The stock ticker symbol.
        - Date (date): The date of the record.
        - Price (float): The stock price on the given date.
    :raises FileNotFoundError: If the zip file does not exist.
    :raises BadZipFile: If the zip file is invalid or corrupted.
    """
    if not os.path.exists(zip_file_path):
        raise FileNotFoundError(f"Zip file not found: {zip_file_path}")
    try:
        with ZipFile(zip_file_path) as zip_file:
            return (
                pl.concat(
                    (read_file(zip_file, file) for file in zip_file.namelist() if file.endswith('.csv')),
                    parallel=True
                )
                .pipe(parse_date)
            )
    except BadZipFile:
        raise BadZipFile(f"Invalid or corrupted zip file: {zip_file_path}")


def read_file(zip_file: ZipFile, file: str) -> LazyFrame:
    """
    Reads a CSV file from within a zip archive and returns a Polars LazyFrame.
    :param zip_file: The zip archive containing the CSV file.
    :param file: The name of the CSV file to read from the zip archive.
    :return: A Polars LazyFrame containing the data from the CSV file.
    :raises ValueError: If the CSV schema does not match the expected schema.
    """
    try:
        return pl.scan_csv(
            zip_file.open(file),
            has_header=True,
            try_parse_dates=False,
            schema_overrides=CSV_SCHEMA
        )
    except Exception as e:
        raise ValueError(f"Error reading file '{file}' from zip: {e}")

def parse_date(lf: LazyFrame) -> LazyFrame:
    """
    Parses the 'Date' column in various formats and returns a LazyFrame with the parsed dates.
    :param lf: The LazyFrame containing the 'Date' column to be parsed.
    :return: A LazyFrame with the 'Date' column parsed into a date format.
    """
    return lf.with_columns(
        pl.coalesce(
            pl.col("Date").str.strptime(pl.Date, "%m/%d/%Y", strict=False),
            pl.col("Date").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            pl.col("Date").str.strptime(pl.Date, "%Y/%m/%d", strict=False),
            pl.col("Date").str.strptime(pl.Date, "%m-%d-%Y", strict=False),
            pl.col("Date").str.strptime(pl.Date, "%d-%m-%Y", strict=False),
            pl.col("Date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        ).cast(pl.Date).alias("Date")
    )
