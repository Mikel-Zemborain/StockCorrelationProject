from pathlib import Path
import polars as pl

# Path to the ZIP file containing stock data
ZIP_FILE_PATH = "data/stock_data.zip"

# Cache directory for storing correlation results
CORRELATION_CACHE_PATH = Path("cache/correlation")

# Cache directory for storing returns results
RETURNS_CACHE_PATH = Path("cache/returns")

# Required columns for the correlation DataFrame
CORRELATION_SCHEMA = {'Date': pl.String,
                      'Correlation': pl.Float64
                      }
RETURNS_SCHEMA = {'Date': pl.String,
                  'Return': pl.Float64
                  }

# Polars schema for the CSV files within the ZIP archive
CSV_SCHEMA = {'Ticker': pl.String,
              'Date': pl.String,
              'Price': pl.Float64
              }

# Rolling window size for correlation calculation
ROLLING_WINDOW_SIZE = 20

# Streamlit app settings
APP_TITLE = "Rolling 20-Day Return Correlation Calculator"
