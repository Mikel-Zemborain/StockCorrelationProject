from datetime import timedelta

import streamlit as st
import matplotlib.pyplot as plt
import polars as pl

from src.main.correlation_engine import CorrelationEngine
from src.main.returns_engine import ReturnEngine


def main():
    returns_engine = ReturnEngine("path/to/your/zipfile.zip")
    correlation_engine = CorrelationEngine(returns=returns_engine.returns)
    ticker_list = returns_engine.ticker_list()

    st.title("rolling 20-day return correlation calculator")
    min_date, max_date = returns_engine.get_date_range()

    selected_tickers = st.multiselect("Select Stocks", ticker_list)
    min_date, max_date = st.slider(
        "Select a date range",
        min_value=min_date,
        max_value=min_date,
        value=(min_date, max_date),
        step=timedelta(days=1),
    )

    if selected_tickers:
        correlations = correlation_engine.get_correlations(set(selected_tickers))
        filtered_returns = correlations.filter(
            (pl.col("Ticker").is_in(selected_tickers)) &
            (pl.col("Date") >= min_date) &
            (pl.col("Date") <= max_date)
        ).collect()

        # Plot the rolling correlation
        plt.figure(figsize=(10, 6))
        for ticker_pair, corr_series in filtered_returns.items():
            plt.plot(corr_series["Date"], corr_series["Correlation"], label=f"{ticker_pair[0]} vs {ticker_pair[1]}")

        plt.title("Rolling 20-Day Correlation of Selected Tickers")
        plt.xlabel("Date")
        plt.ylabel("Correlation")
        plt.legend()
        plt.grid()
        st.pyplot(plt)



