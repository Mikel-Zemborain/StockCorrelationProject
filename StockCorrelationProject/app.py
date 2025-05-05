import matplotlib.pyplot as plt
import streamlit as st

from src.main.correlation_engine import CorrelationEngine
from src.main.returns_engine import ReturnEngine


def main(correlation_engine: CorrelationEngine, ticker_list: list[str]):
    st.title("Stock Correlation Calculator")
    calculate = st.button("Calculate")
    selected_tickers = set(st.multiselect("Select Stocks", ticker_list))

    if calculate and selected_tickers:
        correlations = correlation_engine.get_correlations(set(selected_tickers)).collect()
        # Plot the rolling correlation
        plt.figure(figsize=(40, 10))  # Increased figure size
        for name, group in correlations.group_by("Name"):
            plt.plot(group["Date"], group["Correlation"], label=name)

        plt.title("Rolling 20-Day Correlation of Selected Tickers", fontsize=20)
        plt.xlabel("Date", fontsize=30)
        plt.ylabel("Correlation", fontsize=30)
        plt.legend(fontsize=30)
        plt.grid()
        st.pyplot(plt)

if __name__ == "__main__":
    if "correlation_engine" not in st.session_state:
        returns_engine = st.session_state.get("returns_engine", ReturnEngine())
        st.session_state["correlation_engine"] = CorrelationEngine(returns=returns_engine.returns)
    correlation_engine = st.session_state["correlation_engine"]
    ticker_list = correlation_engine.returns.columns[1:]  # Exclude "Dat
    main(correlation_engine, ticker_list)