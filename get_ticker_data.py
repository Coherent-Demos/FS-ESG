import yfinance as yf
import streamlit as st

@st.cache_data
def get_historical_data(ticker):
    """Getting historical data for speified ticker and caching it with streamlit app."""
    return yf.download(ticker, start="2019-01-01", end="2023-01-01", group_by="ticker") 