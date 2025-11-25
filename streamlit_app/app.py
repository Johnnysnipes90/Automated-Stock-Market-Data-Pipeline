# streamlit_app/app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

DATABASE_URL = os.getenv("DATABASE_URL","postgresql://user:pass@localhost:5432/stockdb")
engine = create_engine(DATABASE_URL)

st.title("Stock ETL Dashboard")
symbol = st.selectbox("Symbol", ["AAPL","MSFT"])
query = f"SELECT * FROM stock_prices WHERE symbol='{symbol}' ORDER BY date"
df = pd.read_sql(query, engine, parse_dates=["date"])
st.line_chart(df.set_index("date")[["close","ma_7","ma_30"]])
st.dataframe(df.tail(20))