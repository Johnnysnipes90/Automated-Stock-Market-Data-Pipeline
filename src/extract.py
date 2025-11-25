# src/extract.py
import requests, os, time, pandas as pd
from datetime import datetime

API_KEY = os.getenv("ALPHA_VANTAGE_KEY", "YOUR_KEY")

def fetch_daily(symbol):
    url = "https://www.alphavantage.co/query"
    params = {"function":"TIME_SERIES_DAILY_ADJUSTED","symbol":symbol,"apikey":API_KEY,"outputsize":"compact"}
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    ts = data.get("Time Series (Daily)", {})
    rows = []
    for date, vals in ts.items():
        rows.append({
            "symbol": symbol,
            "date": pd.to_datetime(date),
            "open": float(vals["1. open"]),
            "high": float(vals["2. high"]),
            "low": float(vals["3. low"]),
            "close": float(vals["4. close"]),
            "adjusted_close": float(vals["5. adjusted close"]),
            "volume": int(vals["6. volume"])
        })
    df = pd.DataFrame(rows).sort_values("date")
    return df

if __name__ == "__main__":
    print(fetch_daily("AAPL").tail())