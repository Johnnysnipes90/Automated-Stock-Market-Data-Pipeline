# src/transform.py
def feature_engineer(df):
    df = df.copy()
    df["return_1d"] = df["close"].pct_change()
    df["ma_7"] = df["close"].rolling(7).mean()
    df["ma_30"] = df["close"].rolling(30).mean()
    return df