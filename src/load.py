# src/load.py
from sqlalchemy import create_engine
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/stockdb")

def write_to_db(df, table_name="stock_prices"):
    engine = create_engine(DATABASE_URL)
    df.to_sql(table_name, engine, if_exists="append", index=False)