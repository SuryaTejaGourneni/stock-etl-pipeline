import sqlite3
import pandas as pd
from src.config import DB_PATH

def load(df):
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("stock_prices", conn, if_exists="append", index=False)
    conn.close()
    print(f"Loaded {len(df)} records into {DB_PATH}")

if __name__ == "__main__":
    df = pd.read_csv("data/raw_stocks.csv")
    load(df)

