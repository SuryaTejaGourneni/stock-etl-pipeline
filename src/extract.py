import requests
import pandas as pd
from src.config import API_KEY, SYMBOLS, BASE_URL
import time

def fetch_stock_data(symbol):
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    if "Global Quote" in data and data["Global Quote"]:
        quote = data["Global Quote"]
        quote["symbol"] = symbol
        return quote
    print(f"  WARNING: No data for {symbol}: {data}")
    return None

def extract_all():
    all_data = []
    for symbol in SYMBOLS:
        print(f"Extracting {symbol}...")
        quote = fetch_stock_data(symbol)
        if quote:
            all_data.append(quote)
        time.sleep(12)  # free tier: 5 calls/min

    if not all_data:
        print("ERROR: No data extracted!")
        return None

    df = pd.DataFrame(all_data)
    df.to_csv("data/raw_stocks.csv", index=False)
    print(f"Extracted {len(df)} records → data/raw_stocks.csv")
    return df

if __name__ == "__main__":
    extract_all()

