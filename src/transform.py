import pandas as pd
from datetime import datetime

def transform(df):
    # Drop duplicate symbol column if exists
    if "symbol" in df.columns:
        df = df.drop(columns=["symbol"])

    df = df.rename(columns={
        "01. symbol": "symbol",
        "02. open": "open",
        "03. high": "high",
        "04. low": "low",
        "05. price": "price",
        "06. volume": "volume",
        "07. latest trading day": "trading_day",
        "08. previous close": "prev_close",
        "09. change": "change",
        "10. change percent": "change_pct"
    })

    numeric_cols = ["open", "high", "low", "price", "volume", "prev_close", "change"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["change_pct"] = df["change_pct"].str.replace("%", "").astype(float)
    df["extracted_at"] = datetime.utcnow().isoformat()

    df = df[["symbol", "price", "open", "high", "low", "volume",
             "prev_close", "change", "change_pct", "trading_day", "extracted_at"]]

    print(f"Transformed {len(df)} records")
    return df

if __name__ == "__main__":
    df_raw = pd.read_csv("data/raw_stocks.csv")
    df_clean = transform(df_raw)
    print(df_clean)

