import requests
import csv
from pathlib import Path

# -------------------------------
# CONFIG
# -------------------------------
API_KEY = "6020c21410d6d4fd38b2b54d57ab2444"
TICKERS = ["AAPL", "TSLA", "NVDA"]
INTERVAL = "1min"  # minute granularity
BASE_URL = "http://api.marketstack.com/v2/intraday"
OUTPUT_DIR = Path("C:/sentiment-analyzer/data/marketstack_intraday")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------
# FETCH AND SAVE CSV
# -------------------------------
for ticker in TICKERS:
    print(f"Fetching intraday data for {ticker}...")
    
    params = {
        "access_key": API_KEY,
        "symbols": ticker,
        "interval": INTERVAL
    }
    
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code != 200:
        print(f"Error {response.status_code} for {ticker}: {response.text}")
        continue
    
    data = response.json().get("data", [])
    
    if not data:
        print(f"No intraday data returned for {ticker}")
        continue
    
    # Write CSV header based on JSON keys
    csv_file = OUTPUT_DIR / f"{ticker}_intraday.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Write header row
        header = list(data[0].keys())
        writer.writerow(header)
        
        # Write rows
        for entry in data:
            writer.writerow([entry.get(col) for col in header])
    
    print(f"Saved {ticker} intraday to {csv_file}")