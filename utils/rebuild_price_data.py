"""
rebuild_price_data.py — Rebuild price_data.csv from existing bars without any API calls.

Use this when price_data.csv has a schema mismatch or duplicate rows from mixed runs.
Reads ex_99_classified.csv + price_bars.csv, recomputes price changes, writes clean CSV.
"""
import pandas as pd
from scripts.fetch_market_data import (
    _TARGET_CATALYSTS, _is_target, _normalize_date, _PRICE_COLS,
    _OFFSETS_MS, compute_changes, _price_row,
)
from scripts.edgar import load_cik_cache

INPUT_CSV   = "data/ex_99_classified.csv"
BARS_CSV    = "data/price_bars.csv"
OUTPUT_CSV  = "data/price_data.csv"


def main():
    # Load signal PRs
    pr_df = pd.read_csv(INPUT_CSV)
    pr_df = pr_df[pr_df["is_pr"] == True].reset_index(drop=True)
    pr_df = pr_df[pr_df["catalyst"].apply(_is_target)].reset_index(drop=True)
    print(f"Signal PRs: {len(pr_df)}")

    # Resolve tickers from cache
    cik_ticker = load_cik_cache()
    pr_df = pr_df.copy()
    pr_df["ticker"]   = pr_df["cik"].astype(str).map(cik_ticker)
    pr_df["date_str"] = pr_df["date_filed"].apply(_normalize_date)

    # Load bars into memory keyed by (ticker, date_str)
    print(f"Loading bars from {BARS_CSV}...")
    bars_df = pd.read_csv(BARS_CSV, on_bad_lines='skip')
    bars_map: dict = {}
    for key, grp in bars_df.groupby(["ticker", "date_str"]):
        bars_map[key] = grp[["t", "o", "h", "l", "c", "v"]].to_dict("records")
    print(f"  {len(bars_map)} (ticker, date_str) pairs in bars")

    rows = []
    no_bars = 0
    for _, row in pr_df.iterrows():
        ticker   = row["ticker"]
        date_str = row["date_str"]

        if pd.isna(ticker) or not ticker:
            rows.append(_price_row(row, ticker, date_str, {}))
            continue

        bars = bars_map.get((ticker, date_str), [])
        if not bars:
            no_bars += 1

        changes = compute_changes(bars, row.get("acceptance_dt"))
        rows.append(_price_row(row, ticker, date_str, changes))

    out = pd.DataFrame(rows)
    out.to_csv(OUTPUT_CSV, index=False)
    print(f"\nWrote {len(out)} rows to {OUTPUT_CSV}")
    print(f"  With price_t0:    {out['price_t0'].notna().sum()}")
    print(f"  Missing bars:     {no_bars}")
    print(f"  Missing ticker:   {out['ticker'].isna().sum()}")


if __name__ == "__main__":
    main()
