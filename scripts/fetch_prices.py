"""
fetch_prices.py — Fetch and store raw price data for detected press releases.

Pipeline:
  1. Read data/ex_99_classified.csv (is_pr=True rows only)
  2. For each unique CIK: resolve ticker via SEC submissions API
  3. For each unique (ticker, date): 3 sequential Polygon calls —
       a. 1-min OHLCV bars  → data/price_bars.csv
       b. Daily bars (40 calendar days prior) → data/daily_bars.csv
       c. Ticker details (market cap, shares, exchange as of that date) → data/ticker_details.csv
  4. PR metadata row (cik, ex99_url, ticker, date_str) → data/price_data.csv (used as processed-set tracker)

No computations — raw data only. Computations happen in a separate step.

Requirements:
  Set MASSIVE_API_KEY env var (also accepted as POLYGON_API_KEY — same API, rebranded).

Rate limits:
  Massive free tier: 5 calls/min  →  MASSIVE_INTERVAL = 12.1s between calls
  Paid tier: 100 calls/min → set MASSIVE_INTERVAL = 0.62
"""
import ast
import asyncio
import os
import time
from datetime import datetime, timedelta

import httpx
import pandas as pd
from edgar import fetch_ticker, load_cik_cache, save_cik_cache

# ── Config ────────────────────────────────────────────────────────────────────

MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY")
POLYGON_BASE = "https://api.polygon.io"
MASSIVE_INTERVAL = 12.1      # seconds between Polygon calls (free tier: 5/min)

INPUT_CSV          = "data/ex_99_classified.csv"
OUTPUT_CSV         = "data/price_data.csv"       # computed price changes per PR
OUTPUT_BARS_CSV    = "data/price_bars.csv"        # raw 1-min OHLCV bars
OUTPUT_DAILY_CSV   = "data/daily_bars.csv"        # raw daily OHLCV bars (pre-PR window)
OUTPUT_DETAILS_CSV = "data/ticker_details.csv"    # ticker details as of filing date

# T+N offsets in milliseconds
_OFFSETS_MS = {
    "5m":  5  * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}


# ── Polygon fetch helpers ─────────────────────────────────────────────────────

async def fetch_1min_bars(client: httpx.AsyncClient, ticker: str, date_str: str) -> list:
    """Fetch 1-min OHLCV bars for the filing day (+ next day to cover after-hours)."""
    to_date = (
        datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")
    url = (
        f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/minute"
        f"/{date_str}/{to_date}"
        f"?adjusted=true&sort=asc&limit=50000&apiKey={MASSIVE_API_KEY}"
    )
    try:
        r = await client.get(url, timeout=30)
        if r.status_code == 200:
            return r.json().get("results", [])
        print(f"  Polygon HTTP {r.status_code} for {ticker} 1min on {date_str}", flush=True)
    except Exception as exc:
        print(f"  Polygon error {ticker} 1min: {exc}", flush=True)
    return []


async def fetch_daily_bars(client: httpx.AsyncClient, ticker: str, date_str: str) -> list:
    """Fetch daily OHLCV bars for the 40 calendar days ending the day before date_str."""
    end = datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)
    start = end - timedelta(days=40)
    url = (
        f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day"
        f"/{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}"
        f"?adjusted=true&sort=asc&limit=50&apiKey={MASSIVE_API_KEY}"
    )
    try:
        r = await client.get(url, timeout=30)
        if r.status_code == 200:
            return r.json().get("results", [])
        print(f"  Polygon HTTP {r.status_code} for {ticker} daily", flush=True)
    except Exception as exc:
        print(f"  Polygon error {ticker} daily: {exc}", flush=True)
    return []


async def fetch_ticker_details(client: httpx.AsyncClient, ticker: str, date_str: str) -> dict:
    """Fetch ticker reference data (market cap, shares, exchange) as of date_str."""
    url = (
        f"{POLYGON_BASE}/v3/reference/tickers/{ticker}"
        f"?date={date_str}&apiKey={MASSIVE_API_KEY}"
    )
    try:
        r = await client.get(url, timeout=30)
        if r.status_code == 200:
            return r.json().get("results", {})
        print(f"  Polygon HTTP {r.status_code} for {ticker} details", flush=True)
    except Exception as exc:
        print(f"  Polygon error {ticker} details: {exc}", flush=True)
    return {}


# ── Price change computation ──────────────────────────────────────────────────

def _bar_at_or_after(bars: list, ts_ms: int) -> dict | None:
    for bar in bars:
        if bar["t"] >= ts_ms:
            return bar
    return None


def compute_changes(bars: list, acceptance_dt: str | None) -> dict:
    result: dict = {
        "price_t0": None,
        **{f"change_{label}_pct": None for label in _OFFSETS_MS},
    }
    if not bars or not acceptance_dt:
        return result
    try:
        dt = datetime.fromisoformat(acceptance_dt.replace("Z", "+00:00"))
    except ValueError:
        return result
    t0_ms  = int(dt.timestamp() * 1000)
    t0_bar = _bar_at_or_after(bars, t0_ms)
    if t0_bar is None:
        return result
    p0 = t0_bar["c"]
    result["price_t0"] = p0
    for label, offset_ms in _OFFSETS_MS.items():
        bar = _bar_at_or_after(bars, t0_ms + offset_ms)
        if bar:
            result[f"change_{label}_pct"] = round((bar["c"] - p0) / p0 * 100, 4)
    return result


# ── Utilities ─────────────────────────────────────────────────────────────────

def _normalize_date(d) -> str:
    """Convert YYYYMMDD (int or str) to YYYY-MM-DD."""
    s = str(d)[:8]
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


def _flatten_details(ticker: str, date_str: str, d: dict) -> dict:
    """Pick the fields we want from a Polygon ticker details response."""
    return {
        "ticker":                         ticker,
        "date_str":                       date_str,
        "name":                           d.get("name"),
        "market_cap":                     d.get("market_cap"),
        "weighted_shares_outstanding":    d.get("weighted_shares_outstanding"),
        "share_class_shares_outstanding": d.get("share_class_shares_outstanding"),
        "primary_exchange":               d.get("primary_exchange"),
        "sic_description":                d.get("sic_description"),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    if not MASSIVE_API_KEY:
        raise RuntimeError(
            "Missing API key. Set MASSIVE_API_KEY or POLYGON_API_KEY environment variable."
        )

    pr_df = pd.read_csv(INPUT_CSV)
    pr_df = pr_df[pr_df["is_pr"] == True].reset_index(drop=True)

    _TARGET_CATALYSTS = {"m&a", "clinical", "private_placement", "new_product"}
    def _is_target(val):
        try: tags = ast.literal_eval(val)
        except Exception: tags = [val]
        return bool(set(tags) & _TARGET_CATALYSTS)
    pr_df = pr_df[pr_df["catalyst"].apply(_is_target)].reset_index(drop=True)
    print(f"Loaded {len(pr_df)} PR rows from {INPUT_CSV} (catalyst filter: {_TARGET_CATALYSTS})")

    # ── Step 1: SEC submissions — CIK → ticker (cached) ──────────────────────
    unique_ciks = pr_df["cik"].unique()
    cik_ticker: dict = load_cik_cache()

    new_ciks = [cik for cik in unique_ciks if str(cik) not in cik_ticker]
    print(f"\n{len(unique_ciks)} unique CIKs in batch — {len(cik_ticker)} in cache total, {len(new_ciks)} to resolve...")

    if new_ciks:
        async with httpx.AsyncClient(timeout=20) as sec_client:
            for cik in new_ciks:
                ticker = await fetch_ticker(sec_client, int(cik))
                cik_ticker[str(cik)] = ticker
                print(f"  CIK {cik} → {ticker or 'no ticker'}", flush=True)
                await asyncio.sleep(0.15)
        save_cik_cache(cik_ticker)

    # ── Step 2: Annotate rows ─────────────────────────────────────────────────
    pr_df = pr_df.copy()
    pr_df["ticker"]   = pr_df["cik"].astype(str).map(cik_ticker)
    pr_df["date_str"] = pr_df["date_filed"].apply(_normalize_date)

    no_ticker  = pr_df["ticker"].isna().sum()
    no_acc_dt  = pr_df["acceptance_dt"].isna().sum()
    print(f"\n{no_ticker}/{len(pr_df)} rows missing ticker (will skip price fetch)")
    print(f"{no_acc_dt}/{len(pr_df)} rows missing acceptance_dt")

    # ── Step 3: Fetch — 3 Polygon calls per unique (ticker, date) ────────────
    # Build processed set from existing price_data for O(1) skip check
    if os.path.exists(OUTPUT_CSV):
        _existing = pd.read_csv(OUTPUT_CSV, usecols=["cik", "ex99_url"])
        fetched: set = set(zip(_existing["cik"], _existing["ex99_url"]))
        print(f"  {len(fetched)} rows already processed — skipping")
    else:
        fetched: set = set()

    bars_cache: dict    = {}   # (ticker, date_str) → list[dict]
    daily_cache: dict   = {}   # (ticker, date_str) → list[dict]
    details_cache: dict = {}   # (ticker, date_str) → dict

    rows_out        = []   # → price_data.csv
    intraday_rows   = []   # → price_bars.csv
    daily_rows      = []   # → daily_bars.csv
    details_rows    = []   # → ticker_details.csv

    total_written   = {OUTPUT_CSV: 0, OUTPUT_BARS_CSV: 0, OUTPUT_DAILY_CSV: 0, OUTPUT_DETAILS_CSV: 0}
    write_header    = {p: not os.path.exists(p) for p in total_written}
    api_calls       = 0

    def _flush():
        pairs = [
            (rows_out,      OUTPUT_CSV),
            (intraday_rows, OUTPUT_BARS_CSV),
            (daily_rows,    OUTPUT_DAILY_CSV),
            (details_rows,  OUTPUT_DETAILS_CSV),
        ]
        for buf, path in pairs:
            if buf:
                pd.DataFrame(buf).to_csv(path, mode="a", header=write_header[path], index=False)
                write_header[path] = False
                total_written[path] += len(buf)
                buf.clear()

    async def _poly_get(coro, label: str):
        nonlocal api_calls
        api_calls += 1
        t = time.monotonic()
        result = await coro
        elapsed = time.monotonic() - t
        print(f"  [call {api_calls}] {label}  {elapsed:.2f}s", flush=True)
        remaining = MASSIVE_INTERVAL - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)
        return result

    try:
        async with httpx.AsyncClient(timeout=30) as poly_client:
            for _, row in pr_df.iterrows():
                ticker    = row["ticker"]
                date_str  = row["date_str"]
                base      = row.to_dict()

                if (row["cik"], row["ex99_url"]) in fetched:
                    continue

                if pd.isna(ticker) or not ticker:
                    rows_out.append({
                        **base,
                        "price_t0": None,
                        **{f"change_{l}_pct": None for l in _OFFSETS_MS},
                    })
                    continue

               

                cache_key = (ticker, date_str)

                if cache_key not in bars_cache:
                    print(f"\n  {ticker}  {date_str}", flush=True)

                    # Call 1 — 1-min intraday bars
                    bars = await _poly_get(
                        fetch_1min_bars(poly_client, ticker, date_str),
                        f"{ticker} 1min",
                    )
                    bars_cache[cache_key] = bars
                    for bar in bars:
                        intraday_rows.append({"ticker": ticker, "date_str": date_str, **bar})

                    # Call 2 — daily bars (pre-PR window)
                    daily = await _poly_get(
                        fetch_daily_bars(poly_client, ticker, date_str),
                        f"{ticker} daily",
                    )
                    daily_cache[cache_key] = daily
                    for bar in daily:
                        daily_rows.append({"ticker": ticker, "date_str": date_str, **bar})

                    # Call 3 — ticker details as of filing date
                    details = await _poly_get(
                        fetch_ticker_details(poly_client, ticker, date_str),
                        f"{ticker} details",
                    )
                    details_cache[cache_key] = details
                    if details:
                        details_rows.append(_flatten_details(ticker, date_str, details))

                    _flush()
                else:
                    bars = bars_cache[cache_key]

                changes = compute_changes(bars, row.get("acceptance_dt"))
                rows_out.append({**base, **changes})

    except (KeyboardInterrupt, Exception) as exc:
        print(f"\nInterrupted ({exc.__class__.__name__}) — saving progress...", flush=True)
        _flush()

    _flush()
    print(f"\nDone. API calls: {api_calls}")
    for path, n in total_written.items():
        print(f"  {n:>6} rows → {path}")


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
