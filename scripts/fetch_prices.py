"""
fetch_prices.py — Fetch 1-minute price data for detected press releases.

Pipeline:
  1. Read parsed/batch_filter_results.csv — keep is_pr=True rows
  2. For each unique CIK: resolve ticker + acceptance datetimes via SEC submissions API
  3. For each unique (ticker, date): fetch 1-min OHLCV bars from Massive (Polygon) API
  4. Compute % price changes at T+5m, T+30m, T+1h, T+4h, T+1d relative to filing time
  5. Save to parsed/price_data.csv

Requirements:
  Set MASSIVE_API_KEY env var (also accepted as POLYGON_API_KEY — same API, rebranded).

Rate limits:
  Massive free tier: 5 calls/min  →  MASSIVE_INTERVAL = 12.1s between calls
  SEC submissions: polite at ~10 req/s  →  1s pause every 10 requests
"""
import asyncio
import os
import re
import time
from datetime import datetime, timedelta

import httpx
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────

MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY")
POLYGON_BASE = "https://api.polygon.io"
MASSIVE_INTERVAL = 12.1      # seconds between Polygon calls (free tier: 5/min)

INPUT_CSV = "parsed/batch_filter_results.csv"
OUTPUT_CSV = "parsed/price_data.csv"
OUTPUT_BARS_CSV = "parsed/price_bars.csv"

SEC_HEADERS = {"User-Agent": "SentimentAnalyzer contact@example.com"}

# Matches accession number in EDGAR index URLs:
# .../0001193125-26-099334-index.html  →  0001193125-26-099334
_ACCESSION_RE = re.compile(r"/([\d]{10}-[\d]{2}-[\d]{6})-index\.html")

# T+N offsets in milliseconds
_OFFSETS_MS = {
    "5m":  5  * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}


# ── SEC helpers ───────────────────────────────────────────────────────────────

async def fetch_submissions(client: httpx.AsyncClient, cik: int):
    """
    Fetch SEC submissions JSON for a CIK.

    Returns:
        ticker (str | None): primary exchange ticker, e.g. "ABM"
        acc_map (dict): accession_number → acceptanceDateTime (ISO 8601 string)
    """
    url = f"https://data.sec.gov/submissions/CIK{cik:010d}.json"
    try:
        r = await client.get(url, headers=SEC_HEADERS)
        if r.status_code != 200:
            return None, {}
        data = r.json()
    except Exception:
        return None, {}

    tickers = data.get("tickers", [])
    ticker = tickers[0] if tickers else None

    recent = data.get("filings", {}).get("recent", {})
    acc_nums = recent.get("accessionNumber", [])
    acc_dts = recent.get("acceptanceDateTime", [])
    acc_map = dict(zip(acc_nums, acc_dts))

    return ticker, acc_map


def _extract_accession(index_url: str) -> str | None:
    """Extract accession number string from EDGAR index URL."""
    m = _ACCESSION_RE.search(str(index_url))
    return m.group(1) if m else None


# ── Massive / Polygon helpers ─────────────────────────────────────────────────

async def fetch_1min_bars(client: httpx.AsyncClient, ticker: str, date_str: str) -> list:
    """
    Fetch 1-minute OHLCV bars for (ticker, date_str YYYY-MM-DD).

    Requests date through date+1 to cover pre-market, regular hours, and
    after-hours on the filing day (many 8-Ks drop at 7-8am ET before open).

    Returns sorted list of bar dicts or [] on failure.
    """
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
        print(f"  Polygon HTTP {r.status_code} for {ticker} on {date_str}", flush=True)
    except Exception as exc:
        print(f"  Polygon error {ticker}: {exc}", flush=True)
    return []


# ── Price change computation ───────────────────────────────────────────────────

def _bar_at_or_after(bars: list, ts_ms: int) -> dict | None:
    """Return the first bar whose timestamp >= ts_ms, or None."""
    for bar in bars:
        if bar["t"] >= ts_ms:
            return bar
    return None


def compute_changes(bars: list, acceptance_dt: str | None) -> dict:
    """
    Given 1-min bars sorted ascending and an acceptance datetime string,
    return price at T0 and % price changes at each configured offset.

    All values are None when bars are empty or T0 bar cannot be located.
    """
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

    t0_ms = int(dt.timestamp() * 1000)
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


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    if not MASSIVE_API_KEY:
        raise RuntimeError(
            "Missing API key. Set MASSIVE_API_KEY or POLYGON_API_KEY environment variable."
        )

    pr_df = pd.read_csv(INPUT_CSV).head(2)
    print(f"Loaded {len(pr_df)} PR rows from {INPUT_CSV}")

    # ── Step 1: SEC submissions — CIK → ticker + acceptance datetimes ─────────
    unique_ciks = pr_df["cik"].unique()
    print(f"\nResolving {len(unique_ciks)} unique CIKs via SEC submissions API...")

    cik_ticker: dict = {}
    cik_acc_map: dict = {}

    async with httpx.AsyncClient(timeout=20) as sec_client:
        for cik in unique_ciks:
            ticker, acc_map = await fetch_submissions(sec_client, int(cik))
            cik_ticker[cik] = ticker
            cik_acc_map[cik] = acc_map
            print(f"  CIK {cik} → {ticker or 'no ticker'}", flush=True)
            await asyncio.sleep(0.15)  # ~6 req/s, well under SEC's 10 req/s limit

    # ── Step 2: Annotate rows ─────────────────────────────────────────────────
    pr_df = pr_df.copy()
    pr_df["ticker"] = pr_df["cik"].map(cik_ticker)
    pr_df["date_str"] = pr_df["date_filed"].apply(_normalize_date)
    pr_df["acceptance_dt"] = pr_df.apply(
        lambda row: cik_acc_map.get(row["cik"], {}).get(
            _extract_accession(row["index_url"]) or ""
        ),
        axis=1,
    )

    no_ticker = pr_df["ticker"].isna().sum()
    no_acc_dt = pr_df["acceptance_dt"].isna().sum()
    print(f"\n{no_ticker}/{len(pr_df)} rows missing ticker (will skip price fetch)")
    print(f"{no_acc_dt}/{len(pr_df)} rows missing acceptance_dt (changes will be None)")

    # ── Step 3: Fetch 1-min bars, one API call per unique (ticker, date) ──────
    # Load already-processed pairs from price_data for O(1) skip check
    if os.path.exists(OUTPUT_CSV):
        _existing = pd.read_csv(OUTPUT_CSV, usecols=["cik", "ex99_url"])
        fetched: set = set(zip(_existing["cik"], _existing["ex99_url"]))
        print(f"  {len(fetched)} rows already in price_data — skipping")
    else:
        fetched: set = set()

    bars_cache: dict = {}
    rows_out = []
    all_bars_rows = []

    write_bars_header = not os.path.exists(OUTPUT_BARS_CSV)
    write_data_header = not os.path.exists(OUTPUT_CSV)

    def _flush():
        nonlocal write_bars_header, write_data_header
        if rows_out:
            pd.DataFrame(rows_out).to_csv(
                OUTPUT_CSV, mode="a", header=write_data_header, index=False,
            )
            write_data_header = False
            rows_out.clear()
        if all_bars_rows:
            pd.concat(all_bars_rows).to_csv(
                OUTPUT_BARS_CSV, mode="a", header=write_bars_header, index=False,
            )
            write_bars_header = False
            all_bars_rows.clear()

    try:
        async with httpx.AsyncClient(timeout=30) as poly_client:
            for _, row in pr_df.iterrows():
                ticker = row["ticker"]
                base = row.to_dict()

                if pd.isna(ticker) or not ticker:
                    rows_out.append({
                        **base,
                        "price_t0": None,
                        **{f"change_{l}_pct": None for l in _OFFSETS_MS},
                    })
                    continue

                cache_key = (ticker, row["date_str"])

                if (row["cik"], row["ex99_url"]) in fetched:
                    continue  # already in price_data, skip entirely
                elif cache_key not in bars_cache:
                    call_num = len(bars_cache) + 1
                    print(f"  [{call_num}] Fetching {ticker} on {row['date_str']}...", flush=True)
                    t_start = time.monotonic()
                    bars = await fetch_1min_bars(poly_client, ticker, row["date_str"])
                    t_network = time.monotonic() - t_start
                    bars_cache[cache_key] = bars
                    if bars:
                        bars_df = pd.DataFrame(bars)
                        bars_df["ticker"] = ticker
                        bars_df["date_str"] = row["date_str"]
                        all_bars_rows.append(bars_df)
                    t_processing = time.monotonic() - t_start - t_network
                    _flush()
                    t_flush = time.monotonic() - t_start - t_network - t_processing
                    print(
                        f"  [{call_num}] network={t_network:.2f}s  processing={t_processing:.3f}s  flush={t_flush:.3f}s  bars={len(bars)}",
                        flush=True,
                    )
                    remaining = MASSIVE_INTERVAL - (time.monotonic() - t_start)
                    if remaining > 0:
                        await asyncio.sleep(remaining)
                else:
                    bars = bars_cache[cache_key]

                changes = compute_changes(bars, row.get("acceptance_dt"))
                rows_out.append({**base, **changes})
    except (KeyboardInterrupt, Exception) as exc:
        print(f"\nInterrupted ({exc.__class__.__name__}) — saving progress...", flush=True)
        _flush()

    _flush()
    print(f"\nDone. {len(rows_out)} new rows written to {OUTPUT_CSV}")
    print(f"{sum(len(b) for b in all_bars_rows)} new bars written to {OUTPUT_BARS_CSV}")


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
