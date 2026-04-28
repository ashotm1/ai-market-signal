"""
fetch_market_data.py — Fetch and store raw market data for detected press releases.

Pipeline (EDGAR source):
  1. Read data/ex_99_classified.csv (is_pr=True rows only)
  2. For each unique CIK: resolve ticker via SEC submissions API
  3. For each unique (ticker, date): 3 sequential Polygon calls —
       a. 1-min OHLCV bars  → data/price_bars.csv
       b. Daily bars (40 calendar days prior) → data/daily_bars.csv
       c. Ticker details (market cap, shares, exchange as of that date) → data/ticker_details.csv
  4. PR metadata row → data/price_data.csv (dedup tracker)

Pipeline (StockTitan source):
  Same Polygon calls, ticker already in CSV — no CIK resolution.
  Output → data/st_price_data.csv

Dedup key: (ticker, date_str) — shared across both sources via ticker_details.csv.

Requirements:
  Set MASSIVE_API_KEY env var (also accepted as POLYGON_API_KEY — same API, rebranded).

Rate limits:
  Massive free tier: 5 calls/min  →  MASSIVE_INTERVAL = 12.1s between calls
  Paid tier: unlimited → set MASSIVE_INTERVAL = 0

Usage:
  python scripts/fetch_market_data.py                      # EDGAR source (default)
  python scripts/fetch_market_data.py --source stocktitan  # StockTitan source
"""
import argparse
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

EDGAR_INPUT_CSV    = "data/ex_99_classified.csv"
EDGAR_OUTPUT_CSV   = "data/price_data.csv"
ST_INPUT_CSV       = "data/stocktitan_news_filtered.csv"
ST_OUTPUT_CSV      = "data/st_price_data.csv"
OUTPUT_BARS_CSV    = "data/price_bars.csv"        # shared — raw 1-min OHLCV bars
OUTPUT_DAILY_CSV   = "data/daily_bars.csv"        # shared — raw daily OHLCV bars
OUTPUT_DETAILS_CSV = "data/ticker_details.csv"    # shared — dedup key lives here

# Signal-group catalysts to fetch prices for (see pr_detection.py classify_catalyst)
_TARGET_CATALYSTS = {
    "biotech", "private_placement", "collaboration",
    "m&a", "new_product", "contract", "crypto_treasury",
}


def _is_target(v) -> bool:
    try:
        tags = ast.literal_eval(v) if isinstance(v, str) else [v]
        return bool(set(tags) & _TARGET_CATALYSTS)
    except Exception:
        return False



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
    """Fetch ticker reference data (market cap, shares, exchange) as of prior trading day."""
    prior = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    url = (
        f"{POLYGON_BASE}/v3/reference/tickers/{ticker}"
        f"?date={prior}&apiKey={MASSIVE_API_KEY}"
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


def _bar_before(bars: list, ts_ms: int) -> dict | None:
    prev = None
    for bar in bars:
        if bar["t"] >= ts_ms:
            return prev
        prev = bar
    return prev


def compute_changes(bars: list, acceptance_dt: str | None, daily: list | None = None) -> dict:
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
    t0_ms   = int(dt.timestamp() * 1000)
    pre_bar = _bar_before(bars, t0_ms)
    if pre_bar is not None:
        p0 = pre_bar["c"]
    elif daily:
        p0 = daily[-1]["c"]  # prior day close fallback for pre-market/after-hours
    else:
        return result
    result["price_t0"] = p0
    for label, offset_ms in _OFFSETS_MS.items():
        bar = _bar_at_or_after(bars, t0_ms + offset_ms)
        if bar:
            result[f"change_{label}_pct"] = round((bar["c"] - p0) / p0 * 100, 4)
    return result


# ── Utilities ─────────────────────────────────────────────────────────────────

def _normalize_date(d) -> str:
    """Convert YYYYMMDD (int or str) or MM/DD/YYYY to YYYY-MM-DD."""
    s = str(d).strip()
    if "/" in s:
        return datetime.strptime(s.split()[0], "%m/%d/%Y").strftime("%Y-%m-%d")
    s = s[:8]
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


def _normalize_acceptance_dt(dt_str: str) -> str | None:
    """Convert StockTitan '04/25/2026 01:00 PM' to ISO '2026-04-25 13:00:00'."""
    try:
        return datetime.strptime(dt_str.strip(), "%m/%d/%Y %I:%M %p").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def load_edgar(catalyst: str | None) -> pd.DataFrame:
    df = pd.read_csv(EDGAR_INPUT_CSV)
    df = df[df["is_pr"] == True].reset_index(drop=True)
    catalyst_filter = {catalyst} if catalyst else _TARGET_CATALYSTS
    df = df[df["catalyst"].apply(
        lambda v: bool(set(ast.literal_eval(v) if isinstance(v, str) else [v]) & catalyst_filter)
    )].reset_index(drop=True)
    return df


_ST_SIG_TAGS = {"acquisition", "partnership", "clinical trial", "crypto", "private placement", "fda approval"}


def load_stocktitan(sig: bool = False) -> pd.DataFrame:
    df = pd.read_csv(ST_INPUT_CSV)
    df["date_str"]      = df["date"].apply(_normalize_date)
    df["acceptance_dt"] = df["datetime"].apply(_normalize_acceptance_dt)
    if sig:
        def _has_sig_tag(tags_str):
            if pd.isna(tags_str):
                return False
            tags = {t.strip().lower() for t in tags_str.split("|")}
            return bool(tags & _ST_SIG_TAGS)
        df = df[df["tags"].apply(_has_sig_tag)].reset_index(drop=True)
    return df


_EDGAR_PRICE_COLS = ["cik", "ex99_url", "company", "date_filed", "acceptance_dt"]


def _price_row(row, ticker, date_str: str, changes: dict, source: str) -> dict:
    if source == "stocktitan":
        return {
            **row.to_dict(),
            "price_t0": changes.get("price_t0"),
            **{f"change_{l}_pct": changes.get(f"change_{l}_pct") for l in _OFFSETS_MS},
        }
    return {
        **{col: row.get(col) for col in _EDGAR_PRICE_COLS},
        "ticker":   ticker,
        "date_str": date_str,
        "price_t0": changes.get("price_t0"),
        **{f"change_{l}_pct": changes.get(f"change_{l}_pct") for l in _OFFSETS_MS},
    }


def _flatten_details(ticker: str, date_str: str, d: dict) -> dict:
    """Pick the fields we want from a Polygon ticker details response."""
    return {
        "ticker":                         ticker,
        "date_str":                       date_str,
        "name":                           d.get("name"),
        "type":                           d.get("type"),
        "market_cap":                     d.get("market_cap"),
        "weighted_shares_outstanding":    d.get("weighted_shares_outstanding"),
        "share_class_shares_outstanding": d.get("share_class_shares_outstanding"),
        "primary_exchange":               d.get("primary_exchange"),
        "sic_description":                d.get("sic_description"),
        "total_employees":                d.get("total_employees"),
        "list_date":                      d.get("list_date"),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(source: str = "edgar", catalyst: str | None = None, sig: bool = False, refetch: bool = False):
    if not MASSIVE_API_KEY:
        raise RuntimeError(
            "Missing API key. Set MASSIVE_API_KEY or POLYGON_API_KEY environment variable."
        )

    OUTPUT_CSV = ST_OUTPUT_CSV if source == "stocktitan" else EDGAR_OUTPUT_CSV

    # ── Load input ────────────────────────────────────────────────────────────
    if source == "stocktitan":
        pr_df = load_stocktitan(sig=sig)
        print(f"Loaded {len(pr_df)} rows from {ST_INPUT_CSV}{' (sig filter)' if sig else ''}")
    else:
        pr_df = load_edgar(catalyst)
        print(f"Loaded {len(pr_df)} PR rows from {EDGAR_INPUT_CSV}")

    # ── CIK → ticker resolution (EDGAR only) ─────────────────────────────────
    if source == "edgar":
        unique_ciks = pr_df["cik"].unique()
        cik_ticker: dict = load_cik_cache()
        new_ciks = [cik for cik in unique_ciks if str(cik) not in cik_ticker]
        print(f"\n{len(unique_ciks)} unique CIKs — {len(new_ciks)} to resolve...")
        if new_ciks:
            async with httpx.AsyncClient(timeout=20) as sec_client:
                for cik in new_ciks:
                    ticker = await fetch_ticker(sec_client, int(cik))
                    cik_ticker[str(cik)] = ticker
                    print(f"  CIK {cik} -> {ticker or 'no ticker'}", flush=True)
                    await asyncio.sleep(0.15)
            save_cik_cache(cik_ticker)
        pr_df = pr_df.copy()
        pr_df["ticker"]   = pr_df["cik"].astype(str).map(cik_ticker)
        pr_df["date_str"] = pr_df["date_filed"].apply(_normalize_date)

    cutoff = (pd.Timestamp.today() - 2 * pd.tseries.offsets.BDay()).strftime("%Y-%m-%d")
    pr_df = pr_df[pr_df["date_str"] <= cutoff].reset_index(drop=True)
    print(f"  {len(pr_df)} rows after excluding last 2 trading days (cutoff {cutoff})")

    no_ticker = pr_df["ticker"].isna().sum()
    no_acc_dt = pr_df["acceptance_dt"].isna().sum()
    print(f"\n{no_ticker}/{len(pr_df)} rows missing ticker")
    print(f"{no_acc_dt}/{len(pr_df)} rows missing acceptance_dt")

    # ── Refetch: find incomplete rows (price_t0 not null but some changes null) ─
    change_cols = [f"change_{l}_pct" for l in _OFFSETS_MS]
    refetch_pairs: set = set()
    if refetch and os.path.exists(OUTPUT_CSV):
        _out = pd.read_csv(OUTPUT_CSV)
        _out_complete = _out["price_t0"].notna()
        _out_has_null = _out[change_cols].isnull().any(axis=1)
        _out_old_enough = _out["date_str"] <= cutoff
        _incomplete = _out[_out_complete & _out_has_null & _out_old_enough]
        refetch_pairs = set(zip(_incomplete["ticker"], _incomplete["date_str"]))
        if refetch_pairs:
            _out_clean = _out[~(_out_complete & _out_has_null & _out_old_enough)]
            _out_clean.to_csv(OUTPUT_CSV, index=False)
            print(f"  {len(refetch_pairs)} incomplete rows removed from {OUTPUT_CSV} for re-fetch")

    # ── Dedup: (ticker, date_str) from both output CSVs ──────────────────────
    fetched: set = set()
    for path in (EDGAR_OUTPUT_CSV, ST_OUTPUT_CSV):
        if os.path.exists(path):
            _existing = pd.read_csv(path, usecols=["ticker", "date_str"])
            fetched |= set(zip(_existing["ticker"], _existing["date_str"]))
    fetched -= refetch_pairs  # allow re-fetch of incomplete rows
    print(f"  {len(fetched)} (ticker, date_str) pairs already processed — skipping")
    input_pairs = set(zip(pr_df["ticker"].fillna(""), pr_df["date_str"]))
    to_fetch = len(input_pairs - fetched)
    print(f"  {to_fetch}/{len(input_pairs)} input pairs to fetch")

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
                ticker   = row["ticker"]
                date_str = row["date_str"]

                if (ticker, date_str) in fetched:
                    continue

                if pd.isna(ticker) or not ticker:
                    rows_out.append(_price_row(row, ticker, date_str, {}, source))
                    _flush()
                    continue

                cache_key = (ticker, date_str)

                if cache_key not in bars_cache:
                    print(f"\n  {ticker}  {date_str}", flush=True)

                    # Call 1 — ticker details (most failable: delisted, unknown ticker, bad date)
                    details = await _poly_get(
                        fetch_ticker_details(poly_client, ticker, date_str),
                        f"{ticker} details",
                    )
                    if not details:
                        print(f"  No details — skipping bars for {ticker} {date_str}", flush=True)
                        bars_cache[cache_key] = []
                        rows_out.append(_price_row(row, ticker, date_str, {}, source))
                        _flush()
                        continue

                    market_cap = details.get("market_cap")
                    if market_cap and market_cap > 500_000_000:
                        print(f"  Skipping {ticker} — market cap ${market_cap/1e6:.0f}M > $500M", flush=True)
                        bars_cache[cache_key] = []
                        rows_out.append(_price_row(row, ticker, date_str, {}, source))
                        _flush()
                        continue

                    # Call 2 — 1-min intraday bars (fails on weekends, halted stocks)
                    bars = await _poly_get(
                        fetch_1min_bars(poly_client, ticker, date_str),
                        f"{ticker} 1min",
                    )
                    if not bars:
                        print(f"  No 1min bars — skipping daily for {ticker} {date_str}", flush=True)
                        bars_cache[cache_key] = []
                        rows_out.append(_price_row(row, ticker, date_str, {}, source))
                        _flush()
                        continue

                    # Call 3 — daily bars (least failable)
                    daily = await _poly_get(
                        fetch_daily_bars(poly_client, ticker, date_str),
                        f"{ticker} daily",
                    )

                    if not daily:
                        print(f"  No daily bars for {ticker} {date_str}", flush=True)

                    # Commit to buffers
                    bars_cache[cache_key] = bars
                    daily_cache[cache_key] = daily
                    details_cache[cache_key] = details

                    for bar in bars:
                        intraday_rows.append({"ticker": ticker, "date_str": date_str, **bar})
                    for bar in daily:
                        daily_rows.append({"ticker": ticker, "date_str": date_str, **bar})
                    details_rows.append(_flatten_details(ticker, date_str, details))

                else:
                    bars = bars_cache.get(cache_key, [])

                daily = daily_cache.get(cache_key, [])
                changes = compute_changes(bars, row.get("acceptance_dt"), daily=daily or None)
                rows_out.append(_price_row(row, ticker, date_str, changes, source))
                _flush()

    except (KeyboardInterrupt, Exception) as exc:
        print(f"\nInterrupted ({exc.__class__.__name__}) — saving progress...", flush=True)
        _flush()

    _flush()
    print(f"\nDone. API calls: {api_calls}")
    for path, n in total_written.items():
        print(f"  {n:>6} rows → {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["edgar", "stocktitan"], default="edgar",
                        help="input source (default: edgar)")
    parser.add_argument("--catalyst", metavar="NAME",
                        help="EDGAR only: filter by catalyst tag (e.g. crypto_treasury)")
    parser.add_argument("--sig", action="store_true",
                        help="StockTitan only: filter to significant tags (acquisition, partnership, clinical trial, crypto, private placement, fda approval)")
    parser.add_argument("--refetch", action="store_true",
                        help="re-fetch rows with partial price data (price_t0 set but some changes null)")
    args = parser.parse_args()
    asyncio.run(run(source=args.source, catalyst=args.catalyst, sig=args.sig, refetch=args.refetch))


if __name__ == "__main__":
    main()
