"""
fetch_market_data.py — Fetch market data driven by newswire signal events.

Three bar files, each with a different dedup strategy:
  1min_bars.csv   — tight window per event (event-2d..event+7d)
                    dedup: skip (ticker, event_date) already present
  10min_bars.csv  — today → 5yr back per ticker, gap-fill on repeat runs
                    dedup: fetch only from max(bar_date) → today per ticker
  daily_bars_nw.csv — same gap-fill strategy as 10-min
  ticker_details.csv — market cap / shares / exchange per (ticker, event_date)
                    dedup: skip (ticker, event_date) already present

Sources: gnw_signal_filtered, prnw_signal_filtered, bw_signal_filtered
Market-cap gate: skip tickers with cap > 500M at event date.

Requirements: MASSIVE_API_KEY or POLYGON_API_KEY env var.
Usage:
  python -m market.fetch_market_data
  python -m market.fetch_market_data --sources gnw prnw
  python -m market.fetch_market_data --catalyst private_placement
"""
import argparse
import ast
import asyncio
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

from config.paths import (
    GNW_SIGNAL, PRNW_SIGNAL, BW_SIGNAL,
    BARS_1MIN, BARS_10MIN, BARS_DAILY_NW, TICKER_DETAILS,
    ensure_dirs,
)

_ET  = ZoneInfo("America/New_York")

MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY")
POLYGON_BASE    = "https://api.polygon.io"
MAX_CONCURRENT  = 5
MAX_MKTCAP      = 500_000_000
_BAR_FIELDS     = ("v", "vw", "o", "c", "h", "l", "t", "n")
_FETCH_ERROR    = object()
_log_file       = None   # set in main(); verbose lines go here only, not terminal

def _log(msg: str):
    if _log_file:
        _log_file.write(msg + "\n")
        _log_file.flush()

# "starter" enforces a 5-year lookback cap on all event fetches
POLYGON_TIER          = "starter"
_STARTER_LOOKBACK_YRS = 5


# ── Polygon helpers ───────────────────────────────────────────────────────────

async def _get_single(client: httpx.AsyncClient, url: str) -> list:
    """Single-page GET. Returns _FETCH_ERROR on failure."""
    try:
        r = await client.get(url, timeout=30)
    except Exception:
        return _FETCH_ERROR
    if r.status_code != 200:
        return _FETCH_ERROR
    return r.json().get("results") or []


async def _stream_pages(client: httpx.AsyncClient, url: str, on_page) -> bool:
    """Paginate through all results, calling on_page(results) per page.
    Writes and discards each page immediately — never accumulates full history.
    Returns False on first fetch error."""
    while url:
        try:
            r = await client.get(url, timeout=30)
        except Exception:
            return False
        if r.status_code != 200:
            return False
        data = r.json()
        results = data.get("results") or []
        if results:
            on_page(results)
        nxt  = data.get("next_url")
        url  = f"{nxt}&apiKey={MASSIVE_API_KEY}" if nxt else None
    return True


async def fetch_1min_bars(client: httpx.AsyncClient, ticker: str, event_date: str) -> list:
    """Tight window — single page, return list directly."""
    base  = datetime.strptime(event_date, "%Y-%m-%d")
    from_ = (base - timedelta(days=1)).strftime("%Y-%m-%d")
    to_   = (base + timedelta(days=2)).strftime("%Y-%m-%d")
    url = (
        f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/minute/{from_}/{to_}"
        f"?adjusted=true&sort=asc&limit=50000&apiKey={MASSIVE_API_KEY}"
    )
    return await _get_single(client, url)


async def fetch_10min_bars_stream(client: httpx.AsyncClient, ticker: str,
                                   from_date: str, to_date: str, on_page) -> bool:
    """10-min bars, paginated — calls on_page(results) per page, never accumulates."""
    url = (
        f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/10/minute/{from_date}/{to_date}"
        f"?adjusted=true&sort=asc&limit=50000&apiKey={MASSIVE_API_KEY}"
    )
    return await _stream_pages(client, url, on_page)


async def fetch_daily_bars_stream(client: httpx.AsyncClient, ticker: str,
                                   from_date: str, to_date: str, on_page) -> bool:
    """Daily bars, paginated — calls on_page(results) per page, never accumulates."""
    url = (
        f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
        f"?adjusted=true&sort=asc&limit=50000&apiKey={MASSIVE_API_KEY}"
    )
    return await _stream_pages(client, url, on_page)


_NO_DATA = object()  # 404 / empty result — ticker unknown or delisted

async def fetch_ticker_details(client: httpx.AsyncClient, ticker: str, event_date: str) -> dict:
    prior = (datetime.strptime(event_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    url   = f"{POLYGON_BASE}/v3/reference/tickers/{ticker}?date={prior}&apiKey={MASSIVE_API_KEY}"
    try:
        r = await client.get(url, timeout=30)
    except Exception:
        return _FETCH_ERROR
    if r.status_code == 404:
        return _NO_DATA
    if r.status_code != 200:
        return _FETCH_ERROR
    return r.json().get("results") or {}


# ── NW source loaders ─────────────────────────────────────────────────────────

def _parse_et(s) -> str | None:
    """'YYYY-MM-DD HH:MM' (ET, no TZ) → ISO with ET offset."""
    try:
        return datetime.strptime(str(s).strip()[:16], "%Y-%m-%d %H:%M").replace(tzinfo=_ET).isoformat()
    except Exception:
        return None


def _parse_iso(s) -> str | None:
    """ISO string with TZ offset → normalized ISO string."""
    try:
        return datetime.fromisoformat(str(s).strip().replace("Z", "+00:00")).isoformat()
    except Exception:
        return None


def _has_catalyst(v, target: str) -> bool:
    try:
        tags = ast.literal_eval(v) if isinstance(v, str) else [v]
        return target in tags
    except Exception:
        return False


def load_nw_events(sources: list[str], catalyst: str | None = None) -> pd.DataFrame:
    """Load (ticker, event_date, event_dt) from requested NW sources, deduped by (ticker, event_date)."""
    frames = []

    if "gnw" in sources and os.path.exists(GNW_SIGNAL):
        df = pd.read_csv(GNW_SIGNAL, usecols=["datetime", "ticker", "catalyst"])
        df = df[df["ticker"].notna() & (df["ticker"].astype(str).str.strip() != "")]
        if catalyst:
            df = df[df["catalyst"].apply(lambda v: _has_catalyst(v, catalyst))]
        df["event_dt"]   = df["datetime"].apply(_parse_et)
        df["event_date"] = df["event_dt"].str[:10]
        frames.append(df[["ticker", "event_date", "event_dt"]].dropna(subset=["event_dt"]))

    if "prnw" in sources and os.path.exists(PRNW_SIGNAL):
        df = pd.read_csv(PRNW_SIGNAL, usecols=["datetime", "ticker", "catalyst"])
        df = df[df["ticker"].notna() & (df["ticker"].astype(str).str.strip() != "")]
        if catalyst:
            df = df[df["catalyst"].apply(lambda v: _has_catalyst(v, catalyst))]
        df["event_dt"]   = df["datetime"].apply(_parse_iso)
        df["event_date"] = df["event_dt"].str[:10]
        frames.append(df[["ticker", "event_date", "event_dt"]].dropna(subset=["event_dt"]))

    if "bw" in sources and os.path.exists(BW_SIGNAL):
        df = pd.read_csv(BW_SIGNAL, usecols=["datetime", "ticker"])
        df = df[df["ticker"].notna() & (df["ticker"].astype(str).str.strip() != "")]
        df["event_dt"]   = df["datetime"].apply(_parse_et)
        df["event_date"] = df["event_dt"].str[:10]
        frames.append(df[["ticker", "event_date", "event_dt"]].dropna(subset=["event_dt"]))

    if not frames:
        return pd.DataFrame(columns=["ticker", "event_date", "event_dt"])

    combined = pd.concat(frames, ignore_index=True)
    # keep earliest event_dt per (ticker, event_date) when sources overlap
    combined = combined.sort_values("event_dt").drop_duplicates(subset=["ticker", "event_date"])
    return combined.reset_index(drop=True)


# ── Dedup loaders ─────────────────────────────────────────────────────────────

def load_done_1min() -> set:
    """Set of (ticker, event_date) already stored in 1min_bars."""
    if not os.path.exists(BARS_1MIN):
        return set()
    df = pd.read_csv(BARS_1MIN, usecols=["ticker", "event_date"], on_bad_lines="skip")
    return set(zip(df["ticker"], df["event_date"]))


def load_last_bar_dates(path: str) -> dict:
    """max bar date (YYYY-MM-DD) per ticker from a wide (10min/daily) bars file."""
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path, usecols=["ticker", "t"], on_bad_lines="skip")
    df["bar_date"] = pd.to_datetime(df["t"], unit="ms").dt.strftime("%Y-%m-%d")
    return df.groupby("ticker")["bar_date"].max().to_dict()


def load_done_details() -> tuple[set, dict]:
    """Returns (done set of (ticker, date_str), {ticker: market_cap} for mktcap gate)."""
    if not os.path.exists(TICKER_DETAILS):
        return set(), {}
    df = pd.read_csv(TICKER_DETAILS, usecols=["ticker", "date_str", "market_cap"], on_bad_lines="skip")
    done = set(zip(df["ticker"], df["date_str"]))
    mc_map = df.dropna(subset=["market_cap"]).groupby("ticker")["market_cap"].last().to_dict()
    return done, mc_map


# ── Helpers ───────────────────────────────────────────────────────────────────

def _repair_csv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r+b") as f:
        f.seek(-1, 2)
        if f.read(1) == b"\n":
            return
        f.seek(0, 2)
        pos = f.tell() - 1
        while pos > 0:
            pos -= 1
            f.seek(pos)
            if f.read(1) == b"\n":
                f.seek(pos + 1)
                f.truncate()
                print(f"  repaired partial write in {path}", flush=True)
                return


def _flatten_details(ticker: str, event_date: str, d: dict) -> dict:
    return {
        "ticker":                         ticker,
        "date_str":                       event_date,
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


def _gap_from_date(last_date: str | None, five_yr: str) -> str:
    """Start date for a gap-fill fetch: one day before last known bar, or five years ago."""
    if not last_date:
        return five_yr
    return (datetime.strptime(last_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")


# ── Label computation (news-time baseline) ────────────────────────────────────

_HORIZONS_MS = {
    "5m":  5  * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}


def compute_changes(bars: list, event_dt: str) -> dict:
    """News-time baseline: bar before event_dt → forward returns at 5m/30m/1h/1d."""
    result = {"price_t0": None, **{f"change_{h}_pct": None for h in _HORIZONS_MS}}
    if not bars or not event_dt:
        return result
    try:
        t0_ms = int(datetime.fromisoformat(event_dt).timestamp() * 1000)
    except ValueError:
        return result
    p0_bar = next((b for b in reversed(bars) if b["t"] < t0_ms), None)
    if p0_bar is None:
        return result
    p0 = p0_bar["c"]
    result["price_t0"] = p0
    for h, off in _HORIZONS_MS.items():
        fwd = next((b for b in bars if b["t"] >= t0_ms + off), None)
        if fwd:
            result[f"change_{h}_pct"] = round((fwd["c"] - p0) / p0 * 100, 4)
    return result


# ── Main orchestrator ─────────────────────────────────────────────────────────

async def run_nw(sources: list[str] | None = None, catalyst: str | None = None):
    if not MASSIVE_API_KEY:
        raise RuntimeError("Set MASSIVE_API_KEY or POLYGON_API_KEY.")
    ensure_dirs()

    sources = sources or ["gnw", "prnw", "bw"]
    today   = datetime.today().strftime("%Y-%m-%d")
    five_yr = (datetime.today() - timedelta(days=_STARTER_LOOKBACK_YRS * 365)).strftime("%Y-%m-%d")

    events = load_nw_events(sources, catalyst)
    print(f"Loaded {len(events)} unique (ticker, event_date) pairs from {sources}")

    cutoff = (pd.Timestamp.today() - 2 * pd.tseries.offsets.BDay()).strftime("%Y-%m-%d")
    events = events[events["event_date"] <= cutoff].reset_index(drop=True)
    print(f"  {len(events)} after 2-trading-day cutoff ({cutoff})")

    if POLYGON_TIER == "starter":
        events = events[events["event_date"] >= five_yr].reset_index(drop=True)
        print(f"  {len(events)} after Starter tier 5yr lookback filter (>= {five_yr})")

    done_1min    = load_done_1min()
    last_10min   = load_last_bar_dates(BARS_10MIN)
    last_daily   = load_last_bar_dates(BARS_DAILY_NW)
    done_details, known_mc = load_done_details()

    by_ticker: dict[str, list] = {}
    for _, row in events.iterrows():
        by_ticker.setdefault(row["ticker"], []).append(row)
    print(f"  {len(by_ticker)} unique tickers\n")

    write_header = {
        BARS_1MIN:      not os.path.exists(BARS_1MIN),
        BARS_10MIN:     not os.path.exists(BARS_10MIN),
        BARS_DAILY_NW:  not os.path.exists(BARS_DAILY_NW),
        TICKER_DETAILS: not os.path.exists(TICKER_DETAILS),
    }
    written = {p: 0 for p in write_header}
    PROGRESS_EVERY = 500
    # counters per call type: [ok, no_data, error]
    c = {
        "det":   [0, 0, 0],
        "1min":  [0, 0, 0],
        "10min": [0, 0, 0],
        "daily": [0, 0, 0],
        "events": 0,   # total events seen (all tickers, including skipped)
        "tickers": 0,  # unique tickers processed
    }

    def _append(path: str, rows: list):
        if not rows:
            return
        df = pd.DataFrame(rows)
        df.to_csv(path, mode="a", header=write_header[path], index=False)
        write_header[path] = False
        written[path] += len(rows)

    def _progress():
        def fmt(counts): return f"ok={counts[0]}  no_data={counts[1]}  err={counts[2]}"
        print(
            f"  [events:{c['events']}  tickers:{c['tickers']}]"
            f"  det({fmt(c['det'])})"
            f"  1min({fmt(c['1min'])})"
            f"  10min({fmt(c['10min'])})"
            f"  daily({fmt(c['daily'])})",
            flush=True,
        )

    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async def _process_ticker(client: httpx.AsyncClient, ticker: str, ticker_events: list):
        async with sem:
            try:
                events_need_1min    = [e for e in ticker_events if (ticker, e["event_date"]) not in done_1min]
                events_need_details = [e for e in ticker_events if (ticker, e["event_date"]) not in done_details]

                last_10 = last_10min.get(ticker)
                last_dy = last_daily.get(ticker)
                need_10min = not last_10 or last_10 < today
                need_daily = not last_dy or last_dy < today

                if not events_need_1min and not events_need_details and not need_10min and not need_daily:
                    return

                # ── Ticker details + market-cap gate ──────────────────────────
                mc = None
                if events_need_details:
                    first = min(events_need_details, key=lambda e: e["event_date"])
                    det = await fetch_ticker_details(client, ticker, first["event_date"])
                    if det is _FETCH_ERROR:
                        c["det"][2] += 1
                        _log(f"  {ticker}  det=err")
                        return
                    if det is _NO_DATA or not det:
                        c["det"][1] += 1
                        _log(f"  {ticker}  det=no_data")
                        return
                    mc = det.get("market_cap")
                    if mc and mc > MAX_MKTCAP:
                        c["det"][1] += 1
                        _log(f"  {ticker}  det=skip_mktcap_{mc/1e6:.0f}M")
                        return
                    c["det"][0] += 1
                    _append(TICKER_DETAILS, [_flatten_details(ticker, e["event_date"], det) for e in events_need_details])
                    _log(f"  {ticker}  det=ok  mktcap={mc/1e6:.0f}M" if mc else f"  {ticker}  det=ok  mktcap=unknown")
                elif events_need_1min or need_10min or need_daily:
                    mc = known_mc.get(ticker)
                    if mc and mc > MAX_MKTCAP:
                        return

                # ── 1-min: per-event tight window ─────────────────────────────
                for ev in events_need_1min:
                    bars = await fetch_1min_bars(client, ticker, ev["event_date"])
                    if not bars or bars is _FETCH_ERROR:
                        c["1min"][2 if bars is _FETCH_ERROR else 1] += 1
                        _log(f"  {ticker}  {ev['event_date']}  1min=no_data")
                    else:
                        c["1min"][0] += 1
                        _append(BARS_1MIN, [{"ticker": ticker, "event_date": ev["event_date"],
                                             **{k: b.get(k) for k in _BAR_FIELDS}} for b in bars])
                        _log(f"  {ticker}  {ev['event_date']}  1min=ok  bars={len(bars)}")

                # ── 10-min: gap-fill or full 5yr (streamed page-by-page) ───────
                if need_10min:
                    from_10 = _gap_from_date(last_10, five_yr)
                    n_10min = 0
                    def _on_10min(results):
                        nonlocal n_10min
                        _append(BARS_10MIN, [{"ticker": ticker, **{k: b.get(k) for k in _BAR_FIELDS}} for b in results])
                        n_10min += len(results)
                    ok_10 = await fetch_10min_bars_stream(client, ticker, from_10, today, _on_10min)
                    if ok_10 and n_10min:
                        c["10min"][0] += 1
                        _log(f"  {ticker}  10min=ok  bars={n_10min}")
                    else:
                        c["10min"][2 if not ok_10 else 1] += 1
                        _log(f"  {ticker}  10min={'err' if not ok_10 else 'no_data'}")

                # ── Daily: gap-fill or full 5yr (streamed page-by-page) ────────
                if need_daily:
                    from_dy = _gap_from_date(last_dy, five_yr)
                    n_daily = 0
                    def _on_daily(results):
                        nonlocal n_daily
                        _append(BARS_DAILY_NW, [{"ticker": ticker, **{k: b.get(k) for k in _BAR_FIELDS}} for b in results])
                        n_daily += len(results)
                    ok_dy = await fetch_daily_bars_stream(client, ticker, from_dy, today, _on_daily)
                    if ok_dy and n_daily:
                        c["daily"][0] += 1
                        _log(f"  {ticker}  daily=ok  bars={n_daily}")
                    else:
                        c["daily"][2 if not ok_dy else 1] += 1
                        _log(f"  {ticker}  daily={'err' if not ok_dy else 'no_data'}")

            finally:
                c["events"]  += len(ticker_events)
                c["tickers"] += 1
                if c["events"] % PROGRESS_EVERY < len(ticker_events):
                    _progress()

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await asyncio.gather(*[
                _process_ticker(client, ticker, evs)
                for ticker, evs in by_ticker.items()
            ])
    except (KeyboardInterrupt, Exception) as exc:
        print(f"\nInterrupted ({type(exc).__name__}) — repairing CSVs...", flush=True)
        for p in write_header:
            _repair_csv(p)

    print("\nDone.")
    for p, n in written.items():
        if n:
            print(f"  {n:>8} rows → {p}")


# ── CLI ───────────────────────────────────────────────────────────────────────

class _Tee:
    """Writes to multiple streams so every print hits both terminal and log file."""
    def __init__(self, *streams):
        self._streams = streams
    def write(self, s):
        for st in self._streams:
            st.write(s)
    def flush(self):
        for st in self._streams:
            st.flush()


def main():
    global _log_file
    os.makedirs("logs", exist_ok=True)
    log_path = os.path.join("logs", f"fetch_market_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    _log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    sys.stdout = _Tee(sys.stdout, _log_file)
    sys.stderr = _Tee(sys.stderr, _log_file)
    print(f"[log] {log_path}", flush=True)

    parser = argparse.ArgumentParser(description="Fetch NW-driven market data from Polygon.")
    parser.add_argument("--sources", nargs="+", default=["gnw", "prnw", "bw"],
                        choices=["gnw", "prnw", "bw"],
                        help="NW sources to include (default: all three)")
    parser.add_argument("--catalyst", metavar="NAME",
                        help="filter to a single catalyst tag (e.g. private_placement)")
    args = parser.parse_args()
    asyncio.run(run_nw(sources=args.sources, catalyst=args.catalyst))


if __name__ == "__main__":
    main()
