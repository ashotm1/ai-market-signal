"""
bw_scraper.py — Scrape BusinessWire newsroom via CDP-attached real browser.

BW is protected by Akamai Bot Manager. Headless / non-browser clients fail
its JS challenge. This scraper sidesteps that by attaching to a real Chrome
instance that you've already started — Akamai sees your real browser, not a
bot.

SETUP (one-time per session):
  1. Quit Chrome completely.
  2. Start Chrome with a debug port and an isolated profile:

       Windows:
         "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" ^
           --remote-debugging-port=9222 ^
           --user-data-dir="C:\\bw-chrome-profile"

  3. In that Chrome, visit https://www.businesswire.com/newsroom?language=en&page=1
     Wait until articles render (Akamai challenge will auto-solve).
  4. Leave that Chrome window open. Run this script.

Fields: date, time, datetime, ticker, exchange, source, title, url

Usage:
    python scraper/bw_scraper.py --probe              # inspect page 1 structure
    python scraper/bw_scraper.py                       # scrape with default config
    python scraper/bw_scraper.py --max-pages 500
    python scraper/bw_scraper.py --from-page 200 --max-pages 100
"""

import argparse
import csv
import math
import os
import random
import re
import signal
import time
from datetime import datetime

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

OUTPUT_CSV    = "data/bw_news.csv"
RUNS_CSV      = "data/bw_runs.csv"
LOG_DIR       = "logs"
BASE_URL      = "https://www.businesswire.com"

CSV_FIELDS  = ["datetime", "ticker", "exchange", "title", "url"]
RUNS_FIELDS = ["started_at", "from_page", "to_page", "total_pages", "duration"]

# from gnw_scraper.py — same exchange-ticker pattern
_TICKER_RE = re.compile(
    r"\(?(?P<exchange>NYSE American|NYSE Arca|NASDAQ GSM|NASDAQ CM|NASDAQ|NYSE|OTCQB|OTCQX)"
    r"(?:/(?:NYSE|NASDAQ|LSE))?[:\s]+(?P<ticker>[A-Z]{1,6})[;,\s)]*",
    re.IGNORECASE,
)

# BW date format: "May 11, 2026 at 12:17 AM ET"
_BW_DATETIME = re.compile(
    r"([A-Z][a-z]+ \d{1,2}, \d{4})\s+at\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET",
    re.IGNORECASE,
)


def parse_ticker(text: str) -> tuple:
    m = _TICKER_RE.search(text or "")
    if m:
        return m.group("ticker").upper(), m.group("exchange").upper()
    return "", ""


def parse_bw_datetime(text: str) -> str:
    """'May 11, 2026 at 12:17 AM ET' or 'Apr 16, 2026 at 8:30 AM ET' → '2026-05-11 00:17'."""
    if not text:
        return ""
    m = _BW_DATETIME.search(text)
    if not m:
        return ""
    for fmt in ("%B %d, %Y", "%b %d, %Y"):  # full month, then abbreviated
        try:
            d = datetime.strptime(m.group(1), fmt).strftime("%Y-%m-%d")
            break
        except ValueError:
            continue
    else:
        return ""
    hh = int(m.group(2)) % 12
    if m.group(4).upper() == "PM":
        hh += 12
    return f"{d} {hh:02d}:{m.group(3)}"


def parse_page(html: str) -> list:
    """Extract article rows from a BW newsroom page."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    # Anchor: <a class="font-figtree" href="/news/home/...">
    for a in soup.select('a.font-figtree[href*="/news/home/"]'):
        url = a.get("href", "")
        if not url:
            continue
        if not url.startswith("http"):
            url = BASE_URL + url
        if url in seen:
            continue
        seen.add(url)

        # Title is the <h2> text inside the anchor
        h2 = a.find("h2")
        title = h2.get_text(strip=True) if h2 else a.get_text(strip=True)
        if not title:
            continue

        # Article row is the nearest <div> with class 'border-gray300'
        row = a.find_parent("div", class_="border-gray300")

        dt = ""
        ticker = exchange = ""
        if row:
            for span in row.find_all("span"):
                m = parse_bw_datetime(span.get_text(strip=True))
                if m:
                    dt = m
                    break
            preview = row.select_one(".rich-text")
            if preview:
                ticker, exchange = parse_ticker(preview.get_text(" ", strip=True))

        items.append({
            "datetime": dt,
            "ticker":   ticker,
            "exchange": exchange,
            "title":    title,
            "url":      url,
        })
    return items


_last_mouse_pos = [400, 400]  # tracked across calls; mouse starts somewhere reasonable


def human_move(page, x: int, y: int, duration_ms: int | None = None):
    """Move mouse to (x, y) over duration_ms with human-like pacing.

    Real mouse moves take 200–500ms; default Playwright `.move(x, y, steps=N)`
    fires events instantly, which Akamai's behavioral model can spot.
    This interpolates with explicit sleeps between sub-moves.
    """
    if duration_ms is None:
        duration_ms = random.randint(200, 500)
    sx, sy = _last_mouse_pos
    steps = max(8, duration_ms // 25)
    per_step = max(1, duration_ms // steps)
    for i in range(1, steps + 1):
        t = i / steps
        page.mouse.move(sx + (x - sx) * t, sy + (y - sy) * t)
        page.wait_for_timeout(per_step)
    _last_mouse_pos[0], _last_mouse_pos[1] = x, y


def simulate_human(page):
    """Variable mix of mouse moves, scrolls, and pauses. Number of actions per page
    is randomized to avoid the 'identical activity profile' bot signal."""
    try:
        n_actions = random.choices(
            [0, 1, 2, 3, 4],
            weights=[10, 30, 35, 20, 5],
        )[0]
        for _ in range(n_actions):
            action = random.choices(
                ["move", "scroll_down", "scroll_up", "pause"],
                weights=[40, 30, 10, 20],
            )[0]
            if action == "move":
                human_move(page, random.randint(150, 1300), random.randint(150, 750))
            elif action == "scroll_down":
                page.evaluate(f"window.scrollBy(0, {random.randint(100, 900)})")
            elif action == "scroll_up":
                page.evaluate(f"window.scrollBy(0, -{random.randint(50, 400)})")
            # 'pause' is just the wait below
            page.wait_for_timeout(random.randint(100, 700))

        # ~1/30 pages: a no-op click (real click event, doesn't navigate)
        if random.random() < 1 / 30:
            page.evaluate("document.body.click()")
        # ~1/15 pages: a Tab keypress (fires focus + key events)
        if random.random() < 1 / 15:
            page.keyboard.press("Tab")
    except Exception:
        pass


def load_existing_rows() -> dict:
    """url → row dict from existing CSV."""
    rows: dict = {}
    if not os.path.exists(OUTPUT_CSV):
        return rows
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["url"]] = row
    return rows


def write_all(rows: dict):
    """Atomic full rewrite via tmp + rename."""
    tmp = OUTPUT_CSV + ".tmp"
    with open(tmp, "w", newline="\n", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows.values():
            writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
    os.replace(tmp, OUTPUT_CSV)


def fmt_duration(seconds: float) -> str:
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def load_runs() -> list:
    if not os.path.exists(RUNS_CSV):
        return []
    with open(RUNS_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_runs(runs: list):
    tmp = RUNS_CSV + ".tmp"
    with open(tmp, "w", newline="\n", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RUNS_FIELDS)
        w.writeheader()
        for r in runs:
            w.writerow({k: r.get(k, "") for k in RUNS_FIELDS})
    os.replace(tmp, RUNS_CSV)




def navigate(page, url: str) -> bool:
    """One nav attempt: goto + spoof visibility + wait for anchors + wait for date
    hydration. Returns True on success, False on any error. Prints inline status."""
    try:
        page.goto(url, wait_until="commit", timeout=15000)
        page.evaluate("""
            Object.defineProperty(document, 'hidden',          {configurable: true, get: () => false});
            Object.defineProperty(document, 'visibilityState', {configurable: true, get: () => 'visible'});
        """)
        page.wait_for_selector('a[href*="/news/home/"]', timeout=10000)
        try:
            page.wait_for_function(
                """() => {
                    const anchors = document.querySelectorAll('a[href*="/news/home/"]');
                    const re = / at \\d{1,2}:\\d{2}\\s*(AM|PM)\\s*ET/i;
                    const dated = [...document.querySelectorAll('span')]
                        .filter(s => re.test(s.textContent)).length;
                    return anchors.length > 0 && dated >= anchors.length;
                }""",
                timeout=8000,
            )
        except Exception:
            print("hydrate-timeout", end=" ", flush=True)
        return True
    except Exception as e:
        print(f"nav-fail ({e.__class__.__name__})", end=" ", flush=True)
        return False


class _Tee:
    """Writes to multiple streams. Lets every existing `print` also hit a log file."""
    def __init__(self, *streams):
        self._streams = streams
    def write(self, s):
        for st in self._streams:
            st.write(s)
    def flush(self):
        for st in self._streams:
            st.flush()


def main():
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"bw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    log_file = open(log_path, "a", encoding="utf-8", buffering=1)
    sys.stdout = _Tee(sys.stdout, log_file)
    sys.stderr = _Tee(sys.stderr, log_file)
    print(f"[log] {log_path}", flush=True)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug-port", type=int, default=9222)
    parser.add_argument("--from-page",  type=int, default=None,
                        help="start page (default: max_page+1 from bw_max_page.txt, or 1 if none)")
    parser.add_argument("--to-page",    type=int, default=None,
                        help="end page (inclusive). If omitted, scrape until dup-stop or until-date triggers.")
    parser.add_argument("--until-date", type=str, default=None,
                        help="stop when ALL items on a page have datetime < this YYYY-MM-DD (for backfill)")
    parser.add_argument("--dup-stop",   type=int, default=5,
                        help="stop after N consecutive pages of all duplicates (default 5)")
    parser.add_argument("--probe",      action="store_true",
                        help="fetch page 1, print structure, exit (no CSV writes)")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(OUTPUT_CSV) or ".", exist_ok=True)

    with sync_playwright() as p:
        try:
            browser = p.chromium.connect_over_cdp(f"http://127.0.0.1:{args.debug_port}")
        except Exception as e:
            print(f"Failed to connect to Chrome via CDP on port {args.debug_port}.")
            print(f"Error: {e}")
            print("\nDid you start Chrome with --remote-debugging-port? See header of this script.")
            return

        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
        page = ctx.new_page()
        print(f"Working tab opened. Context has {len(ctx.pages)} tab(s).", flush=True)

        def _cleanup():
            try:
                page.close()
            except Exception:
                pass

        import atexit
        atexit.register(_cleanup)

        if args.probe:
            url = f"{BASE_URL}/newsroom?language=en&page=1"
            print(f"PROBE: navigating to {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)
            html = page.content()
            print(f"  html length: {len(html)}")
            items = parse_page(html)
            print(f"  parsed items: {len(items)}")
            if items:
                print("\n--- first 3 items ---")
                for it in items[:3]:
                    for k, v in it.items():
                        print(f"  {k}: {v}")
                    print()
            else:
                # dump a snippet to help refine selectors
                print("\n--- HTML head (first 3000 chars) ---")
                print(html[:3000])
            return

        existing_rows = load_existing_rows()
        prior_runs = load_runs()
        prior_to_pages = [int(r["to_page"]) for r in prior_runs if r.get("to_page", "").isdigit()]
        max_page = max(prior_to_pages) if prior_to_pages else 0
        start_page = args.from_page if args.from_page is not None else (max_page + 1 if max_page else 1)
        end_page = args.to_page if args.to_page is not None else start_page + 100000
        existing_dts = [r.get("datetime", "") for r in existing_rows.values() if r.get("datetime")]
        newest = max(existing_dts) if existing_dts else "(none)"
        oldest = min(existing_dts) if existing_dts else "(none)"
        print(f"Existing: {len(existing_rows)} URLs in {OUTPUT_CSV}")
        print(f"  newest: {newest}   oldest: {oldest}   max page seen: {max_page or '(none)'}")
        print(f"Pages {start_page}..{end_page}", end="")
        if args.until_date:
            print(f"   until_date={args.until_date}", end="")
        print("\n")

        total_new = 0
        dup_streak = 0
        nav_fail_streak = 0
        pages_scraped = 0

        # Session pacing: scrape for 30-120min (left-skewed toward 2h), then break 5-10min
        def _new_session_max():
            return (30 + random.betavariate(5, 2) * 90) * 60  # seconds
        session_start = time.time()
        session_max = _new_session_max()
        print(f"  session window: {session_max/60:.0f}min before break\n")

        run_start = time.time()
        runs = prior_runs
        current_run = {
            "started_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "from_page":   str(start_page),
            "to_page":     "",
            "total_pages": "0",
            "duration":    "00:00:00",
        }
        runs.append(current_run)
        write_runs(runs)

        # Backoff (seconds) before each retry. First try has no backoff.
        # Schedule: try, retry-now, 30s, 1min, 5min, then give up.
        retry_waits = (0, 30, 60, 300)

        for page_n in range(start_page, end_page + 1):
            url = f"{BASE_URL}/newsroom?language=en&page={page_n}"
            cycle_start = time.time()
            print(f"  page {page_n}: nav...", end=" ", flush=True)

            nav_ok = navigate(page, url)
            for wait in retry_waits:
                if nav_ok:
                    break
                if wait:
                    print(f"wait {wait}s, retry...", end=" ", flush=True)
                    time.sleep(wait)
                else:
                    print("retry now...", end=" ", flush=True)
                nav_ok = navigate(page, url)

            if not nav_ok:
                print("\nnav retries exhausted — stopping")
                break
            simulate_human(page)
            nav_fail_streak = 0

            html = page.content()
            if len(html) < 5000:
                print(f"suspiciously small response ({len(html)}B) — possibly re-challenged")
                nav_fail_streak += 1
                if nav_fail_streak >= 3:
                    print("\n3 consecutive small/failed responses — exiting (likely blocked)")
                    break
                time.sleep(15 + random.uniform(0, 10))  # back off, let Akamai cool
                continue

            items = parse_page(html)

            new_count = 0
            updated_count = 0
            for it in items:
                prev = existing_rows.get(it["url"])
                if prev is None:
                    existing_rows[it["url"]] = it
                    new_count += 1
                else:
                    changed = False
                    for k in CSV_FIELDS:
                        if not prev.get(k) and it.get(k):
                            prev[k] = it[k]
                            changed = True
                    if changed:
                        updated_count += 1

            if new_count or updated_count:
                write_all(existing_rows)

            total_new += new_count
            print(f"new={new_count}  updated={updated_count}  total_new={total_new}")

            pages_scraped += 1
            current_run["to_page"]     = str(page_n)
            current_run["total_pages"] = str(pages_scraped)
            current_run["duration"]    = fmt_duration(time.time() - run_start)
            write_runs(runs)

            if args.until_date and items:
                page_dts = [it["datetime"] for it in items if it.get("datetime")]
                if page_dts and all(dt[:10] < args.until_date for dt in page_dts):
                    print(f"\nall items on page {page_n} older than {args.until_date} — done")
                    break

            if not items:
                print("  0 items parsed — page may have changed structure or empty, stopping")
                break

            if new_count == 0:
                dup_streak += 1
                if dup_streak >= args.dup_stop:
                    print(f"\n{dup_streak} consecutive all-duplicate pages — done")
                    break
            else:
                dup_streak = 0

            # Cycle-target sleep: target whole-iteration time ~3s (log-normal),
            # subtract elapsed nav+hydration+sim+parse+write so the inter-request
            # cadence the server sees is what's randomized, not the leftover pad.
            target = min(15.0, random.lognormvariate(math.log(3) - 0.18, 0.6))
            time.sleep(max(0.0, target - (time.time() - cycle_start)))

            # Long break: triggers once session_max elapsed (30-120min, left-skewed)
            if time.time() - session_start >= session_max:
                break_secs = random.uniform(5, 10) * 60
                print(f"\n  [break] sleeping {break_secs/60:.1f}min — session was {(time.time()-session_start)/60:.1f}min\n")
                time.sleep(break_secs)
                # Shift run_start forward so duration excludes the break
                run_start += break_secs
                session_start = time.time()
                session_max = _new_session_max()
                print(f"  [resume] next session window: {session_max/60:.0f}min\n")

        current_run["duration"] = fmt_duration(time.time() - run_start)
        write_runs(runs)
        print(f"\nDone. {total_new} new articles -> {OUTPUT_CSV}")
        print(f"Ran {pages_scraped} pages ({current_run['from_page']} -> {current_run['to_page'] or 'none'}) in {current_run['duration']}")


if __name__ == "__main__":
    main()
