"""
pr_classifier.py — Classify EX-99 exhibits as press releases or not.
Reads data/8k_ex99.csv, fetches each EX-99, classifies using heuristics
+ LLM fallback for weak signals. Saves all exhibits to data/ex_99_classified.csv
with is_pr flag. Skips earnings filings (8-K item 2.02).

Rate: BATCH_SIZE=10 per BATCH_INTERVAL=1.0s → exactly 10 req/s.
Append-safe: skips ex99_urls already present in the output CSV.
"""
import argparse
import asyncio
import os
import time
import httpx
import pandas as pd
from edgar import fetch_html
from classifier import analyze_heuristics, classify_heuristic, classify_llm, extract_title, extract_title_llm, is_earnings, classify_catalyst

BATCH_SIZE = 10
BATCH_INTERVAL = 1.0
LLM_INTERVAL = 1.2   # seconds between LLM calls → ~50 RPM
INPUT_CSV = "data/8k_ex99.csv"
OUTPUT_CSV = "data/ex_99_classified.csv"


async def _fetch_and_score(client, row):
    """Fetch HTML and run heuristics. Returns (row, html, signals, heuristic_label)."""
    url = row["ex99_url"]
    html = await fetch_html(client, url)
    if html is None:
        return row, None, None, None
    signals = analyze_heuristics(html)
    heuristic = classify_heuristic(signals)
    return row, html, signals, heuristic


async def _run(df, fetched_urls, use_llm):
    write_header = not os.path.exists(OUTPUT_CSV)

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            pending = [
                row for _, row in batch.iterrows()
                if row["ex99_url"] not in fetched_urls
            ]

            if not pending:
                continue

            print(f"\n=== BATCH {batch_num} ({len(pending)} exhibits) ===", flush=True)
            t_start = time.monotonic()

            # Fetch HTML + heuristics concurrently (SEC rate: 10 req/s)
            scored = await asyncio.gather(*[_fetch_and_score(client, row) for row in pending])

            # LLM calls sequentially for weak signals (Anthropic rate: 50 RPM)
            results = []
            for row, html, signals, heuristic in scored:
                if html is None:
                    results.append({**row.to_dict(), "H1": None, "H2": None, "H3": None,
                                    "H4": None, "H5": None, "H6": None,
                                    "heuristic": None, "is_pr": False,
                                    "title": None, "catalyst": None})
                    continue
                if heuristic in {None, "H6", "combined"} and is_earnings(html):
                    heuristic = "earnings"
                elif heuristic in {"H6", "combined"} and use_llm:
                    heuristic = await classify_llm(html)
                    await asyncio.sleep(LLM_INTERVAL)
                is_pr = heuristic is not None and heuristic != "earnings"
                title = None
                if is_pr or heuristic == "earnings":
                    title = extract_title(html)
                    if title is None and use_llm:
                        title = await extract_title_llm(html)
                        await asyncio.sleep(LLM_INTERVAL)
                catalyst = classify_catalyst(title) if title else ["other"]
                print(f"  {f'PR [{heuristic}]' if is_pr else 'not PR    '} | {title} | {', '.join(catalyst)}", flush=True)
                results.append({**row.to_dict(), **signals, "heuristic": heuristic, "is_pr": is_pr, "title": title, "catalyst": catalyst})

            rows_out = [r for r in results if r is not None]
            if rows_out:
                pd.DataFrame(rows_out).to_csv(
                    OUTPUT_CSV, mode="a", header=write_header, index=False
                )
                write_header = False

            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(df):
                await asyncio.sleep(remaining)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM fallback, heuristics only")
    args = parser.parse_args()
    use_llm = not args.no_llm

    df = pd.read_csv(INPUT_CSV)
    df = df[df["ex99_url"].notna() & (df["ex99_url"] != "")].reset_index(drop=True)
    before = len(df)
    df = df[~df["items"].fillna("").str.contains(r"\b2\.02\b", regex=True)].reset_index(drop=True)
    print(f"Loaded {len(df)} EX-99 exhibits ({before - len(df)} earnings filings excluded)", flush=True)

    if os.path.exists(OUTPUT_CSV):
        existing = pd.read_csv(OUTPUT_CSV, usecols=["ex99_url"])
        fetched_urls = set(existing["ex99_url"])
        print(f"  {len(fetched_urls)} exhibits already classified — skipping", flush=True)
    else:
        fetched_urls = set()

    asyncio.run(_run(df, fetched_urls, use_llm))
    print(f"\nDone. Exhibits saved to {OUTPUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
