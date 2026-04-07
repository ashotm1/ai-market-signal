"""
pr_filter.py — Classify EX-99 exhibits as press releases or not.
Reads parsed/8k_ex99.csv, fetches each EX-99, classifies using heuristics
+ LLM fallback. Saves PR-only rows to parsed/batch_filter_results.csv.

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
from classifier import analyze_heuristics, classify_heuristic, classify_llm

BATCH_SIZE = 10
BATCH_INTERVAL = 1.0
INPUT_CSV = "parsed/8k_ex99.csv"
OUTPUT_CSV = "parsed/prs.csv"


async def _process_exhibit(client, row, use_llm):
    url = row["ex99_url"]
    company = row["company"]

    html = await fetch_html(client, url)
    if html is None:
        print(f"  fetch fail | {company}", flush=True)
        return None

    signals = analyze_heuristics(html)
    heuristic = classify_heuristic(signals)
    if heuristic is None and use_llm:
        heuristic = await classify_llm(html)

    if heuristic is not None:
        print(f"  PR [{heuristic}] | {company}", flush=True)
        return {**row.to_dict(), **signals, "heuristic": heuristic}

    print(f"  not PR     | {company}", flush=True)
    return None


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
            results = await asyncio.gather(*[_process_exhibit(client, row, use_llm) for row in pending])

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
    print(f"Loaded {len(df)} EX-99 exhibits", flush=True)

    if os.path.exists(OUTPUT_CSV):
        existing = pd.read_csv(OUTPUT_CSV, usecols=["ex99_url"])
        fetched_urls = set(existing["ex99_url"])
        print(f"  {len(fetched_urls)} exhibits already classified — skipping", flush=True)
    else:
        fetched_urls = set()

    asyncio.run(_run(df, fetched_urls, use_llm))
    print(f"\nDone. PRs saved to {OUTPUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
