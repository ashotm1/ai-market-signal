"""
batch_filter.py — Batch press release detection over 8-K filings.
Reads parsed/8k.csv, fetches EX-99 exhibits, classifies each as PR or not.
"""
import asyncio
import time
import httpx
import pandas as pd
from edgar import fetch_ex99_urls, fetch_html, SEC_ARCHIVES
from classifier import classify

BATCH_SIZE = 10
BATCH_INTERVAL = 1.0
OUTPUT_CSV = "parsed/batch_filter_results.csv"


async def _process_filing(client, row):
    company = row["Company Name"]
    cik = row["CIK"]
    date_filed = row["Date Filed"]
    index_url = SEC_ARCHIVES + row["File Name"].replace(".txt", "-index.html")

    ex99_urls = await fetch_ex99_urls(client, index_url)

    if not ex99_urls:
        print(f"  no ex-99  | {company}", flush=True)
        return [{
            "cik": cik, "company": company, "date_filed": date_filed,
            "index_url": index_url, "ex99_url": None,
            "is_pr": False, "heuristic": None,
        }]

    results = []
    for url in ex99_urls:
        html = await fetch_html(client, url)

        if html is None:
            print(f"  fetch fail | {company}", flush=True)
            results.append({
                "cik": cik, "company": company, "date_filed": date_filed,
                "index_url": index_url, "ex99_url": url,
                "is_pr": None, "heuristic": "fetch_failed",
            })
            continue

        heuristic = await classify(html)
        is_pr = heuristic is not None

        if is_pr:
            print(f"  PR [{heuristic}] | {company}", flush=True)
        else:
            print(f"  not PR     | {company}", flush=True)

        results.append({
            "cik": cik, "company": company, "date_filed": date_filed,
            "index_url": index_url, "ex99_url": url,
            "is_pr": is_pr, "heuristic": heuristic,
        })

    return results


async def _run(df):
    all_results = []

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            t_start = time.monotonic()

            print(f"\n=== BATCH {batch_num} ({len(batch)} filings) ===", flush=True)
            tasks = [_process_filing(client, row) for _, row in batch.iterrows()]
            batch_results = await asyncio.gather(*tasks)
            all_results.extend(item for sublist in batch_results for item in sublist)

            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(df):
                await asyncio.sleep(remaining)

    return all_results


def main():
    df = pd.read_csv("parsed/8k.csv").head(100)
    rows = asyncio.run(_run(df))
    out_df = pd.DataFrame(rows)
    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved {len(out_df)} rows to {OUTPUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
