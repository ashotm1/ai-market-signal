"""
heuristic_analysis.py — Statistical analysis of heuristic performance.
Runs all heuristics independently (no hierarchy) across 8-K filings.
"""
import asyncio
import time
import httpx
import pandas as pd
from edgar import fetch_ex99_urls, fetch_html, SEC_ARCHIVES
from classifier import analyze_heuristics, classify_heuristic

BATCH_SIZE = 10       # filings per batch (stay under SEC's 10 req/s)
BATCH_INTERVAL = 1.0  # minimum seconds between batches
INPUT_CSV = "parsed/8k.csv"
OUTPUT_CSV = "parsed/heuristic_analysis.csv"


async def _process_filing(client, row):
    """Process one filing. Returns result dicts only for found EX-99 exhibits."""
    cik = row["CIK"]
    company = row["Company Name"]
    date_filed = row["Date Filed"]
    index_url = SEC_ARCHIVES + row["File Name"].replace(".txt", "-index.html")

    ex99_urls = await fetch_ex99_urls(client, index_url)

    if not ex99_urls:
        return []

    results = []
    for url in ex99_urls:
        html = await fetch_html(client, url)

        if html is None:
            continue

        signals = analyze_heuristics(html)
        final_label = classify_heuristic(html)

        print(
            f"{company[:40]:<40} | "
            + " ".join(f"{k}={v}" for k, v in signals.items())
            + f" | final={final_label}",
            flush=True,
        )

        results.append({
            "cik": cik, "company": company, "date_filed": date_filed,
            "ex99_url": url, **signals, "final_label": final_label,
        })

    return results


async def _run(df):
    all_results = []
    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            t_start = time.monotonic()

            tasks = [_process_filing(client, row) for _, row in batch.iterrows()]
            batch_results = await asyncio.gather(*tasks)
            all_results.extend(item for sublist in batch_results for item in sublist)

            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(df):
                await asyncio.sleep(remaining)

    return all_results


def main():
    df = pd.read_csv(INPUT_CSV).head(100)
    print(f"Loaded {len(df)} 8-K filings", flush=True)

    rows = asyncio.run(_run(df))
    out_df = pd.DataFrame(rows)
    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved {len(out_df)} rows to {OUTPUT_CSV}", flush=True)

    if out_df.empty:
        print("No results — no EX-99 exhibits found or all fetches failed.")
        return

    print("\n--- Heuristic fire rates ---")
    for h in ["H1", "H2", "H4", "H3", "H5", "H6", "H7", "H8"]:
        fired = out_df[h].sum()
        pct = fired / len(out_df) * 100
        print(f"  {h}: fired {fired}/{len(out_df)} ({pct:.1f}%)")


if __name__ == "__main__":
    main()
