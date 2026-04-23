"""
pipeline.py — Run the full SEC EDGAR pipeline from index download to PR classification.

Steps (in order):
  1. download_idx.py     — download daily index files from EDGAR
  2. parse_idx.py        — parse index files → data/8k.csv
  3. batch_filter.py     — fetch filing index pages → data/8k_ex99.csv
  4. classify_exhibits.py      — classify EX-99 exhibits → data/ex_99_classified.csv
  5. classify_catalyst_llm.py  — (optional --llm) LLM catalyst classify for 'other' rows
  6. fetch_market_data.py — (optional --prices) fetch Polygon market data for signal rows

Each step is append-safe and skips already-processed rows.
Feature extraction (extract_features.py) is run separately.

Usage:
  python pipeline.py --date-from 2022-01-01 --date-to 2025-12-31
  python pipeline.py --days 30 --llm --market
"""
import argparse
import subprocess
import sys


def run(cmd: list[str], label: str):
    print(f"\n{'='*60}", flush=True)
    print(f"  {label}", flush=True)
    print(f"{'='*60}", flush=True)
    result = subprocess.run([sys.executable] + cmd)
    if result.returncode != 0:
        print(f"\nPipeline failed at: {label} (exit code {result.returncode})")
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date-from", metavar="YYYY-MM-DD",
                       help="Start date for index download")
    group.add_argument("--days", type=int,
                       help="Download last N days of index files")
    parser.add_argument("--date-to", metavar="YYYY-MM-DD", default=None,
                        help="End date for index download (default: today)")
    parser.add_argument("--llm", action="store_true",
                        help="Run classify_catalyst_llm after exhibits (submits batch, polls, collects)")
    parser.add_argument("--market", action="store_true",
                        help="Run fetch_market_data after classification")
    args = parser.parse_args()

    # Step 1 — download index files
    dl_args = ["scripts/download_idx.py"]
    if args.days:
        dl_args += ["--days", str(args.days)]
    else:
        dl_args += ["--date-from", args.date_from]
        if args.date_to:
            dl_args += ["--date-to", args.date_to]
    run(dl_args, "Step 1: download_idx.py")

    # Steps 2-4 — no args needed, each reads from previous step's output
    run(["scripts/parse_idx.py"],        "Step 2: parse_idx.py")
    run(["scripts/batch_filter.py"],     "Step 3: batch_filter.py")
    run(["scripts/classify_exhibits.py"], "Step 4: classify_exhibits.py")

    if args.llm:
        run(["scripts/classify_catalyst_llm.py", "--run"], "Step 5: classify_catalyst_llm.py")

    if args.market:
        run(["scripts/fetch_market_data.py"], "Step 6: fetch_market_data.py")

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
