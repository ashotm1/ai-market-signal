"""
extract_features.py — Extract structured features from press release HTML using LLM.

For each confirmed PR (is_pr=True in data/ex_99_classified.csv), fetches the first 500
words of the EX-99 exhibit and extracts structured features via Claude Haiku.

Output fields:
  catalyst_type     : offering | acquisition | clinical | partnership |
                      contract | product_launch | financial | guidance |
                      buyback | dividend | other
  dollar_amount     : largest deal/transaction figure in millions (float or null)
  has_named_partner : bool — specific external company/institution named
  commitment_level  : 1-10 — speculative language (1) vs binding/done-deal (10)
  significance_score: 1-10 — actual news weight, substance over language
  is_dilutive       : bool — explicit offering/placement/warrant language present
  earnings_guidance : pos | neg | reaffirmed | null
  milestone_guidance: bool — forward milestone projected (FDA filing, close by Q3, etc.)
  has_revenue       : bool
  revenue_figure    : float (millions) or null
  has_growth_pct    : bool
  growth_pct        : float or null
  sentiment         : positive | negative | neutral
  llm_notes         : list of strings — anything notable not captured above

Output: data/pr_features.csv
Append-safe: skips ex99_urls already in output.

Usage:
  python extract_features.py                  # real-time mode (~10 min, 50 RPM)
  python extract_features.py --submit-batch   # submit batch job and exit (~seconds)
  python extract_features.py --collect-batch  # check status and collect results when ready
"""
import argparse
import asyncio
import json
import os
import re
import time

import httpx
import pandas as pd
from anthropic import AsyncAnthropic, Anthropic

from edgar import fetch_html

# ── Config ─────────────────────────────────────────────────────────────────────
INPUT_CSV = "data/ex_99_classified.csv"
OUTPUT_CSV = "data/pr_features.csv"
BATCH_STATE_FILE = "data/pr_features_batch.json"
BATCH_SIZE = 10
BATCH_INTERVAL = 1.0
LLM_INTERVAL = 1.2
WORDS_TO_CONSUME = 500
MODEL = "claude-haiku-4-5-20251001"

CATALYST_TYPES = [
    "offering", "acquisition", "clinical", "partnership", "contract",
    "product_launch", "financial", "guidance", "buyback", "dividend", "legal",
    "other",
]

_anthropic_async = AsyncAnthropic()
_anthropic_sync  = Anthropic()

_SYSTEM_PROMPT = f"""You are a financial analyst extracting structured data from press release excerpts.
Return ONLY a valid JSON object with exactly these fields — no explanation, no markdown:

{{
  "catalyst_type": one of {json.dumps(CATALYST_TYPES)},
  "dollar_amount": largest deal/transaction figure in millions as float, or null,
  "has_named_partner": true/false — is a specific external company or institution named,
  "commitment_level": integer 1-10 — 1=speculative/exploratory, 10=binding/already closed,
  "significance_score": integer 1-10 — actual news weight based on substance not language,
  "is_dilutive": true/false — only if PR explicitly mentions offering, placement, or warrant issuance,
  "earnings_guidance": "pos", "neg", "reaffirmed", or null — only for financial outlook statements,
  "milestone_guidance": true/false — forward milestone projected (e.g. expects FDA filing by Q2),
  "has_revenue": true/false,
  "revenue_figure": revenue figure in millions as float, or null,
  "has_growth_pct": true/false,
  "growth_pct": explicit growth percentage as float, or null,
  "sentiment": "positive", "negative", or "neutral",
  "llm_notes": list of strings — any notable observations not captured above, empty list if none
}}"""

_FEATURE_KEYS = [
    "catalyst_type", "dollar_amount", "has_named_partner",
    "commitment_level", "significance_score", "is_dilutive",
    "earnings_guidance", "milestone_guidance",
    "has_revenue", "revenue_figure", "has_growth_pct", "growth_pct",
    "sentiment", "llm_notes",
]

_NULL_FEATURES = {k: None for k in _FEATURE_KEYS}


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _extract_text(html: str, max_words: int = WORDS_TO_CONSUME) -> str:
    """Strip HTML and return first max_words words, skipping XBRL metadata tokens."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    text = " ".join(soup.stripped_strings)
    text = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", text)
    words = text.split()
    skip = re.compile(r"^(EX-\d+\.\d+|Exhibit|\S+\.html?|\d+\.\d+|\d+)$", re.IGNORECASE)
    while words and skip.match(words[0]):
        words.pop(0)
    return " ".join(words[:max_words])


def _parse_llm_response(raw: str) -> dict:
    """Parse JSON from LLM response, serializing llm_notes for CSV storage."""
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    data = json.loads(raw)
    if "llm_notes" in data and isinstance(data["llm_notes"], list):
        data["llm_notes"] = json.dumps(data["llm_notes"])
    return {k: data.get(k) for k in _FEATURE_KEYS}


def _load_pending(done_urls: set) -> pd.DataFrame:
    df = pd.read_csv(INPUT_CSV)
    df = df[df["is_pr"] == True].reset_index(drop=True)
    print(f"Loaded {len(df)} confirmed PRs from {INPUT_CSV}")
    df = df[~df["ex99_url"].isin(done_urls)].reset_index(drop=True)
    print(f"  {len(df)} pending after skipping {len(done_urls)} already processed")
    return df


def _done_urls() -> set:
    if os.path.exists(OUTPUT_CSV):
        existing = pd.read_csv(OUTPUT_CSV, usecols=["ex99_url"])
        return set(existing["ex99_url"])
    return set()


# ── Real-time mode ─────────────────────────────────────────────────────────────

async def _call_llm(excerpt: str) -> dict:
    try:
        msg = await _anthropic_async.messages.create(
            model=MODEL,
            max_tokens=512,
            temperature=0,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": excerpt}],
        )
        return _parse_llm_response(msg.content[0].text.strip())
    except Exception as exc:
        print(f"  LLM error: {exc}", flush=True)
        return _NULL_FEATURES.copy()


async def _run_realtime(df: pd.DataFrame):
    write_header = not os.path.exists(OUTPUT_CSV)

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            pending = list(batch.itertuples(index=False))

            print(f"\n=== BATCH {batch_num} ({len(pending)} PRs) ===", flush=True)
            t_batch = time.monotonic()

            # Fetch HTML concurrently at SEC rate (10 req/s)
            htmls = await asyncio.gather(*[
                fetch_html(client, row.ex99_url) for row in pending
            ])

            # LLM calls sequentially to respect rate limit (~50 RPM)
            results = []
            for row, html in zip(pending, htmls):
                row_dict = row._asdict()
                if html is None:
                    print(f"  fetch failed  | {row.company}", flush=True)
                    results.append({**row_dict, **_NULL_FEATURES})
                    continue

                excerpt = _extract_text(html)
                if not excerpt:
                    print(f"  empty text    | {row.company}", flush=True)
                    results.append({**row_dict, **_NULL_FEATURES})
                    continue

                t_llm = time.monotonic()
                features = await _call_llm(excerpt)
                elapsed_llm = time.monotonic() - t_llm

                print(
                    f"  {row.company[:38]:38s}"
                    f" | {str(features.get('catalyst_type', '')):14s}"
                    f" | sig={features.get('significance_score', '?')}"
                    f"  com={features.get('commitment_level', '?')}",
                    flush=True,
                )
                results.append({**row_dict, **features})

                remaining_llm = LLM_INTERVAL - elapsed_llm
                if remaining_llm > 0:
                    await asyncio.sleep(remaining_llm)

            pd.DataFrame(results).to_csv(
                OUTPUT_CSV, mode="a", header=write_header, index=False
            )
            write_header = False

            elapsed_batch = time.monotonic() - t_batch
            remaining_batch = BATCH_INTERVAL - elapsed_batch
            if remaining_batch > 0 and i + BATCH_SIZE < len(df):
                await asyncio.sleep(remaining_batch)


# ── Batch submit mode ──────────────────────────────────────────────────────────

async def _fetch_all_excerpts(df: pd.DataFrame) -> dict:
    """Fetch HTML for all rows and return {custom_id: excerpt}."""
    excerpts = {}
    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i:i + BATCH_SIZE]
            t_start = time.monotonic()
            htmls = await asyncio.gather(*[
                fetch_html(client, row["ex99_url"]) for _, row in batch.iterrows()
            ])
            for (_, row), html in zip(batch.iterrows(), htmls):
                custom_id = str(row.name)  # dataframe index as string
                if html is None:
                    print(f"  fetch failed  | {row['company']}", flush=True)
                    excerpts[custom_id] = None
                else:
                    text = _extract_text(html)
                    excerpts[custom_id] = text if text else None
                    print(f"  fetched       | {row['company']}", flush=True)

            elapsed = time.monotonic() - t_start
            remaining = BATCH_INTERVAL - elapsed
            if remaining > 0 and i + BATCH_SIZE < len(df):
                await asyncio.sleep(remaining)

    return excerpts


def _submit_batch(df: pd.DataFrame, excerpts: dict) -> str:
    """Submit all requests to Anthropic Batch API. Returns batch_id."""
    requests = []
    for _, row in df.iterrows():
        custom_id = str(row.name)
        excerpt = excerpts.get(custom_id)
        if not excerpt:
            continue
        requests.append({
            "custom_id": custom_id,
            "params": {
                "model": MODEL,
                "max_tokens": 512,
                "temperature": 0,
                "system": [
                    {
                        "type": "text",
                        "text": _SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                "messages": [{"role": "user", "content": excerpt}],
            },
        })

    print(f"\nSubmitting {len(requests)} requests to Anthropic Batch API...", flush=True)
    batch = _anthropic_sync.messages.batches.create(requests=requests)
    print(f"  Batch ID: {batch.id}", flush=True)
    print(f"  Status:   {batch.processing_status}", flush=True)
    return batch.id


async def run_submit_batch():
    df = _load_pending(_done_urls())
    if df.empty:
        print("Nothing to process.")
        return

    if os.path.exists(BATCH_STATE_FILE):
        print(f"  State file already exists at {BATCH_STATE_FILE}")
        print("  Run --collect-batch to collect results, or delete the state file to resubmit.")
        return

    print(f"\nFetching HTML from SEC ({len(df)} PRs)...")
    excerpts = await _fetch_all_excerpts(df)

    batch_id = _submit_batch(df, excerpts)

    # Save state: batch_id + row data keyed by custom_id
    state = {
        "batch_id": batch_id,
        "rows": {str(row.name): row.to_dict() for _, row in df.iterrows()},
    }
    with open(BATCH_STATE_FILE, "w") as f:
        json.dump(state, f)

    print(f"\nState saved to {BATCH_STATE_FILE}")
    print("Run --collect-batch when ready to collect results.")


# ── Batch collect mode ─────────────────────────────────────────────────────────

def run_status():
    if not os.path.exists(BATCH_STATE_FILE):
        print(f"No batch state file found at {BATCH_STATE_FILE}. Run --submit-batch first.")
        return

    with open(BATCH_STATE_FILE) as f:
        state = json.load(f)

    batch = _anthropic_sync.messages.batches.retrieve(state["batch_id"])
    c = batch.request_counts
    total = c.processing + c.succeeded + c.errored + c.canceled + c.expired
    done = c.succeeded + c.errored + c.canceled + c.expired
    pct = round(done / total * 100) if total else 0

    print(f"Batch {batch.id}")
    print(f"  Status:     {batch.processing_status}")
    print(f"  Progress:   {done}/{total} ({pct}%)")
    print(f"  Succeeded:  {c.succeeded}")
    print(f"  Processing: {c.processing}")
    print(f"  Errored:    {c.errored}")
    print(f"  Canceled:   {c.canceled}")
    print(f"  Expired:    {c.expired}")


def run_collect_batch():
    if not os.path.exists(BATCH_STATE_FILE):
        print(f"No batch state file found at {BATCH_STATE_FILE}. Run --submit-batch first.")
        return

    with open(BATCH_STATE_FILE) as f:
        state = json.load(f)

    batch_id = state["batch_id"]
    rows = state["rows"]

    batch = _anthropic_sync.messages.batches.retrieve(batch_id)
    print(f"Batch {batch_id}: {batch.processing_status}")
    print(f"  Request counts: {batch.request_counts}")

    if batch.processing_status != "ended":
        print("Not ready yet. Try again later.")
        return

    # Collect results
    write_header = not os.path.exists(OUTPUT_CSV)
    results = []
    succeeded = failed = 0

    for result in _anthropic_sync.messages.batches.results(batch_id):
        custom_id = result.custom_id
        row_data = rows.get(custom_id, {})

        if result.result.type == "succeeded":
            try:
                features = _parse_llm_response(result.result.message.content[0].text.strip())
                succeeded += 1
                print(
                    f"  {str(row_data.get('company', ''))[:38]:38s}"
                    f" | {str(features.get('catalyst_type', '')):14s}"
                    f" | sig={features.get('significance_score', '?')}"
                    f"  com={features.get('commitment_level', '?')}",
                    flush=True,
                )
            except Exception as exc:
                print(f"  parse error [{custom_id}]: {exc}", flush=True)
                features = _NULL_FEATURES.copy()
                failed += 1
        else:
            print(f"  failed [{custom_id}]: {result.result.type}", flush=True)
            features = _NULL_FEATURES.copy()
            failed += 1

        results.append({**row_data, **features})

    if results:
        pd.DataFrame(results).to_csv(
            OUTPUT_CSV, mode="a", header=write_header, index=False
        )

    print(f"\nDone. {succeeded} succeeded, {failed} failed.")
    print(f"Results saved to {OUTPUT_CSV}")

    os.remove(BATCH_STATE_FILE)
    print(f"State file removed.")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--submit-batch", action="store_true",
                        help="Fetch HTML, submit batch job to Anthropic, and exit")
    parser.add_argument("--collect-batch", action="store_true",
                        help="Check batch status and collect results when ready")
    parser.add_argument("--status", action="store_true",
                        help="Show live progress of the current batch")
    args = parser.parse_args()

    if args.submit_batch:
        asyncio.run(run_submit_batch())
    elif args.collect_batch:
        run_collect_batch()
    elif args.status:
        run_status()
    else:
        df = _load_pending(_done_urls())
        if df.empty:
            print("Nothing to process.")
            return
        asyncio.run(_run_realtime(df))
        print(f"\nDone. Features saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
