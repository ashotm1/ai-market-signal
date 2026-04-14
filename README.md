# AI Market Signal

Event-driven trading signal pipeline for small-cap stocks. Detects and classifies SEC 8-K press releases, extracts structured features via LLM, and pairs them with intraday price reactions for ML training.

Scope is non-earnings catalyst events only — M&A, clinical readouts, offerings, product launches, etc. Pure earnings releases are filtered out at the classification stage.

---

## EDGAR PR Pipeline (`scripts/`)

**Steps:**
1. `download_idx.py` — downloads EDGAR daily index files (async, 10 req/s)
2. `parse_idx.py` — parses index files, extracts 8-K rows → `data/8k.csv` (append-safe)
3. `batch_filter.py` — fetches filing index pages, collects EX-99 exhibit URLs + acceptance timestamps + 8-K item numbers → `data/8k_ex99.csv`
4. `pr_classifier.py` — fetches each EX-99, classifies as PR or not via heuristics + LLM fallback, tags catalyst type → `data/ex_99_classified.csv`. Skips pure earnings filings (item 2.02 only).
5. `extract_features.py` — LLM feature extraction on first 500 words of each confirmed PR → `data/pr_features.csv`. Supports real-time and batch API mode.
6. `fetch_prices.py` — resolves tickers via SEC submissions API, fetches price data from Polygon → `data/price_bars.csv`, `data/daily_bars.csv`, `data/ticker_details.csv`, `data/price_data.csv`

**Classifier heuristics (H1–H6):**

| Signal | Description | Trust |
|--------|-------------|-------|
| H1 | Investor/media contact block in last 200 words | Direct |
| H2 | Wire service name in first 200 words (BusinessWire, PRNewswire, GlobeNewswire, etc.) | Direct |
| H3 | Explicit PR header in first 40 words ("press release", "for immediate release") | Direct |
| H4+H6 | Standalone date + exchange ticker | Direct |
| H5+H6 | PR action verb + exchange ticker | Direct |
| H6 alone | Exchange ticker only | LLM verify |
| H4+H5 | Date + PR verb, no ticker | LLM verify |

**Catalyst types** (tagged from PR title, multiple allowed):
`offering`, `m&a`, `clinical`, `private_placement`, `new_product`, `split`, `dividend`, `nasdaq_alert`, `personnel`, `agreement`, `earnings`, `other`

**LLM features extracted per PR:**
commitment level, significance score, catalyst type, dollar amount, named partner, dilution flag, earnings/milestone guidance, revenue figures, sentiment

---

## Live Sentiment UI *(demo)*

Quick headline sentiment demo — scrapes stock headlines from StockTitan and runs sentiment on titles. Separate from the main pipeline, not representative of the full LLM feature extraction.

- **Models**: FinBERT (local), GPT-4o Mini, Claude Haiku
- Stack: FastAPI + Flask (FinBERT service) + simple UI

---

## Running it

**EDGAR pipeline:**
```
python scripts/download_idx.py --days 30
python scripts/parse_idx.py
python scripts/batch_filter.py
python scripts/pr_classifier.py
python scripts/extract_features.py
python scripts/fetch_prices.py
```

**Web UI:**
```
uvicorn api.main:app --reload
python finbert_service/server.py   # only if using FinBERT
```

**Utilities (`utils/`):**
```
python utils/stats.py
python utils/validate_combined.py
python utils/inspect_excerpts.py
```

---

## Requirements

- `ANTHROPIC_API_KEY` — classifier LLM fallback + feature extraction
- `MASSIVE_API_KEY` or `POLYGON_API_KEY` — price data
- `SEC_USER_AGENT` — required by SEC EDGAR fair-access policy (e.g. `"Name email@example.com"`)
- `OPENAI_API_KEY` — only if using GPT model in UI
