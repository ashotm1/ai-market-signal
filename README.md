# Stock PR Sentiment Analyzer

Personal research project for building a trading signal pipeline around small cap stocks. Currently contains a live news sentiment UI (demo) and a SEC EDGAR pipeline to scrape news release data.

---

## What's in here

### Live Sentiment UI
Scrapes stock headlines from StockTitan and runs sentiment on the titles using your choice of model.

- **Models**: FinBERT (local), GPT-4o Mini, Claude Haiku
- Body analysis is planned but not done yet — just headlines for now
- Stack: FastAPI + Flask (FinBERT service) + simple UI

### EDGAR PR Pipeline (`scripts/`)
Pulls historical 8-K filings from SEC EDGAR, detects which exhibits are actual press releases, and extracts features for ML training.

**Steps:**
1. `download_idx.py` — downloads EDGAR quarterly index files
2. `parse_idx.py` — parses index files, extracts 8-K rows → `parsed/8k.csv`
3. `batch_filter.py` — fetches filing index pages, collects EX-99 URLs + acceptance timestamps + 8-K item numbers → `parsed/8k_ex99.csv`
4. `pr_classifier.py` — fetches each EX-99 exhibit, classifies as PR or not → `parsed/prs.csv`
5. `fetch_prices.py` — resolves tickers via SEC submissions API, fetches 1-min OHLCV bars from Massive/Polygon, computes % price changes at T+5m, T+30m, T+1h, T+4h, T+1d → `parsed/price_data.csv`

**Classifier (`classifier.py`):**

Heuristics run first, LLM only called for weak signals:

| Signal | Description | Action |
|--------|-------------|--------|
| H1 | Investor/media contact block | Trust directly |
| H2 | Wire service name (BusinessWire, PRNewswire, GlobeNewswire) | Trust directly |
| H3 | Explicit PR header (press release, for immediate release) | Trust directly |
| H5+H7 | Dateline + exchange ticker | Trust directly |
| H6+H7 | PR verb + exchange ticker | Trust directly |
| H7 alone | Ticker only | LLM verify |
| combined (H5+H6) | Dateline + PR verb, no ticker | LLM verify |

H4 (`("the Company")`) is tracked in output CSVs for historical compatibility but not used in classification — too broad, fires on non-PR legal exhibits.

**Goal:** Extract structured features (commitment level, specificity, hype, credibility) and pair with price data to train an XGBoost model. Design notes in `notes/ml_pipeline_notes.txt`.

---

## Flow

```
Live:   StockTitan → scraper → AI model → sentiment scores → UI

EDGAR:  SEC index → parse_idx → batch_filter → pr_classifier → features → XGBoost
```

---

## Running it

**FinBERT service** (only needed if using FinBERT):
```
python finbert_service/server.py
```

**Web UI:**
```
uvicorn api.main:app --reload
```

**EDGAR pipeline:**
```
python scripts/download_idx.py
python scripts/parse_idx.py
python scripts/batch_filter.py
python scripts/pr_classifier.py
python scripts/fetch_prices.py
```

**Utilities (`utils/`):**
```
python utils/stats.py                                          # heuristic fire rates on prs.csv
python utils/validate_combined.py --input parsed/prs.csv      # re-validate weak-signal PRs with LLM
python utils/inspect_excerpts.py                               # debug LLM input excerpts
```

---

## Notes

- Needs `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` for those models
- Needs `MASSIVE_API_KEY` or `POLYGON_API_KEY` for price data (Massive/Polygon API)
- FinBERT runs locally via Hugging Face (`ProsusAI/finbert`)
- SEC rate limit is 10 req/s — pipeline handles this
- ML pipeline design notes in `notes/ml_pipeline_notes.txt`
