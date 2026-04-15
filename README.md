# AI Market Signal

Event-driven trading signal pipeline for small-cap stocks. Detects and classifies SEC 8-K press releases, extracts structured features via LLM, and pairs them with intraday price reactions for ML training.

Scope is non-earnings catalyst events — M&A, clinical readouts, crypto treasury, collaborations, contract wins, product launches, etc.

---

## Pipeline

```bash
python pipeline.py --days 30 --llm --prices
```

**Steps:**
1. `download_idx.py` — download EDGAR daily index files
2. `parse_idx.py` — extract 8-K rows → `data/8k.csv`
3. `batch_filter.py` — collect EX-99 exhibit URLs → `data/8k_ex99.csv`
4. `regex_classifier.py` — classify exhibits as PRs, tag catalyst → `data/ex_99_classified.csv`
5. `llm_classifier.py` — Haiku batch API refines unclassified rows (optional `--llm`)
6. `extract_features.py` — LLM feature extraction on signal PRs → `data/pr_features.csv`
7. `fetch_prices.py` — Polygon price data for signal tickers → `data/price_bars.csv` etc.

**Signal catalysts:** `clinical`, `private_placement`, `collaboration`, `m&a`, `new_product`, `contract`, `crypto_treasury`

---

## Live Sentiment UI *(demo)*

Quick headline sentiment demo — scrapes stock headlines and runs sentiment on titles. Separate from the main pipeline.

- **Models**: FinBERT (local), GPT-4o Mini, Claude Haiku
- Stack: FastAPI + Flask + simple UI

---

## Requirements

- `ANTHROPIC_API_KEY` — LLM classification + feature extraction
- `MASSIVE_API_KEY` or `POLYGON_API_KEY` — Polygon.io price data
- `SEC_USER_AGENT` — SEC EDGAR fair-access policy (e.g. `"Name email@example.com"`)
- `OPENAI_API_KEY` — only if using GPT model in UI
