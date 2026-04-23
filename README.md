# AI Market Signal

Event-driven trading signal pipeline for small-cap stocks. Detects and classifies SEC 8-K press releases, extracts structured features via LLM, and pairs them with intraday price reactions for ML training.

Scope is non-earnings catalyst events — M&A, biotech, crypto treasury, collaborations, contracts, product launches, etc.

---

## Data Pipeline

Downloads EDGAR 8-K filings, identifies press releases, classifies them by catalyst type, extracts structured LLM features, and fetches corresponding intraday price data from Polygon.

Classification works in two passes — a fast regex pass on the PR title, followed by an optional LLM batch pass for anything the regex didn't catch.

The `--days` / `--date-from` / `--date-to` flags apply only to the index download step. All subsequent steps process the full accumulated dataset, skipping rows already handled in prior runs.

```bash
python pipeline.py --days 30 --llm --market
python pipeline.py --date-from 2024-01-01 --date-to 2024-12-31
```

---

## Sentiment UI *(demo)*

Quick headline sentiment demo — scrapes stock headlines and runs sentiment on titles. Runs locally, separate from the main pipeline.

- **Models**: FinBERT (local), GPT-4o Mini, Claude Haiku
- Stack: FastAPI + Flask + simple UI

---

## Requirements

- `ANTHROPIC_API_KEY` — LLM classification + feature extraction
- `MASSIVE_API_KEY` or `POLYGON_API_KEY` — Polygon.io price data
- `SEC_USER_AGENT` — SEC EDGAR fair-access policy (e.g. `"Name email@example.com"`)
- `OPENAI_API_KEY` — only if using GPT model in UI
