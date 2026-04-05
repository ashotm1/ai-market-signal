# Stock PR Sentiment Analyzer

Personal research project for building a trading signal pipeline around small cap stocks. Two things going on: a live news sentiment UI (mostly a demo for now) and a historical SEC EDGAR pipeline that's the real meat of it.

---

## What's in here

### Live Sentiment UI
Scrapes stock headlines from StockTitan and runs sentiment on the titles using your choice of model.

- **Models**: FinBERT (local), GPT-4o Mini, Claude Haiku
- Body analysis is planned but not done yet — just headlines for now
- Stack: FastAPI + Flask (FinBERT service) + simple UI

### EDGAR PR Pipeline (`scripts/`)
Pulls historical 8-K filings from SEC EDGAR, detects which exhibits are actual press releases, and extracts features for ML training.

- Downloads EDGAR index files, parses 8-K rows
- Fetches EX-99.x exhibits and classifies them as PR or not (heuristics first, LLM fallback)
- Goal: extract structured features (commitment level, specificity, hype, credibility) and pair with price data to train an XGBoost model

---

## Flow

```
Live:   StockTitan → scraper → AI model → sentiment scores → UI

EDGAR:  SEC index → parse → batch_filter → classifier → features → XGBoost
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
```

---

## Notes

- Needs `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` for those models
- FinBERT runs locally via Hugging Face (`ProsusAI/finbert`)
- SEC rate limit is 10 req/s — pipeline handles this
- ML pipeline design notes in `notes/ml_pipeline_notes.txt`
