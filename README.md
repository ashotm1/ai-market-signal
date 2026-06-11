# Small Cap Signal

Event-driven stock price prediction pipeline for small-cap stocks. Scrapes catalyst press releases from the major newswires, extracts structured features with LLM, and combines them with intraday price data to predict price continuation or reversal.

Scope: M&A, biotech, crypto treasury, collaborations, contracts, product launches, private placements.

---

## Repository layout

Run from the root with `python -m <package.module>`

| Package | Role |
|---|---|
| `ingest/` | Production scrapers + HTTP price fetch (`ingest/recon/` = dev reverse-engineering tools) |
| `sources/` | Per-source processing — `gnw/`, `prnw/`, `bw/`, `anw/` (classify → filter → extract) |
| `sec/` | SEC/EDGAR 8-K/EX-99 stream (secondary source) |
| `regex/` | Shared regex — `catalysts.py` (the catalyst recall gate) |
| `features/` | Pre-ML feature extraction — `runner.py` + `schemas/` registry |
| `market/` | Polygon price/market-data fetch |
| `ml/` | ML table builder (`ml/features.py` — market join + economic ratios + labels); model training not yet built |
| `analysis/` | One-off eval / inspection / cleanup scripts |

---

## Newswire sources

**scrape headlines/URLs → classify/filter → fetch article body**

| Source | Scrape → list | Filter | Body extractor |
|---|---|---|---|
| GlobeNewswire | `ingest/gnw_scraper.py` → `data/gnw/gnw_news.csv` | `sources/gnw/gnw_classifier.py` → `gnw_signal_filter.py` | `sources/gnw/gnw_extract_fields.py` → `data/gnw/gnw_signal_articles.csv` |
| PRNewswire | `ingest/prnw_scraper.py` → `data/prnw_monthly/` | `sources/prnw/prnw_classifier.py` (ticker ∈ universe) | `sources/prnw/prnw_extract_fields.py` → `data/prnw/articles/` |
| ACCESS Newswire | `ingest/anw_scraper.py` → `data/anw_monthly/` | post-hoc (full-run, then filter) | `sources/anw/anw_extract_fields.py` → `data/anw/articles/` |
| Business Wire | `ingest/bw_scraper.py` → `data/bw/bw_news.csv` | `sources/bw/bw_signal_filter.py` | `sources/bw/bw_extract_fields.py` → `data/bw/articles/` |


Body extraction fetches each PR page and pulls the full article body. Most sources use plain `httpx`; Business Wire is behind Akamai Bot Manager so its scraper drives a real warmed Chrome over CDP instead.

Filtering keeps rows whose ticker is in [data/ticker_universe.csv](data/ticker_universe.csv), drops law-firm / class-action litigation releases, and keeps only signal catalyst types.

---

## Feature extraction

The ML input stage turns each press-release body into typed, structured features via a **per-category schema registry** — every catalyst type has its own schema, so a private placement and a clinical readout extract different data.

- [features/base.py](features/base.py) — public engine: `FeatureSchema`/`FieldSpec` dataclasses, JSON-schema + system-prompt rendering, `register`/`get_schema` registry; `deriver` — optional per-category callable injected into the ML table builder.
- `features/schemas/` — private repo (gitignored). One module per catalyst declares its fields (types, enums, extraction rules); `private_placement.py` is the first. Adding a category is a new module + `register()` — the runner doesn't change.
- [features/runner.py](features/runner.py) — the runner (`python -m features.runner --category private_placement --run`). Filters bodies to one category and sends **one body per request** through the Anthropic Batch API (full attention per document), writing one wide, namespaced row per release to `data/features_<category>.csv`.

The schema extracts **facts, not judgments**: every field is nullable and "not stated" → null (the prompt forbids guessing — a wrong number is worse than null). Scoring and weighting are deliberately left to the downstream ML model, learned from price outcomes rather than asked of the LLM.

Catalyst tagging itself is a shared, source-agnostic regex gate (`classify_catalyst` in [regex/catalysts.py](regex/catalysts.py)) applied on the title before the expensive body fetch — tuned for recall (a missed catalyst is a permanent drop; a false positive is cheap and caught downstream).

---

## SEC EDGAR (8-K / EX-99) — secondary source

An additional catalyst stream from SEC filings. [sec/pipeline.py](sec/pipeline.py) chains the ingest stages, each append-safe; `--days` / `--date-from` / `--date-to` scope only the index download, later steps process the full accumulated set.

```bash
python -m sec.pipeline --days 30 --llm --market
```

Flow: [sec/download_idx.py](sec/download_idx.py) + [sec/parse_idx.py](sec/parse_idx.py) → `data/sec/8k.csv` → [sec/batch_filter.py](sec/batch_filter.py) (fetch filing exhibits) → `data/sec/8k_ex99.csv` → [sec/classify_exhibits.py](sec/classify_exhibits.py) (heuristic + regex catalyst) → `data/sec/ex_99_classified.csv` → [sec/classify_catalyst_llm.py](sec/classify_catalyst_llm.py) *(`--llm`, Haiku reclass of `catalyst=other`)* → [market/fetch_market_data.py](market/fetch_market_data.py) *(`--market`, Polygon prices + `<$500M` cap filter)*. This stream is a cross-check / coverage backstop — an 8-K confirms a release was material enough to file — rather than the primary feature feed, which comes from the newswire bodies above.

---

## ML

- Scope: <$500M point-in-time market cap, signal catalyst, price up >=10% within first 5m of news
- Decision time `t` = the moment the stock crosses +10%, not news time
- Target: quantile regression (P10/P50/P90) on log returns at multiple horizons, market-residualized at 1d
- Features: pre-news technical state + lookback news history + current-PR extracted features (~40 cols)
- Validation: walk-forward only

Layer 1 hard filters (no model): drop dilutive offerings, restatements, excluded catalyst types. Layer 2 XGBoost runs only on what passes.

---

## Requirements

- `MASSIVE_API_KEY` — Polygon.io price data
- `ANTHROPIC_API_KEY` — LLM classification + feature extraction
- `SEC_USER_AGENT` — SEC EDGAR fair-access (e.g. `"Name email@example.com"`)
- Business Wire extraction needs a real Chrome (CDP) — see [ingest/bw_scraper.py](ingest/bw_scraper.py) for the warmed-profile setup
