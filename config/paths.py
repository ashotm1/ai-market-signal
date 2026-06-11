"""
config/paths.py — single source of truth for all data file paths.

To restructure: change the DIR constants here; nothing else needs touching.
Scripts import the named constants — no hardcoded strings anywhere else.
"""
import os

DATA = "data"

# ── SEC ───────────────────────────────────────────────────────────────────────
SEC_DIR        = f"{DATA}/sec"
SEC_8K         = f"{SEC_DIR}/8k.csv"
SEC_8K_PARQUET = f"{SEC_DIR}/8k.parquet"
SEC_8K_EX99    = f"{SEC_DIR}/8k_ex99.csv"
SEC_CLASSIFIED = f"{SEC_DIR}/ex_99_classified.csv"
SEC_CIK_CACHE  = f"{SEC_DIR}/cik_tickers.json"

# ── Business Wire ─────────────────────────────────────────────────────────────
BW_DIR          = f"{DATA}/bw"
BW_NEWS         = f"{BW_DIR}/bw_news.csv"
BW_RUNS         = f"{BW_DIR}/bw_runs.csv"
BW_RANGES       = f"{BW_DIR}/bw_worker_ranges.csv"
BW_SIGNAL       = f"{BW_DIR}/bw_signal_filtered.csv"
BW_ARTICLES_DIR = f"{BW_DIR}/articles"

# ── GlobeNewswire ─────────────────────────────────────────────────────────────
GNW_DIR      = f"{DATA}/gnw"
GNW_NEWS     = f"{GNW_DIR}/gnw_news.csv"
GNW_CLASSIFIED = f"{GNW_DIR}/gnw_classified.csv"
GNW_SIGNAL   = f"{GNW_DIR}/gnw_signal_filtered.csv"
GNW_ARTICLES = f"{GNW_DIR}/gnw_signal_articles.csv"

# ── ACCESS Newswire ───────────────────────────────────────────────────────────
ANW_MONTHLY_DIR  = f"{DATA}/anw_monthly"
ANW_MONTHLY_DONE = f"{ANW_MONTHLY_DIR}/sitemap_done.txt"
ANW_DIR          = f"{DATA}/anw"
ANW_SIGNAL       = f"{ANW_DIR}/anw_signal_filtered.csv"
ANW_ARTICLES     = f"{ANW_DIR}/anw_signal_articles.csv"
ANW_ARTICLES_DIR = f"{ANW_DIR}/articles"

# ── PRNewswire ────────────────────────────────────────────────────────────────
PRNW_MONTHLY_DIR  = f"{DATA}/prnw_monthly"
PRNW_MONTHLY_DONE = f"{PRNW_MONTHLY_DIR}/gz_done.txt"
PRNW_DIR          = f"{DATA}/prnw"
PRNW_CLASSIFIED   = f"{PRNW_DIR}/prnw_classified.csv"
PRNW_SIGNAL       = f"{PRNW_DIR}/prnw_signal_filtered.csv"
PRNW_ARTICLES_DIR = f"{PRNW_DIR}/articles"

# ── StockTitan ────────────────────────────────────────────────────────────────
ST_DIR    = f"{DATA}/stocktitan"
ST_NEWS   = f"{ST_DIR}/stocktitan_news.csv"
ST_SIGNAL = f"{ST_DIR}/stocktitan_news_filtered.csv"

# ── Prices ────────────────────────────────────────────────────────────────────
PRICES_DIR     = f"{DATA}/prices"
PRICES_SEC     = f"{PRICES_DIR}/sec_price_data.csv"   # legacy
PRICES_BW      = f"{PRICES_DIR}/bw_price_data.csv"    # legacy
PRICES_ST      = f"{PRICES_DIR}/st_price_data.csv"    # legacy
PRICE_BARS     = f"{PRICES_DIR}/price_bars.csv"        # legacy
DAILY_BARS     = f"{PRICES_DIR}/daily_bars.csv"        # legacy
BARS_1MIN      = f"{PRICES_DIR}/1min_bars.csv"
BARS_10MIN     = f"{PRICES_DIR}/10min_bars.csv"
BARS_DAILY_NW  = f"{PRICES_DIR}/daily_bars_nw.csv"
TICKER_DETAILS = f"{PRICES_DIR}/ticker_details.csv"

# ── Shared reference data ─────────────────────────────────────────────────────
TICKER_UNIVERSE = f"{DATA}/ticker_universe.csv"

# ── Features (category-parameterised) ────────────────────────────────────────
def features_csv(category: str) -> str:
    return f"{DATA}/features_{category}.csv"

def features_batch_json(category: str) -> str:
    return f"{DATA}/features_{category}_batch.json"

def ml_csv(category: str) -> str:
    return f"{DATA}/ml_{category}.csv"

# ── SEC pipeline state ────────────────────────────────────────────────────────
LLM_BATCH_STATE = f"{DATA}/llm_classifier_batch.json"
LLM_MALFORMED   = f"{DATA}/llm_malformed.csv"
BACKUP_DATA     = f"{DATA}/backup_data.csv"

# ── Eval / analysis artifacts ─────────────────────────────────────────────────
SAMPLE_200             = f"{DATA}/sample_200.csv"
SAMPLE_HAIKU           = f"{DATA}/sample_haiku.csv"
SAMPLE_SONNET          = f"{DATA}/sample_sonnet.csv"
COMPARE_SAMPLE         = f"{DATA}/compare_sample.csv"
COMPARE_RESULTS        = f"{DATA}/compare_results.csv"
COMBINED_DISAGREEMENTS = f"{DATA}/combined_disagreements.csv"
TITLE_EXTRACT_NONPR    = f"{DATA}/title_extract_nonpr.csv"
TITLE_EXTRACT_STATE    = f"{DATA}/title_extract_nonpr_state.json"

# ── All dirs that must exist before any script writes ─────────────────────────
ALL_DIRS = [
    SEC_DIR,
    BW_DIR, BW_ARTICLES_DIR,
    GNW_DIR,
    ANW_MONTHLY_DIR, ANW_DIR, ANW_ARTICLES_DIR,
    PRNW_MONTHLY_DIR, PRNW_DIR, PRNW_ARTICLES_DIR,
    ST_DIR,
    PRICES_DIR,
]


def ensure_dirs() -> None:
    """Create all data subdirectories. Call once at project setup or from any writer."""
    for d in ALL_DIRS:
        os.makedirs(d, exist_ok=True)
