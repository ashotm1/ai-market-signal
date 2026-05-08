"""
prn_classifier.py — Classify PRN CSV rows: title, company, ticker, exchange, catalyst tags.

Pipeline (cheapest first):
  url    → title (strip slug)
  title  → catalyst tags (delegated to pr_detection.classify_catalyst)
  title  → company name guess (text before first PR action verb)
  name   → (ticker, exchange) via ticker_details.csv lookup
"""
import csv
import re
import urllib.parse

from pr_detection import classify_catalyst

_URL_TAIL = re.compile(r"-(\d+)\.html?$", re.IGNORECASE)

# Verbs that follow the company name in PR headlines.
_TITLE_VERB = re.compile(
    r"\b(announces?|reports?|launches?|introduces?|completes?|acquires?|appoints?|"
    r"names?|elects?|provides?|releases?|enters?|expands?|files?|receives?|wins?|"
    r"awards?|signs?|secures?|achieves?|unveils?|delivers?|posts?|to\s+acquire|"
    r"to\s+merge|to\s+present|to\s+report|sets?\b|raises?|priced?\b|closes?\b)\b",
    re.IGNORECASE,
)

# Legal-entity suffixes only — do not strip industry words like "Therapeutics".
_LEGAL_SUFFIX = re.compile(
    r"\b(?:common\s+stock|class\s+[a-z](?:\s+common\s+stock)?|"
    r"inc|corp|corporation|company|co|ltd|limited|llc|plc|sa|nv|ag|holdings?)\b\.?",
    re.IGNORECASE,
)
_PUNCT = re.compile(r"[^\w\s]")


def title_from_url(url: str) -> str | None:
    """Convert PRN URL slug to a headline-like string. None if unparseable."""
    path = urllib.parse.urlparse(url).path
    slug = path.rstrip("/").rsplit("/", 1)[-1]
    slug = _URL_TAIL.sub("", slug)
    if not slug or slug == path.rstrip("/"):
        return None
    return slug.replace("-", " ")


def company_from_title(title: str) -> str | None:
    """Return text before the first PR action verb, or None."""
    if not title:
        return None
    m = _TITLE_VERB.search(title)
    if not m:
        return None
    name = title[: m.start()].strip().rstrip(",")
    return name or None


def _normalize(name: str) -> str:
    """Lowercase, strip punctuation and legal-entity suffixes for matching."""
    name = name.lower()
    name = _LEGAL_SUFFIX.sub(" ", name)
    name = _PUNCT.sub(" ", name)
    return " ".join(name.split())


def build_ticker_index(ticker_details_path: str) -> dict[str, tuple[str, str]]:
    """Read ticker_details.csv → {normalized_name: (ticker, exchange)}.

    On duplicate names the first row wins (CSV is sorted by ticker, so this
    is deterministic; fine-tune later if collisions matter).
    """
    index: dict[str, tuple[str, str]] = {}
    with open(ticker_details_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = _normalize(row.get("name", ""))
            if key and key not in index:
                index[key] = (row["ticker"], row.get("primary_exchange", ""))
    return index


def lookup_ticker(name: str, index: dict[str, tuple[str, str]]) -> tuple[str, str] | None:
    """Lookup name → (ticker, exchange). Tries exact then prefix match."""
    if not name:
        return None
    key = _normalize(name)
    if not key:
        return None
    if key in index:
        return index[key]
    prefix = key + " "
    for ix_key, val in index.items():
        if ix_key.startswith(prefix):
            return val
    return None


def classify_row(url: str, ticker_index: dict[str, tuple[str, str]]) -> dict:
    """Full classify pipeline for one PRN URL."""
    title = title_from_url(url)
    name = company_from_title(title) if title else None
    hit = lookup_ticker(name, ticker_index) if name else None
    return {
        "title": title,
        "company": name,
        "tags": classify_catalyst(title) if title else ["other"],
        "ticker": hit[0] if hit else None,
        "exchange": hit[1] if hit else None,
    }
