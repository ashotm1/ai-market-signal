"""
classifier.py — Press release classification logic.
Provides heuristic and LLM-based classifiers for SEC EX-99.x exhibits.
"""
import anthropic
from bs4 import BeautifulSoup
import re

_anthropic_client = anthropic.AsyncAnthropic()

# --- Compiled regex patterns ---

# H1: investor relations / media contact block (checked in last 200 words)
# e.g. "Investor Contact: Denise Barr", "Media Relations:", "Contact:"
_CONTACT_BLOCK = re.compile(
    r"\b(?:investor\s+|media\s+)?(?:contact|relations):\s",
    re.IGNORECASE,
)

# H2: wire service name anywhere in first 200 words — covers datelines and bylines
# e.g. "BOSTON--(BUSINESS WIRE)", "/PRNewswire/", "Source: GlobeNewswire"
# Combines what were two separate checks (regex + list) into one pattern.
_WIRE_SERVICE = re.compile(
    r"Business\s*Wire|PR\s*Newswire|GlobeNewswire",
    re.IGNORECASE,
)

# H3: explicit PR header phrases
_PR_HEADERS = re.compile(
    r"for immediate release|news release|press release",
    re.IGNORECASE,
)

# H4: company self-reference in quotes e.g. '("the Company")'
_COMPANY_QUOTE = re.compile(r'\("the Company"\)', re.IGNORECASE)

# H5: city + date dateline e.g. "MIAMI (March 27, 2026)", "Dallas, TX – March 26, 2026"
_DATELINE = re.compile(
    r"(?:^|\n)[A-Z][A-Za-z\s]{2,20}[\s,–-]+\(?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2},?\s+\d{4}",
    re.MULTILINE,
)

# H5 fallback: standalone date anywhere in text
_STANDALONE_DATE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2},?\s+\d{4}\b"
)

# H6: common press release action verbs
_PR_VERBS = re.compile(
    r"issued a press release|reported|provides an update"
    r"|today announced|today named",
    re.IGNORECASE,
)

# H7: whitelisted exchange ticker e.g. "(NYSE: CCL)", "(NASDAQ: AAPL)", "(NYSE/LSE: CCL; NYSE: CUK)"
_TICKER = re.compile(
    r"\((?:NYSE|NASDAQ|LSE|OTCQB|OTCQX|NYSE American|NYSE Arca|NASDAQ GSM|NASDAQ CM)"
    r"(?:/(?:NYSE|NASDAQ|LSE))?[:\s]+[A-Z]{1,6}[;,\s]*"
    r"(?:(?:NYSE|NASDAQ|LSE|OTCQB|OTCQX)[:\s]+[A-Z]{1,6}[;,\s]*)?"
    r"\)",
    re.IGNORECASE,
)


def _parse_words(html_text):
    """Parse HTML and return all words as a list, stripping zero-width spaces."""
    soup = BeautifulSoup(html_text, "html.parser")
    raw = " ".join(soup.stripped_strings)
    raw = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", raw)
    return [w for w in raw.split() if w]


def analyze_heuristics(html_text):
    """
    Runs all 7 heuristics independently with no hierarchy or early exit.
    Returns a dict with 1 (fired) or 0 (did not fire) for each heuristic.
    Use for statistical analysis — not for production classification.
    """
    words = _parse_words(html_text)
    text_first = " ".join(words[:200])
    text_last = " ".join(words[-200:])

    return {
        "H1": int(bool(_CONTACT_BLOCK.search(text_last))),
        "H2": int(bool(_WIRE_SERVICE.search(text_first))),
        "H3": int(bool(_PR_HEADERS.search(text_first))),
        "H4": int(bool(_COMPANY_QUOTE.search(text_first))),
        "H5": int(bool(_DATELINE.search(text_first) or _STANDALONE_DATE.search(text_first))),
        "H6": int(bool(_PR_VERBS.search(text_first))),
        "H7": int(bool(_TICKER.search(text_first))),
    }


def classify_heuristic(signals):
    """
    Classify from a pre-computed heuristics dict (output of analyze_heuristics).
    Returns label string or None.

    Strong signals (trusted directly):
      H1        - investor/media contact block
      H2        - wire service name
      H3        - explicit PR header phrase
      H5+H7     - dateline + ticker
      H6+H7     - PR verb + ticker

    Weak signals (caller should verify with LLM):
      combined  - dateline (H5) + PR verb (H6), no ticker
      H7        - ticker alone

    H4 is retained in analyze_heuristics for CSV compatibility but not used here.
    """
    if signals["H1"]: return "H1"
    if signals["H2"]: return "H2"
    if signals["H3"]: return "H3"
    if signals["H7"] and signals["H5"]: return "H5+H7"
    if signals["H7"] and signals["H6"]: return "H6+H7"
    if signals["H7"]: return "H7"
    if signals["H5"] and signals["H6"]: return "combined"

    return None


async def classify_llm(html_text):
    """
    Classify using Claude Haiku. Returns "llm" if yes, None if no.
    Sends first 300 words (stripped of zero-width spaces and XBRL metadata).
    Requires ANTHROPIC_API_KEY environment variable.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    raw = " ".join(soup.stripped_strings)
    # Strip zero-width spaces and other invisible Unicode that inflate word count
    raw = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", raw)
    words = [w for w in raw.split() if w]
    # Skip leading XBRL/SGML metadata tokens (e.g. "2 ex99-1.htm EX-99.1 Exhibit 99.1")
    skip_patterns = re.compile(r"^(EX-\d+\.\d+|Exhibit|\S+\.html?|\d+\.\d+|\d+)$", re.IGNORECASE)
    while words and skip_patterns.match(words[0]):
        words.pop(0)

    excerpt = " ".join(words[:100])

    message = await _anthropic_client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=10,
        system="Is the following document excerpt a press release or an earnings announcement? Answer with only 'yes' or 'no'.",
        messages=[
            {
                "role": "user",
                "content": excerpt,
            }
        ],
    )
    answer = message.content[0].text.strip().lower()
    return "llm" if answer.startswith("yes") else None


_LLM_VERIFY = {"combined", "H7"}

async def classify(html_text):
    """
    Full classification pipeline: heuristics first, LLM verification for weak signals.
    Returns label string or None.
    """
    signals = analyze_heuristics(html_text)
    label = classify_heuristic(signals)
    if label is None:
        return None
    if label in _LLM_VERIFY:
        return await classify_llm(html_text)
    return label
