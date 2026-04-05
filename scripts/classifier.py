"""
classifier.py — Press release classification logic.
Provides heuristic and LLM-based classifiers for SEC EX-99.x exhibits.
"""
import anthropic
from bs4 import BeautifulSoup
import re

# --- Compiled regex patterns ---

# H1: wire service city dateline e.g. "BOSTON--(BUSINESS WIRE)", "SAN DIEGO /PRNewswire/"
_WIRE_DATELINE = re.compile(
    r"[A-Z][A-Za-z\s]+--\s*\((?:BUSINESS WIRE|PR Newswire)\)|[A-Z][A-Za-z\s]+\s*/PRNewswire/"
)

# H4: investor relations contact block e.g. "Contact: Denise Barr"
_CONTACT_BLOCK = re.compile(r"\b(?:[Ii]nvestor\s+)?(?:[Cc]ontact|[Rr]elations|CONTACT|RELATIONS):\s+[A-Z][a-z]")

# H5: company self-reference in quotes e.g. '("the Company")'
_COMPANY_QUOTE = re.compile(r'\("the Company"\)|or the .{1,20}Company.{1,5}\)')

# H6: city + date dateline e.g. "MIAMI (March 27, 2026)", "Dallas, TX – March 26, 2026"
_DATELINE = re.compile(
    r"(?:^|\n)[A-Z][A-Za-z\s]{2,20}[\s,–-]+\(?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2},?\s+\d{4}",
    re.MULTILINE,
)

# H6 fallback: standalone date anywhere in text
_STANDALONE_DATE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2},?\s+\d{4}\b"
)

# H8: whitelisted exchange ticker e.g. "(NYSE: CCL)", "(NASDAQ: AAPL)", "(NYSE/LSE: CCL; NYSE: CUK)"
_TICKER = re.compile(
    r"\((?:NYSE|NASDAQ|LSE|OTCQB|OTCQX|NYSE American|NYSE Arca|NASDAQ GSM|NASDAQ CM)"
    r"(?:/(?:NYSE|NASDAQ|LSE))?[:\s]+[A-Z]{1,5}[;,\s]*"
    r"(?:(?:NYSE|NASDAQ|LSE|OTCQB|OTCQX)[:\s]+[A-Z]{1,5}[;,\s]*)?"
    r"\)"
)

# H2: PR distribution service names
_PR_SERVICES = ["globenewswire", "prnewswire", "businesswire"]

# H3: explicit PR header phrases
_PR_HEADERS = ["for immediate release", "news release", "press release"]

# H7: common press release action verbs
_PR_VERBS = [
    "announced", "issued a press release", "today reported",
    "provides an update", "reported that",
    "today announced", "announced today", "today named", "today appointed",
]


def _parse_words(html_text):
    """Parse HTML and return all words as a list."""
    soup = BeautifulSoup(html_text, "html.parser")
    return " ".join(soup.stripped_strings).split()


def analyze_heuristics(html_text):
    """
    Runs all 8 heuristics independently with no hierarchy or early exit.
    Returns a dict with 1 (fired) or 0 (did not fire) for each heuristic.
    Use for statistical analysis — not for production classification.
    """
    words = _parse_words(html_text)
    text_first = " ".join(words[:200])
    text_last = " ".join(words[-200:])
    text_first_lower = text_first.lower()

    return {
        "H1": int(bool(_WIRE_DATELINE.search(text_first))),
        "H2": int(any(s in text_first_lower for s in _PR_SERVICES)),
        "H4": int(bool(_CONTACT_BLOCK.search(text_last))),
        "H3": int(any(h in text_first_lower for h in _PR_HEADERS)),
        "H5": int(bool(_COMPANY_QUOTE.search(text_first))),
        "H6": int(bool(_DATELINE.search(text_first) or _STANDALONE_DATE.search(text_first))),
        "H7": int(any(v in text_first_lower for v in _PR_VERBS)),
        "H8": int(bool(_TICKER.search(text_first))),
    }


def classify_heuristic(html_text):
    """
    Classify using heuristics. Returns label string or None.

    Priority 1 — ordered by specificity (rarest/most reliable first):
      H1  - wire service dateline       (most specific)
      H2  - PR distribution service name
      H4  - investor relations contact block (checked in last 200 words)
      H3  - explicit PR header phrase
      H5  - company self-reference in quotes

    Priority 2 (H6+H7 required — date + PR verb):
      combined - date (H6) + PR verb (H7)
    """
    words = _parse_words(html_text)
    text_first = " ".join(words[:200])
    text_last = " ".join(words[-200:])
    text_first_lower = text_first.lower()

    if _WIRE_DATELINE.search(text_first):
        return "H1"
    if any(s in text_first_lower for s in _PR_SERVICES):
        return "H2"
    if _CONTACT_BLOCK.search(text_last):
        return "H4"
    if any(h in text_first_lower for h in _PR_HEADERS):
        return "H3"
    if _COMPANY_QUOTE.search(text_first):
        return "H5"

    h6 = bool(_DATELINE.search(text_first) or _STANDALONE_DATE.search(text_first))
    h7 = any(v in text_first_lower for v in _PR_VERBS)
    if h6 and h7:
        return "combined"

    return None


async def classify_llm(html_text):
    """
    Classify using Claude Haiku. Returns "llm" if yes, None if no.
    Sends first and last 200 words as context.
    Requires ANTHROPIC_API_KEY environment variable.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    words = " ".join(soup.stripped_strings).split()

    first_200 = " ".join(words[:200])
    last_200 = " ".join(words[-200:]) if len(words) > 400 else ""
    excerpt = f"{first_200}\n\n[...]\n\n{last_200}".strip() if last_200 else first_200

    client = anthropic.AsyncAnthropic()
    message = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=10,
        messages=[
            {
                "role": "user",
                "content": (
                    "Is the following document excerpt a press release or an earnings announcement? "
                    "Answer 'yes' for press releases and quarterly/annual earnings releases. "
                    "Answer 'no' for proforma financials, loan schedules, asset sale documents, or legal exhibits. "
                    "Answer with only 'yes' or 'no'.\n\n"
                    f"{excerpt}"
                ),
            }
        ],
    )
    answer = message.content[0].text.strip().lower()
    return "llm" if answer.startswith("yes") else None


async def classify(html_text):
    """
    Full classification pipeline: heuristics first, LLM fallback.
    Returns label string or None.
    """
    label = classify_heuristic(html_text)
    if label is not None:
        return label
    return await classify_llm(html_text)
