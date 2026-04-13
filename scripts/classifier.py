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
    r"\b(?:investor\s+|media\s+)?(?:contacts?|relations):\s",
    re.IGNORECASE,
)

# H2: wire service name anywhere in first 200 words — covers datelines and bylines
# e.g. "BOSTON--(BUSINESS WIRE)", "/PRNewswire/", "Source: GlobeNewswire"
# Combines what were two separate checks (regex + list) into one pattern.
_WIRE_SERVICE = re.compile(
    r"Business\s*Wire|PR\s*Newswire|Globe\s*Newswire|Access\s*Wire"
    r"|Market\s*wired|Canada\s*Newswire|CNW\s*Group|EQS\s*News|Benzinga|Newsfile"
    r"|Access\s*Newswire",
    re.IGNORECASE,
)

# H3: explicit PR header phrases
_PR_HEADERS = re.compile(
    r"for immediate release|news release|press release",
    re.IGNORECASE,
)

# H4: standalone date anywhere in first 200 words e.g. "March 27, 2026"
_STANDALONE_DATE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2},?\s+\d{4}\b"
)

# H5: common press release action verbs
_PR_VERBS = re.compile(
    r"issued a press release|provides an update"
    r"|today announced|announced today|"
    r"today reported|reported today|today released|released today",
    re.IGNORECASE,
)

# H6: whitelisted exchange ticker e.g. "(NYSE: CCL)", "(NASDAQ: AAPL)", "(NYSE/LSE: CCL; NYSE: CUK)"
_TICKER = re.compile(
    r"\((?:NYSE|NASDAQ|LSE|OTCQB|OTCQX|NYSE American|NYSE Arca|NASDAQ GSM|NASDAQ CM)"
    r"(?:/(?:NYSE|NASDAQ|LSE))?[:\s]+[A-Z]{1,6}[;,\s]*"
    r"(?:(?:NYSE|NASDAQ|LSE|OTCQB|OTCQX)[:\s]+[A-Z]{1,6}[;,\s]*)?"
    r"\)",
    re.IGNORECASE,
)


_SKIP_TOKENS = re.compile(r"^(EX-\d+\.\d+|Exhibit|\S+\.html?|\d+\.\d+|\d+)$", re.IGNORECASE)

# --- Catalyst keyword patterns (matched against extracted title) ---

_EARNINGS = re.compile(
    r"\bQ[1-4]\b|first quarter|second quarter|third quarter|fourth quarter"
    r"|full[- ]year|fiscal year|financial results|\bearnings\b",
    re.IGNORECASE,
)
_PRIVATE_PLACEMENT = re.compile(r"private placement", re.IGNORECASE)
_SPLIT             = re.compile(r"stock split", re.IGNORECASE)
_DIVIDEND          = re.compile(r"dividends?", re.IGNORECASE)
_NASDAQ_ALERT      = re.compile(r"minimum bid price|nasdaq notification", re.IGNORECASE)
_NEW_PRODUCT       = re.compile(r"unveils|\blaunches\b|\bintroduces\b", re.IGNORECASE)
_OFFERING          = re.compile(r"registered direct offering|announces pricing|\boffering\b", re.IGNORECASE)
_MA                = re.compile(r"\bmerger\b|\bacquires\b|to acquire|to merge", re.IGNORECASE)
_REPORTS           = re.compile(r"\breports\b", re.IGNORECASE)
_PERSONNEL         = re.compile(r"\bappointments?\b|\bappoints?\b|\bexecutive\b.{0,40}(?:update|names|departure)", re.IGNORECASE)
_AGREEMENT         = re.compile(r"\bagreement\b|\bdeal\b", re.IGNORECASE)
_CLINICAL          = re.compile(
    r"to presents?.{0,40}data|data.{0,40}to present"
    r"|phase [123i]+[abi]?\b|fda (?:approval|clearance|designation|grants)"
    r"|clinical (?:trial|data|results)|\btrial results\b",
    re.IGNORECASE,
)


def classify_catalyst(title):
    """
    Classify catalyst types from PR title using keyword patterns.
    Returns list of matched catalyst tags, or ['other'] if no match.
    """
    if not title:
        return ["other"]
    tags = []
    if _PRIVATE_PLACEMENT.search(title): tags.append("private_placement")
    if _SPLIT.search(title):             tags.append("split")
    if _DIVIDEND.search(title):          tags.append("dividend")
    if _NASDAQ_ALERT.search(title):      tags.append("nasdaq_alert")
    if _NEW_PRODUCT.search(title):       tags.append("new_product")
    if _OFFERING.search(title):          tags.append("offering")
    if _MA.search(title):                tags.append("m&a")
    if _CLINICAL.search(title):          tags.append("clinical")
    if _REPORTS.search(title) and _EARNINGS.search(title): tags.append("earnings")
    if _PERSONNEL.search(title):          tags.append("personnel")
    if _AGREEMENT.search(title):         tags.append("agreement")
    return tags if tags else ["other"]


def _parse_soup(html_text):
    """Parse HTML, return (soup, words) — single parse reused by callers."""
    soup = BeautifulSoup(html_text, "html.parser")
    raw = " ".join(soup.stripped_strings)
    raw = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", raw)
    return soup, [w for w in raw.split() if w]


def _is_bold(el):
    """Return True if element or any descendant carries bold styling."""
    style = el.get("style", "").replace(" ", "")
    if "font-weight:700" in style or "font-weight:bold" in style:
        return True
    if el.find(["b", "strong"]):
        return True
    for child in el.find_all(True):
        s = child.get("style", "").replace(" ", "")
        if "font-weight:700" in s or "font-weight:bold" in s:
            return True
    return False


def _bold_title(soup):
    """Return text of first bold <p> or bold <font> (in <div>) with 4+ words, or None."""
    for p in soup.find_all("p", limit=20):
        if not _is_bold(p):
            continue
        text = p.get_text(" ", strip=True)
        if len(text.split()) >= 4:
            return text
    # Fallback: <font> inside <div> that is bold
    for font in soup.find_all("font", limit=20):
        if font.find_parent("p"):
            continue  # already covered above
        is_bold = _is_bold(font) or bool(font.find_parent(["b", "strong"]))
        if not is_bold:
            continue
        text = font.get_text(" ", strip=True)
        if len(text.split()) >= 4:
            return text
    return None


def is_earnings(html_text):
    """Return True if the bold title contains earnings keywords."""
    soup, _ = _parse_soup(html_text)
    title = _bold_title(soup)
    return bool(title and _EARNINGS.search(title))


def analyze_heuristics(html_text):
    """
    Runs all 6 heuristics independently with no hierarchy or early exit.
    Returns a dict with 1 (fired) or 0 (did not fire) for each heuristic.
    """
    _, words = _parse_soup(html_text)
    first = words[:200]
    text_first = " ".join(first)
    text_last = " ".join(words[-200:])

    return {
        "H1": int(bool(_CONTACT_BLOCK.search(text_last))),
        "H2": int(bool(_WIRE_SERVICE.search(text_first))),
        "H3": int(bool(_PR_HEADERS.search(" ".join(first[:40])))),
        "H4": int(bool(_STANDALONE_DATE.search(text_first))),
        "H5": int(bool(_PR_VERBS.search(text_first))),
        "H6": int(bool(_TICKER.search(text_first))),
    }


def classify_heuristic(signals):
    """
    Classify from a pre-computed heuristics dict (output of analyze_heuristics).
    Returns label string or None.

    Strong signals (trusted directly):
      H1        - investor/media contact block
      H2        - wire service name
      H3        - explicit PR header phrase
      H4+H6     - dateline + ticker
      H5+H6     - PR verb + ticker

    Weak signals (caller should verify with LLM):
      combined  - dateline (H4) + PR verb (H5), no ticker
      H6        - ticker alone
    """
    if signals["H1"]: return "H1"
    if signals["H2"]: return "H2"
    if signals["H3"]: return "H3"
    if signals["H6"] and signals["H4"]: return "H4+H6"
    if signals["H6"] and signals["H5"]: return "H5+H6"
    if signals["H6"]: return "H6"
    if signals["H4"] and signals["H5"]: return "combined"

    return None


def extract_title(html_text):
    """
    Extract press release title from HTML.
    """
    soup, _ = _parse_soup(html_text)
    return _bold_title(soup)


async def extract_title_llm(html_text):
    """
    Extract title via LLM when heuristic extraction fails.
    Sends first 50 stripped words to Claude Haiku.
    Returns title string or None.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    raw = " ".join(soup.stripped_strings)
    raw = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", raw)
    words = [w for w in raw.split() if w]
    while words and _SKIP_TOKENS.match(words[0]):
        words.pop(0)
    if not words:
        return None
    excerpt = " ".join(words[:50])
    message = await _anthropic_client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=50,
        system="Extract the press release title from this excerpt. Return only the title text, nothing else. If no clear title is present, return 'unknown'.",
        messages=[{"role": "user", "content": excerpt}],
    )
    result = message.content[0].text.strip()
    return None if result.lower() == "unknown" else result


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
    while words and _SKIP_TOKENS.match(words[0]):
        words.pop(0)

    if not words:
        return None

    excerpt = " ".join(words[:100])

    message = await _anthropic_client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=10,
        system="Is the following document excerpt a press release? Answer with only 'yes' or 'no'.",
        messages=[
            {
                "role": "user",
                "content": excerpt,
            }
        ],
    )
    answer = message.content[0].text.strip().lower()
    return "llm" if answer.startswith("yes") else None


