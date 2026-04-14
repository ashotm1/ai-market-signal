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
_MA                = re.compile(
    r"\bmerger\b|\bacquires\b|to acquire|to merge|\btender offer\b|\bacquisition\b|\bdivest"
    r"|\bcommitted to.{0,20}transaction\b|\bclosing expected\b"
    r"|to be acquired|definitive agreement.{0,30}(?:acqui|merg|sale)"
    r"|sale to.{0,20}(?:private equity|PE firm|\bfirm\b)"
    r"|take.{0,10}private",
    re.IGNORECASE,
)
_REPORTS           = re.compile(r"\breports\b", re.IGNORECASE)
_PERSONNEL         = re.compile(
    r"\bappointments?\b|\bappoints?\b"
    r"|\bexecutive\b.{0,40}(?:update|names|departure|transitions?|changes?)"
    r"|\bretires?\b|\bdeparture\b"
    r"|\bchief\s+(?:executive|financial|operating|marketing|technology|medical)\s+officer\b"
    r"|\bC[FOM]O\b|\bCEO\b|\bCOO\b|\bCTO\b|\bCMO\b"
    r"|\bleadership\s+(?:changes?|transitions?|updates?)"
    r"|\bjoins?\b.{0,30}(?:board of directors|advisory board|board as)"
    r"|\bsucceeds?\b.{0,30}(?:as|CEO|CFO|COO|president|chairman)"
    r"|\belects?\b.{0,30}(?:director|chairman|president)"
    r"|\bnamed\b.{0,30}(?:CEO|CFO|COO|CTO|president|chairman|director)",
    re.IGNORECASE,
)
_AGREEMENT         = re.compile(r"\bagreement\b|\bdeal\b", re.IGNORECASE)
_COLLABORATION     = re.compile(r"strategic collaboration|collaboration agreement|\blicensing\s+agreement\b|strategic partnership|strategic alliance|co-development agreement", re.IGNORECASE)
_DEBT_OFFERING     = re.compile(r"senior notes|senior unsecured|debt restructuring|notes offering|credit facility|\bnotes due\b|credit facilities|exchangeable.*debentures|secured.*credit|\b\d+\.?\d*%\s+(?:senior|notes|debentures)", re.IGNORECASE)
_REBRANDING        = re.compile(r"name change|\brebrands?\b|announces new name|formerly known as", re.IGNORECASE)
_BUYBACK           = re.compile(r"share repurchase|stock repurchase|\bbuyback\b|repurchase program", re.IGNORECASE)
_ASSET_TRANSACTION = re.compile(r"asset sale|sale of.{0,30}(?:operations|division|unit|subsidiary|\bassets?\b)|\bdivests?\b|disposition of", re.IGNORECASE)
_SPAC              = re.compile(r"business combination|over-allotment|separate trading.{0,20}(?:shares|warrants)|de-spac", re.IGNORECASE)
_REGULATORY        = re.compile(r"commission (?:approves?|authorizes?)|authorizes? new rates|regulatory approval(?! of drug| of therapy| of treatment)|restores? compliance|nasdaq.*(?:compliance|rule)", re.IGNORECASE)
_OPERATIONAL_UPDATE= re.compile(r"assets under management|\bAUM\b|monthly production|operational update|business update|\boutlook\b(?!.{0,20}(?:drug|trial|therapy))|annual report|shareholder letter|corporate update", re.IGNORECASE)
_INVESTOR_EVENT    = re.compile(r"to participate|to present at|to host|investor day|analyst day|to speak at|conference call scheduled|to ring the bell|schedules.*(?:earnings call|earnings release)", re.IGNORECASE)
_CONTRACT          = re.compile(r"\bawarded?\b.{0,30}(?:contract|order|grant|funding)|\bwins?\b.{0,30}contract|\bsecures?\b.{0,30}(?:contract|order)|\breceives?\b.{0,30}order\b", re.IGNORECASE)
_FINANCIAL_UPDATE  = re.compile(r"record.{0,30}(?:commitments?|investments?)|distribution rate|net asset value|\bNAV\b", re.IGNORECASE)
_CLINICAL          = re.compile(
    r"to presents?.{0,40}data|data.{0,40}to present"
    r"|phase [123i]+[abi]?\s+(?:study|trial|data|results|clinical|readout|dose)"
    r"|phase [123]/[123]"
    r"|\bPDUFA\b"
    r"|fda (?:approval|clearance|designation|grants|breakthrough)"
    r"|clinical (?:trial|data|results|studies|development|pathway)"
    r"|\btrial results\b|\bregistrational trial\b"
    r"|510\(?k\)?|breakthrough device"
    r"|complete response letter|\bCRL\b"
    r"|(?:first|initial).{0,20}(?:commercial|patient).{0,20}(?:case|treatment|use)"
    r"|\btopline\b|\bpivotal.{0,20}(?:study|trial|data)\b"
    r"|\bIND\b.{0,20}(?:clearance|submission|filing)"
    r"|\bNDA\b|\bBLA\b|\bsNDA\b|\bsBLA\b"
    r"|orphan drug|rare disease designation"
    r"|\benrolls?\b.{0,20}(?:first|initial).{0,20}patient",
    re.IGNORECASE,
)


def classify_catalyst(title):
    """
    Classify catalyst types from PR title using keyword patterns.
    Returns list of matched catalyst tags, or ['other'].

    Tags split into two groups:
      SIGNAL     — goes to LLM feature extraction + model training
      EXCLUSION  — skip LLM extraction (low/no price signal)
    """
    if not title:
        return ["other"]

    checks = [
        # ── SIGNAL tags ───────────────────────────────────────────────────────
        (_CLINICAL,           "clinical"),
        (_PRIVATE_PLACEMENT,  "private_placement"),
        (_COLLABORATION,      "collaboration"),
        (_MA,                 "m&a"),
        (_ASSET_TRANSACTION,  "asset_transaction"),
        (_NEW_PRODUCT,        "new_product"),
        (_CONTRACT,           "contract"),
        (_AGREEMENT,          "agreement"),
        # ── EXCLUSION tags ────────────────────────────────────────────────────
        (_OFFERING,           "offering"),
        (_DEBT_OFFERING,      "debt_offering"),
        (_PERSONNEL,          "personnel"),
        (_BUYBACK,            "buyback"),
        (_SPLIT,              "split"),
        (_DIVIDEND,           "dividend"),
        (_NASDAQ_ALERT,       "nasdaq_alert"),
        (_SPAC,               "spac"),
        (_REBRANDING,         "rebranding"),
        (_INVESTOR_EVENT,     "investor_event"),
        (_REGULATORY,         "regulatory"),
        (_OPERATIONAL_UPDATE, "operational_update"),
        (_FINANCIAL_UPDATE,   "financial_update"),
    ]

    tags = [tag for pattern, tag in checks if pattern.search(title)]

    if _REPORTS.search(title) and _EARNINGS.search(title):
        tags.append("earnings")

    return tags if tags else ["other"]


def _parse_soup(html_text):
    """Parse HTML, return (soup, words) — single parse reused by callers."""
    soup = BeautifulSoup(html_text, "html.parser")
    raw = " ".join(soup.stripped_strings)
    raw = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", raw)
    return soup, [w for w in raw.split() if w]


def _is_bold(el):
    """Return True if element or any descendant carries bold styling."""
    style = el.get("style", "")
    if "font-weight:700" in style.replace(" ", "") or "font-weight:bold" in style.replace(" ", ""):
        return True
    if re.search(r"font\s*:[^;]*\bbold\b", style, re.IGNORECASE):
        return True
    if el.find(["b", "strong"]):
        return True
    for child in el.find_all(True):
        s = child.get("style", "")
        if "font-weight:700" in s.replace(" ", "") or "font-weight:bold" in s.replace(" ", ""):
            return True
        if re.search(r"font\s*:[^;]*\bbold\b", s, re.IGNORECASE):
            return True
    return False


def _bold_title(soup):
    """Return text of first valid bold <p> or bold <font> (in <div>) with 4+ words, or None."""
    for p in soup.find_all("p", limit=20):
        if not _is_bold(p):
            continue
        text = " ".join(p.get_text(" ", strip=True).split())
        if len(text.split()) >= 4 and _is_valid_title(text):
            return text
    # Fallback: <font> inside <div> that is bold
    for font in soup.find_all("font", limit=20):
        if font.find_parent("p"):
            continue  # already covered above
        is_bold = _is_bold(font) or bool(font.find_parent(["b", "strong"]))
        if not is_bold:
            continue
        text = " ".join(font.get_text(" ", strip=True).split())
        if len(text.split()) >= 4 and _is_valid_title(text):
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


_TITLE_GARBAGE = re.compile(
    r"^for more information|^for immediate release|^contacts?:|^media contact|^investor contact"
    r"|^source:|^about ",
    re.IGNORECASE,
)


def _is_valid_title(text: str) -> bool:
    """Return False if text looks like a dateline, contact block, or boilerplate."""
    if not text:
        return False
    if text.rstrip().endswith(":"):
        return False
    if len(text.split()) > 35:
        return False
    if _STANDALONE_DATE.search(text) and len(text.split()) < 8:
        return False  # short dateline e.g. "SALT LAKE CITY, UT – March 12, 2026"
    if _TITLE_GARBAGE.search(text):
        return False
    return True


_PLAIN_TITLE_VERB = re.compile(
    r"\b(announces?|appoints?|completes?|launches?|introduces?|acquires?|divests?|"
    r"names?\s+new|elects?\s+|strengthens?|establishes?|enters?\s+into|expands?\b)",
    re.IGNORECASE,
)

# Leading junk: EDGAR file basenames ("bod_janx2026xfinal-nr"), "-more-" markers,
# single-separator slugs ("jpattenpr_v2"), digit-embedded slugs ("ex991pressrelease")
_JUNK_LEAD = re.compile(
    r"^(?:-\w+-|[a-z0-9]+(?:[_-][a-z0-9]+){1,}|[a-z]+\d+[a-z][a-z0-9]*)$",
    re.IGNORECASE,
)


def _plain_title(words):
    """
    Fallback: scan raw words for a title-like sentence containing a PR action verb.
    Returns title string or None.
    """
    # Skip leading XBRL tokens and EDGAR file-basename junk
    while words and (_SKIP_TOKENS.match(words[0]) or _JUNK_LEAD.match(words[0])):
        words = words[1:]

    if not words:
        return None

    # Work from the start of cleaned words
    text = " ".join(words[:80])

    m = _PLAIN_TITLE_VERB.search(text)
    if not m:
        return None

    # Take from start of text through verb + up to 15 words after
    suffix = text[m.end() :].split()[:25]
    candidate = (text[: m.end()] + (" " + " ".join(suffix) if suffix else "")).strip()

    # Strip leading single-word section headers (e.g. "News", "Source")
    candidate = re.sub(r"^(?:News|Source|Alert|Notice|Update)\s+", "", candidate, flags=re.IGNORECASE)

    # Limit to 35 words
    candidate = " ".join(candidate.split()[:35])

    # Trim at standalone date + any trailing all-caps city name before it
    dl = _STANDALONE_DATE.search(candidate)
    if dl:
        before_date = candidate[: dl.start()]
        before_date = re.sub(r"\s+[A-Z]{2,}(?:\s+[A-Z][a-zA-Z]+)*\s*$", "", before_date)
        candidate = before_date.strip().rstrip(",–—- ")

    return candidate if _is_valid_title(candidate) else None


def _strip_slug(title: str) -> str:
    """Strip leading EDGAR filename slug(s) and skip-tokens from a title string."""
    words = title.split()
    while words and (_JUNK_LEAD.match(words[0]) or _SKIP_TOKENS.match(words[0])):
        words = words[1:]
    return " ".join(words).strip()


def extract_title(html_text):
    """Extract press release title from HTML."""
    soup, words = _parse_soup(html_text)
    title = _bold_title(soup)
    if title:
        return _strip_slug(title) or None
    result = _plain_title(list(words))
    return _strip_slug(result) if result else None


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
    excerpt = " ".join(words[:100])
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

    excerpt = " ".join(words[:300])

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


