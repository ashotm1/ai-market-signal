"""
edgar.py — SEC EDGAR fetching utilities.
Provides functions to fetch EX-99.x exhibit URLs from filing index pages.
"""
import re
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "YourName your@email.com"}
SEC_BASE = "https://www.sec.gov"
SEC_ARCHIVES = "https://www.sec.gov/Archives/"

_EX99_TYPE = re.compile(r"^EX-99\.\d+$")


def parse_ex99_urls(index_html):
    """
    Parse EX-99.x exhibit URLs from an already-fetched EDGAR index page HTML.
    Returns a list of absolute URLs.
    """
    soup = BeautifulSoup(index_html, "html.parser")
    urls = []

    table = soup.find("table", {"summary": "Document Format Files"})
    if not table:
        header = next(
            (tag for tag in soup.find_all(["p", "h2", "h3"])
             if "Document Format Files" in tag.get_text()),
            None,
        )
        if header:
            table = header.find_next("table")

    if not table:
        return urls

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        doc_type = cells[3].get_text(strip=True)
        if not _EX99_TYPE.match(doc_type):
            continue

        a_tag = cells[2].find("a")
        if not a_tag:
            continue

        href = a_tag.get("href", "")
        if href.startswith("/ix?doc="):
            href = href[len("/ix?doc="):]
        if not href.startswith("http"):
            href = SEC_BASE + href

        urls.append(href)

    return urls


async def fetch_ex99_urls(client, index_url):
    """
    Fetch the EDGAR filing index page and return EX-99.x exhibit URLs.
    `client` is an httpx.AsyncClient instance.
    """
    try:
        r = await client.get(index_url, headers=HEADERS)
    except Exception:
        return []

    if r.status_code != 200:
        return []

    return parse_ex99_urls(r.text)


async def fetch_html(client, url):
    """
    Fetch a URL and return the response text, or None on failure.
    `client` is an httpx.AsyncClient instance.
    """
    try:
        r = await client.get(url, headers=HEADERS)
    except Exception:
        return None

    return r.text if r.status_code == 200 else None
