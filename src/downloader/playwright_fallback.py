"""
src/downloader/playwright_fallback.py
--------------------------------------
Playwright-based fallback for JavaScript-rendered investor pages.

When ``requests + BeautifulSoup`` finds zero PDF links, this module
launches a headless Chromium browser, waits for the page to settle,
and extracts all ``<a>`` hrefs that look like PDF links.

The Playwright browsers must be installed separately::

    playwright install chromium

Usage::

    from src.downloader.playwright_fallback import extract_pdf_links_js

    links = extract_pdf_links_js(
        url="https://example.com/investors",
        keywords=["iepf", "shareholding"],
    )
"""

from __future__ import annotations

from urllib.parse import urljoin, urlparse

from .logger import get_logger

_logger = get_logger(__name__)


def _is_pdf_link(href: str, keywords: list[str], extensions: list[str]) -> bool:
    """Return True if the href looks like a PDF download link."""
    href_lower = href.lower()
    has_ext = any(href_lower.endswith(ext.lower()) for ext in extensions)
    has_keyword = any(kw.lower() in href_lower for kw in keywords)
    return has_ext or has_keyword


def extract_pdf_links_js(
    url: str,
    keywords: list[str] | None = None,
    extensions: list[str] | None = None,
    timeout_ms: int = 30_000,
    wait_until: str = "networkidle",
) -> list[str]:
    """Use Playwright (headless Chromium) to extract PDF links from *url*.

    Playwright launches a headless browser, navigates to *url*, waits for
    network activity to settle, then collects all ``<a href>`` values that
    match the given *keywords* or *extensions*.

    Args:
        url:        The investor/shareholder page to visit.
        keywords:   List of substrings (case-insensitive) to match in link
                    text or href (e.g. ``["iepf", "shareholding"]``).
        extensions: File extensions to treat as PDFs (e.g. ``[".pdf"]``).
        timeout_ms: Page-load timeout in milliseconds.
        wait_until: Playwright ``wait_until`` strategy
                    (``"networkidle"``, ``"load"``, ``"domcontentloaded"``).

    Returns:
        List of absolute PDF URLs discovered on the page.
        Returns an empty list if Playwright is not installed or the page
        fails to load.
    """
    keywords = keywords or ["iepf", "unclaimed", "dividend", "shareholding", "pattern"]
    extensions = extensions or [".pdf", ".PDF"]

    try:
        from playwright.sync_api import sync_playwright  # noqa: PLC0415
    except ImportError:
        _logger.warning(
            "Playwright is not installed. Run `pip install playwright` "
            "and `playwright install chromium` to enable JS fallback."
        )
        return []

    base_netloc = urlparse(url).netloc
    found_links: list[str] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()

            _logger.info("Playwright: navigating to %s", url)
            page.goto(url, timeout=timeout_ms, wait_until=wait_until)

            # Collect all anchor hrefs
            hrefs: list[str] = page.eval_on_selector_all(
                "a[href]",
                "elements => elements.map(el => el.href)",
            )
            browser.close()

        for href in hrefs:
            if not href:
                continue
            abs_href = urljoin(url, href)
            if _is_pdf_link(abs_href, keywords, extensions):
                found_links.append(abs_href)

        _logger.info(
            "Playwright found %d PDF link(s) on %s", len(found_links), url
        )

    except Exception as exc:  # noqa: BLE001
        _logger.error("Playwright failed for %s: %s", url, exc)

    return list(dict.fromkeys(found_links))  # deduplicate, preserve order
