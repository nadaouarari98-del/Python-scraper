"""
src/downloader/playwright_fallback.py
--------------------------------------
Playwright-based fallback for JavaScript-rendered investor pages.

Improvements over the original:
- Stealth mode: disables ``navigator.webdriver``, randomises UA from a pool
- Random inter-action delays to mimic human browsing
- Cookie-consent dismissal (common Indian financial sites)
- Configurable ``wait_until`` and extra wait after network idle
- ``extract_pdf_links_js_stealth`` — Playwright-first mode (no static pre-check)

Install (once):
    playwright install chromium

Usage:
    from src.downloader.playwright_fallback import extract_pdf_links_js

    links = extract_pdf_links_js(
        url="https://example.com/investors",
        keywords=["iepf", "shareholding"],
    )
"""
from __future__ import annotations

import random
import time
from urllib.parse import urljoin, urlparse

from .logger import get_logger

_logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Realistic Chrome User-Agent pool
# ---------------------------------------------------------------------------
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

# Common "Accept Cookie" button selectors found on Indian financial sites
_CONSENT_SELECTORS = [
    "button:has-text('Accept')",
    "button:has-text('Accept All')",
    "button:has-text('I Agree')",
    "button:has-text('OK')",
    "[id*='cookie'] button",
    "[class*='cookie'] button",
    "[class*='consent'] button",
    "#accept-cookies",
]


def _is_pdf_link(href: str, keywords: list[str], extensions: list[str]) -> bool:
    """Return True if the href looks like a PDF download link."""
    href_lower = href.lower()
    has_ext = any(href_lower.endswith(ext.lower()) for ext in extensions)
    has_keyword = any(kw.lower() in href_lower for kw in keywords)
    return has_ext or has_keyword


def _apply_stealth(page) -> None:
    """Patch the Playwright page to remove automation fingerprints.

    - Removes `navigator.webdriver`
    - Spoofs `navigator.plugins` to be non-empty
    - Sets a plausible `navigator.language`
    """
    stealth_js = """
        () => {
            // Remove webdriver flag
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            // Spoof plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            // Language
            Object.defineProperty(navigator, 'language', {
                get: () => 'en-US'
            });
            // Platform
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Win32'
            });
        }
    """
    page.add_init_script(stealth_js)


def _dismiss_consent(page) -> None:
    """Try to click common cookie-consent buttons (best-effort, no exception)."""
    for selector in _CONSENT_SELECTORS:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=1000):
                btn.click(timeout=2000)
                _logger.debug("Consent dismissed with selector: %s", selector)
                page.wait_for_timeout(500)
                return
        except Exception:  # noqa: BLE001
            pass


def extract_pdf_links_js(
    url: str,
    keywords: list[str] | None = None,
    extensions: list[str] | None = None,
    timeout_ms: int = 45_000,
    wait_until: str = "networkidle",
    extra_wait_ms: int = 2000,
) -> list[str]:
    """Use Playwright (headless Chromium) to extract PDF links from *url*.

    This function applies stealth patches, randomises the User-Agent, waits
    for the page to fully settle, dismisses cookie-consent banners, and then
    scans all ``<a href>`` values for PDF links.

    Args:
        url:           The investor/shareholder page to visit.
        keywords:      Substrings (case-insensitive) to match in link href.
        extensions:    File extensions to treat as PDFs (e.g. ``[".pdf"]``).
        timeout_ms:    Page-load timeout in milliseconds.
        wait_until:    Playwright ``wait_until`` strategy.
        extra_wait_ms: Additional wait after ``networkidle`` (for lazy loaders).

    Returns:
        List of absolute PDF URLs discovered on the page.
        Returns an empty list if Playwright is not installed or the page fails.
    """
    keywords = keywords or ["iepf", "unclaimed", "dividend", "shareholding", "pattern", "annual"]
    extensions = extensions or [".pdf", ".PDF"]

    try:
        from playwright.sync_api import sync_playwright  # noqa: PLC0415
    except ImportError:
        _logger.warning(
            "Playwright not installed. Run `pip install playwright` "
            "and `playwright install chromium` to enable JS fallback."
        )
        return []

    user_agent = random.choice(_USER_AGENTS)
    found_links: list[str] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent=user_agent,
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Kolkata",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                },
            )
            page = context.new_page()
            _apply_stealth(page)

            # Random startup delay to avoid bursty patterns
            time.sleep(random.uniform(0.5, 1.5))

            _logger.info("Playwright: navigating to %s (UA: ...%s)", url, user_agent[-20:])
            page.goto(url, timeout=timeout_ms, wait_until=wait_until)

            # Extra wait for lazy-loaded content (e.g. React SPAs)
            page.wait_for_timeout(extra_wait_ms)

            # Dismiss cookie banners
            _dismiss_consent(page)

            # Allow scroll-triggered loaders
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(500)

            # Collect all anchor hrefs
            hrefs: list[str] = page.eval_on_selector_all(
                "a[href]",
                "elements => elements.map(el => el.href)",
            )

            # Also check for PDF links inside iframes (common on BSE)
            try:
                frames = page.frames
                for frame in frames[1:]:   # skip main frame
                    try:
                        iframe_hrefs = frame.eval_on_selector_all(
                            "a[href]",
                            "elements => elements.map(el => el.href)",
                        )
                        hrefs.extend(iframe_hrefs)
                    except Exception:  # noqa: BLE001
                        pass
            except Exception:  # noqa: BLE001
                pass

            browser.close()

        for href in hrefs:
            if not href:
                continue
            abs_href = urljoin(url, href)
            if _is_pdf_link(abs_href, keywords, extensions):
                found_links.append(abs_href)

        found_links = list(dict.fromkeys(found_links))   # deduplicate, preserve order
        _logger.info("Playwright found %d PDF link(s) on %s", len(found_links), url)

    except Exception as exc:  # noqa: BLE001
        _logger.error("Playwright failed for %s: %s", url, exc)

    return found_links


def extract_pdf_links_js_stealth(
    url: str,
    keywords: list[str] | None = None,
    extensions: list[str] | None = None,
) -> list[str]:
    """Alias for ``extract_pdf_links_js`` with stealth-optimised defaults.

    Use this as the Playwright-first mode (skip the static requests pre-check)
    when you know the target site is JavaScript-heavy (e.g. BSE/NSE company pages).
    """
    return extract_pdf_links_js(
        url=url,
        keywords=keywords,
        extensions=extensions,
        timeout_ms=60_000,
        wait_until="networkidle",
        extra_wait_ms=3000,
    )
