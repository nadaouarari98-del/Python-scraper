"""
src/downloader/auto_downloader.py
-----------------------------------
MODE 1 — Automated PDF discovery and download.

Workflow per company
--------------------
1. Look up the company in ``config/sources.yaml`` (known_companies list).
2. If not found, search BSE and/or NSE for the investor page URL.
3. Fetch the investor page with ``requests + BeautifulSoup``.
4. If zero PDF links found and ``uses_javascript=True``, fall back to
   Playwright.
5. For every PDF link:
   a. Check ``robots.txt`` compliance.
   b. Respect per-domain rate limit (1 req/s).
   c. Download with ``requests``, retrying up to 3× via ``tenacity``
      (exponential backoff: 2s, 4s, 8s).
   d. Validate magic bytes.
   e. Save to ``data/input/{slug}/{year}/``.
6. Log every attempt and update the JSON progress tracker.
7. Failed downloads are appended to ``data/logs/failed_downloads.log``.

Public API
----------
::

    from src.downloader import download_pdfs

    result = download_pdfs(["Tech Mahindra", "Reliance"], source="both")
    # Returns:
    # {
    #   "Tech Mahindra": {"found": 4, "downloaded": 4, "failed": 0},
    #   ...
    # }
"""

from __future__ import annotations

import datetime
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import DownloaderConfig, KnownCompany, load_config
from .logger import get_logger
from .playwright_fallback import extract_pdf_links_js
from .progress import increment_status
from .rate_limiter import RateLimiter
from .robots_checker import RobotsChecker
from .validator import validate_download_bytes

_logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Session singleton
# ---------------------------------------------------------------------------
_session: requests.Session | None = None


def _get_session(user_agent: str) -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": user_agent})
    return _session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slugify(name: str) -> str:
    """Convert a company name to a filesystem-safe slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def _guess_year(url: str) -> str:
    """Attempt to extract a 4-digit year from a PDF URL."""
    match = re.search(r"(19|20)\d{2}", url)
    return match.group(0) if match else "unknown"


def _build_filename(url: str, company_slug: str, year: str) -> str:
    """Derive a standardised filename for a downloaded PDF."""
    original = Path(urlparse(url).path).name or "document.pdf"
    original = re.sub(r"[^\w.\-]", "_", original)
    return f"{company_slug}_{year}_{original}"


def _log_failure(failed_log: str, entry: dict[str, Any]) -> None:
    """Append a failure record to the failed_downloads.log file."""
    Path(failed_log).parent.mkdir(parents=True, exist_ok=True)
    with open(failed_log, "a", encoding="utf-8") as fh:
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        fh.write(
            f"{timestamp} | company={entry.get('company')} | "
            f"url={entry.get('url')} | reason={entry.get('reason')}\n"
        )


# ---------------------------------------------------------------------------
# PDF link scraping
# ---------------------------------------------------------------------------

def _scrape_pdf_links_static(
    url: str,
    keywords: list[str],
    extensions: list[str],
    session: requests.Session,
    timeout: int,
) -> list[str]:
    """Fetch *url* with requests and extract PDF link hrefs using BeautifulSoup."""
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        _logger.warning("Static fetch failed for %s: %s", url, exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    links: list[str] = []

    for tag in soup.find_all("a", href=True):
        href: str = tag["href"].strip()
        abs_href = urljoin(url, href)
        href_lower = abs_href.lower()
        text_lower = tag.get_text(strip=True).lower()

        has_ext = any(href_lower.endswith(ext.lower()) for ext in extensions)
        has_kw = any(
            kw in href_lower or kw in text_lower for kw in keywords
        )

        if has_ext or has_kw:
            links.append(abs_href)

    return list(dict.fromkeys(links))  # deduplicate, preserve order


# ---------------------------------------------------------------------------
# BSE / NSE company lookup
# ---------------------------------------------------------------------------

def _search_bse(
    company_name: str, cfg: DownloaderConfig, session: requests.Session
) -> str | None:
    """Return an investor page URL for *company_name* via BSE search, or None."""
    params = {"comp_name": company_name}
    try:
        resp = session.get(
            cfg.bse.search_endpoint,
            params=params,
            timeout=cfg.downloader.request_timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        # BSE API returns a list of dicts; try to find the company's page
        for item in (data if isinstance(data, list) else []):
            name_match = company_name.lower() in str(
                item.get("LONG_NAME", item.get("name", ""))
            ).lower()
            if name_match:
                code = item.get("SCRIP_CD", item.get("code", ""))
                slug = _slugify(str(item.get("LONG_NAME", company_name)))
                url = cfg.bse.company_page_pattern.format(slug=slug, code=code)
                _logger.debug("BSE found page for %s: %s", company_name, url)
                return url
    except Exception as exc:  # noqa: BLE001
        _logger.warning("BSE search failed for '%s': %s", company_name, exc)
    return None


def _search_nse(
    company_name: str, cfg: DownloaderConfig, session: requests.Session
) -> str | None:
    """Return an investor page URL for *company_name* via NSE search, or None."""
    try:
        endpoint = cfg.nse.company_search_endpoint.format(
            query=requests.utils.quote(company_name)
        )
        resp = session.get(
            endpoint,
            timeout=cfg.downloader.request_timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("data", []) if isinstance(data, dict) else []
        for item in results:
            symbol = item.get("symbol", "")
            if symbol:
                url = cfg.nse.company_page_pattern.format(symbol=symbol)
                _logger.debug("NSE found page for %s: %s", company_name, url)
                return url
    except Exception as exc:  # noqa: BLE001
        _logger.warning("NSE search failed for '%s': %s", company_name, exc)
    return None


def _resolve_investor_page(
    company_name: str,
    cfg: DownloaderConfig,
    source: str,
    session: requests.Session,
) -> tuple[str | None, bool]:
    """Return ``(investor_page_url, uses_javascript)`` for *company_name*.

    Checks known_companies first, then searches BSE/NSE per *source*.

    Args:
        company_name: Human-readable company name.
        cfg:          Loaded downloader config.
        source:       ``"bse"``, ``"nse"``, or ``"both"``.
        session:      Shared requests session.

    Returns:
        ``(url, uses_javascript)`` or ``(None, False)`` if not found.
    """
    name_lower = company_name.lower()

    # 1. Known companies list (exact or partial match)
    for company in cfg.known_companies:
        if (
            name_lower == company.name.lower()
            or name_lower in company.name.lower()
            or company.name.lower() in name_lower
        ):
            _logger.info(
                "Found '%s' in known_companies: %s",
                company_name,
                company.investor_page,
            )
            return company.investor_page, company.uses_javascript

    # 2. Exchange search
    url: str | None = None
    if source in ("bse", "both"):
        url = _search_bse(company_name, cfg, session)
    if url is None and source in ("nse", "both"):
        url = _search_nse(company_name, cfg, session)

    return url, False


# ---------------------------------------------------------------------------
# Single PDF download (with tenacity retry)
# ---------------------------------------------------------------------------

def _make_retry_downloader(cfg: DownloaderConfig, session: requests.Session):
    """Return a retry-wrapped download function using tenacity."""

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(cfg.downloader.max_retries),
        wait=wait_exponential(
            multiplier=cfg.downloader.retry_backoff_seconds, min=2, max=60
        ),
        reraise=True,
    )
    def _download(url: str) -> bytes:
        resp = session.get(url, timeout=cfg.downloader.request_timeout_seconds, stream=True)
        resp.raise_for_status()
        return resp.content

    return _download


def _download_single_pdf(
    pdf_url: str,
    company_name: str,
    company_slug: str,
    cfg: DownloaderConfig,
    session: requests.Session,
    rate_limiter: RateLimiter,
    robots: RobotsChecker,
    retry_download,
) -> bool:
    """Download one PDF URL and save it to disk.

    Returns:
        ``True`` on success, ``False`` on failure.
    """
    # robots.txt check
    if not robots.can_fetch(pdf_url):
        _logger.warning("Skipping (robots.txt): %s", pdf_url)
        return False

    # Rate limit
    rate_limiter.wait(pdf_url)

    year = _guess_year(pdf_url)
    filename = _build_filename(pdf_url, company_slug, year)
    dest_dir = Path(cfg.paths.input_dir) / company_slug / year
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filename

    if dest_path.exists():
        _logger.info("Already exists, skipping: %s", dest_path)
        return True  # count as success

    _logger.info(
        "Downloading | company=%s | url=%s", company_name, pdf_url
    )

    try:
        data: bytes = retry_download(pdf_url)
    except (RetryError, requests.RequestException) as exc:
        _logger.error("Download failed after retries | url=%s | %s", pdf_url, exc)
        _log_failure(
            cfg.paths.failed_log,
            {"company": company_name, "url": pdf_url, "reason": str(exc)},
        )
        return False

    if not validate_download_bytes(data, pdf_url):
        reason = "Content is not a valid PDF (magic bytes check failed)"
        _logger.error("%s | url=%s", reason, pdf_url)
        _log_failure(
            cfg.paths.failed_log,
            {"company": company_name, "url": pdf_url, "reason": reason},
        )
        return False

    dest_path.write_bytes(data)
    size_kb = len(data) / 1024
    _logger.info(
        "Saved | company=%s | file=%s | size=%.1f KB",
        company_name,
        dest_path,
        size_kb,
    )
    return True


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------

def download_pdfs(
    companies: list[str],
    source: str = "both",
    config_path: str | None = None,
) -> dict[str, dict[str, int]]:
    """MODE 1 — Discover and download IEPF/shareholding PDFs automatically.

    Searches BSE and/or NSE for each company, scrapes investor pages,
    and downloads all discovered PDF files.

    Args:
        companies:   List of company names (e.g. ``["Tech Mahindra", "TCS"]``).
        source:      Which exchange(s) to search: ``"bse"``, ``"nse"``,
                     or ``"both"`` (default).
        config_path: Optional override for the path to ``sources.yaml``.

    Returns:
        A dict mapping each company name to its counters::

            {
                "Tech Mahindra": {"found": 4, "downloaded": 4, "failed": 0},
                "TCS": {"found": 2, "downloaded": 1, "failed": 1},
            }
    """
    cfg = load_config(config_path)

    session = _get_session(cfg.downloader.user_agent)
    rate_limiter = RateLimiter(
        min_interval=cfg.downloader.rate_limit_per_domain_seconds
    )
    robots = RobotsChecker(user_agent=cfg.downloader.user_agent)
    retry_download = _make_retry_downloader(cfg, session)

    keywords = cfg.pdf_discovery.link_keywords
    extensions = cfg.pdf_discovery.file_extensions

    results: dict[str, dict[str, int]] = {}

    for company_name in companies:
        _logger.info("=" * 60)
        _logger.info("Processing company: %s", company_name)

        counters: dict[str, int] = {"found": 0, "downloaded": 0, "failed": 0}
        company_slug = _slugify(company_name)

        # Resolve investor page
        investor_url, uses_js = _resolve_investor_page(
            company_name, cfg, source, session
        )

        if not investor_url:
            _logger.warning(
                "No investor page found for '%s'. Skipping.", company_name
            )
            results[company_name] = counters
            continue

        # Respect rate limit and robots for the investor page itself
        rate_limiter.wait(investor_url)
        if not robots.can_fetch(investor_url):
            _logger.warning(
                "robots.txt disallows investor page for %s", company_name
            )
            results[company_name] = counters
            continue

        # Scrape PDF links — static first
        pdf_links = _scrape_pdf_links_static(
            investor_url, keywords, extensions, session,
            cfg.downloader.request_timeout_seconds,
        )

        # Playwright fallback
        if not pdf_links and uses_js:
            _logger.info(
                "Static scrape found 0 links for %s — trying Playwright",
                company_name,
            )
            pdf_links = extract_pdf_links_js(
                investor_url, keywords=keywords, extensions=extensions
            )

        counters["found"] = len(pdf_links)
        _logger.info(
            "Found %d PDF link(s) for %s", len(pdf_links), company_name
        )

        increment_status(total_found=len(pdf_links))

        # Download each PDF
        for pdf_url in pdf_links:
            ok = _download_single_pdf(
                pdf_url,
                company_name,
                company_slug,
                cfg,
                session,
                rate_limiter,
                robots,
                retry_download,
            )
            if ok:
                counters["downloaded"] += 1
                increment_status(downloaded=1)
            else:
                counters["failed"] += 1
                increment_status(failed=1)

        results[company_name] = counters
        _logger.info(
            "Done | company=%s | found=%d | downloaded=%d | failed=%d",
            company_name,
            counters["found"],
            counters["downloaded"],
            counters["failed"],
        )

    return results
