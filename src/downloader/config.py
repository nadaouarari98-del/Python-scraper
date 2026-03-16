"""
src/downloader/config.py
------------------------
Load and validate config/sources.yaml into typed dataclasses.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


_CONFIG_PATH = Path("config/sources.yaml")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ExchangeConfig:
    """Settings for a single stock exchange (BSE or NSE)."""

    base_url: str
    search_endpoint: str
    investor_page_keywords: list[str] = field(default_factory=list)
    company_page_pattern: str = ""
    company_search_endpoint: str = ""


@dataclass
class KnownCompany:
    """A manually listed company with a direct investor page URL."""

    name: str
    slug: str
    investor_page: str
    uses_javascript: bool = False


@dataclass
class DownloaderSettings:
    """Tuneable download behaviour."""

    rate_limit_per_domain_seconds: float = 1.0
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    request_timeout_seconds: int = 30
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )


@dataclass
class PathsConfig:
    """File-system output paths."""

    input_dir: str = "data/input"
    logs_dir: str = "data/logs"
    downloader_log: str = "data/logs/downloader.log"
    failed_log: str = "data/logs/failed_downloads.log"
    progress_status: str = "data/logs/progress_status.json"


@dataclass
class PdfDiscoveryConfig:
    """Keywords and extensions used to find PDF links on investor pages."""

    link_keywords: list[str] = field(default_factory=list)
    file_extensions: list[str] = field(default_factory=lambda: [".pdf", ".PDF"])


@dataclass
class DownloaderConfig:
    """Root configuration object assembled from sources.yaml."""

    bse: ExchangeConfig
    nse: ExchangeConfig
    known_companies: list[KnownCompany]
    downloader: DownloaderSettings
    paths: PathsConfig
    pdf_discovery: PdfDiscoveryConfig


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict[str, Any]:
    """Read and parse a YAML file.

    Args:
        path: Path to the YAML file.

    Returns:
        Parsed dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the file is malformed.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {path}. "
            "Run from the project root or set the CONFIG_PATH env var."
        )
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_config(config_path: str | Path | None = None) -> DownloaderConfig:
    """Load and parse sources.yaml into a :class:`DownloaderConfig`.

    Args:
        config_path: Override the default ``config/sources.yaml`` path.
                     Reads the ``CONFIG_PATH`` environment variable as a
                     secondary override.

    Returns:
        Fully populated :class:`DownloaderConfig` instance.
    """
    path = Path(
        config_path
        or os.environ.get("CONFIG_PATH", "")
        or _CONFIG_PATH
    )
    raw = _load_yaml(path)

    # BSE
    bse_raw = raw.get("bse", {})
    bse = ExchangeConfig(
        base_url=bse_raw.get("base_url", ""),
        search_endpoint=bse_raw.get("search_endpoint", ""),
        company_page_pattern=bse_raw.get("company_page_pattern", ""),
        investor_page_keywords=bse_raw.get("investor_page_keywords", []),
    )

    # NSE
    nse_raw = raw.get("nse", {})
    nse = ExchangeConfig(
        base_url=nse_raw.get("base_url", ""),
        search_endpoint=nse_raw.get("search_endpoint", ""),
        company_page_pattern=nse_raw.get("company_page_pattern", ""),
        company_search_endpoint=nse_raw.get("company_search_endpoint", ""),
        investor_page_keywords=nse_raw.get("investor_page_keywords", []),
    )

    # Known companies
    known_companies = [
        KnownCompany(
            name=c.get("name", ""),
            slug=c.get("slug", ""),
            investor_page=c.get("investor_page", ""),
            uses_javascript=c.get("uses_javascript", False),
        )
        for c in raw.get("known_companies", [])
    ]

    # Downloader settings
    dl_raw = raw.get("downloader", {})
    downloader = DownloaderSettings(
        rate_limit_per_domain_seconds=float(
            dl_raw.get("rate_limit_per_domain_seconds", 1.0)
        ),
        max_retries=int(dl_raw.get("max_retries", 3)),
        retry_backoff_seconds=float(dl_raw.get("retry_backoff_seconds", 2.0)),
        request_timeout_seconds=int(dl_raw.get("request_timeout_seconds", 30)),
        user_agent=str(dl_raw.get("user_agent", DownloaderSettings.user_agent)),
    )

    # Paths
    paths_raw = raw.get("paths", {})
    paths = PathsConfig(
        input_dir=paths_raw.get("input_dir", "data/input"),
        logs_dir=paths_raw.get("logs_dir", "data/logs"),
        downloader_log=paths_raw.get("downloader_log", "data/logs/downloader.log"),
        failed_log=paths_raw.get("failed_log", "data/logs/failed_downloads.log"),
        progress_status=paths_raw.get(
            "progress_status", "data/logs/progress_status.json"
        ),
    )

    # PDF discovery
    disc_raw = raw.get("pdf_discovery", {})
    pdf_discovery = PdfDiscoveryConfig(
        link_keywords=disc_raw.get("link_keywords", []),
        file_extensions=disc_raw.get("file_extensions", [".pdf", ".PDF"]),
    )

    return DownloaderConfig(
        bse=bse,
        nse=nse,
        known_companies=known_companies,
        downloader=downloader,
        paths=paths,
        pdf_discovery=pdf_discovery,
    )
