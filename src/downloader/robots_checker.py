"""
src/downloader/robots_checker.py
---------------------------------
robots.txt compliance checker wrapping :mod:`urllib.robotparser`.

Fetches and caches each domain's ``robots.txt`` and exposes a simple
:meth:`RobotsChecker.can_fetch` method.
"""

from __future__ import annotations

import threading
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

from .logger import get_logger

_logger = get_logger(__name__)


class RobotsChecker:
    """Check robots.txt rules before fetching a URL.

    Robots files are cached per domain for the lifetime of this instance.

    Args:
        user_agent: The ``User-agent`` token to check rules against.
        timeout:    HTTP timeout for fetching ``robots.txt``.

    Example::

        checker = RobotsChecker(user_agent="Googlebot")
        if checker.can_fetch("https://example.com/page"):
            ...
    """

    def __init__(self, user_agent: str = "*", timeout: int = 10) -> None:
        self._user_agent = user_agent
        self._timeout = timeout
        self._cache: dict[str, RobotFileParser] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _robots_url(url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    def _fetch_parser(self, robots_url: str) -> RobotFileParser:
        """Download and parse robots.txt, returning a :class:`RobotFileParser`."""
        parser = RobotFileParser()
        parser.set_url(robots_url)
        try:
            resp = requests.get(
                robots_url,
                timeout=self._timeout,
                headers={"User-Agent": self._user_agent},
            )
            resp.raise_for_status()
            parser.parse(resp.text.splitlines())
            _logger.debug("Fetched robots.txt from %s", robots_url)
        except Exception as exc:  # noqa: BLE001
            # If we can't fetch robots.txt, assume everything is allowed
            _logger.warning(
                "Could not fetch robots.txt from %s (%s). Assuming allow-all.",
                robots_url,
                exc,
            )
            parser.allow_all = True  # type: ignore[attr-defined]
        return parser

    def can_fetch(self, url: str) -> bool:
        """Return ``True`` if the configured user-agent may fetch *url*.

        Args:
            url: The URL to check.

        Returns:
            ``True`` if allowed (or robots.txt was unreachable), ``False`` if
            disallowed by a ``Disallow`` rule.
        """
        robots_url = self._robots_url(url)
        domain = urlparse(url).netloc

        with self._lock:
            if domain not in self._cache:
                self._cache[domain] = self._fetch_parser(robots_url)
            parser = self._cache[domain]

        allowed: bool = parser.can_fetch(self._user_agent, url)
        if not allowed:
            _logger.info("robots.txt disallows: %s", url)
        return allowed
