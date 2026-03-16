"""
src/downloader/rate_limiter.py
-------------------------------
Per-domain rate limiter (max 1 request per second per domain by default).

Uses a simple dictionary that records the last-request timestamp for each
domain and sleeps the necessary amount before allowing the next request.
Thread-safe via ``threading.Lock``.
"""

from __future__ import annotations

import threading
import time
from urllib.parse import urlparse


class RateLimiter:
    """Enforce a minimum delay between successive requests to the same domain.

    Args:
        min_interval: Minimum seconds to wait between two requests to the
                      same domain.  Defaults to 1.0 second.

    Example::

        limiter = RateLimiter(min_interval=1.0)
        for url in urls:
            limiter.wait(url)
            resp = requests.get(url)
    """

    def __init__(self, min_interval: float = 1.0) -> None:
        self._min_interval = min_interval
        self._last_request: dict[str, float] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _domain(url: str) -> str:
        """Extract the netloc (domain) from a URL string."""
        parsed = urlparse(url)
        return parsed.netloc or url

    def wait(self, url: str) -> None:
        """Block until the rate limit for *url*'s domain has elapsed.

        Args:
            url: The URL about to be requested.  Only the domain portion
                 is used for bucketing.
        """
        domain = self._domain(url)
        with self._lock:
            last = self._last_request.get(domain, 0.0)
            elapsed = time.time() - last
            sleep_for = self._min_interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._last_request[domain] = time.time()

    def set_interval(self, seconds: float) -> None:
        """Update the minimum interval (thread-safe).

        Args:
            seconds: New minimum gap in seconds.
        """
        with self._lock:
            self._min_interval = seconds
