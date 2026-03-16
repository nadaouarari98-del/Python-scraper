import time
import threading
from collections import defaultdict


class RateLimiter:
    def __init__(self):
        self._locks = defaultdict(threading.Lock)
        self._last_request = defaultdict(float)

    def wait(self, domain: str, min_interval: float = 1.0):
        """Wait if necessary to maintain minimum interval between requests to a domain."""
        with self._locks[domain]:
            elapsed = time.time() - self._last_request[domain]
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            self._last_request[domain] = time.time()


GLOBAL_RATE_LIMITER = RateLimiter()
