"""
src/downloader/logger.py
------------------------
Centralised logging setup for the downloader module.

Creates a named logger that writes to:
  - Console (level: INFO)
  - data/logs/downloader.log (level: DEBUG, rotating, 5 MB × 3 backups)
"""

import logging
import logging.handlers
import os
from pathlib import Path


_LOG_DIR = Path("data/logs")
_LOG_FILE = _LOG_DIR / "downloader.log"
_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
_BACKUP_COUNT = 3

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str = "downloader") -> logging.Logger:
    """Return a configured logger.

    The first call for a given *name* configures handlers; subsequent
    calls return the same logger without duplicating handlers.

    Args:
        name: Logger name (usually ``__name__`` of the calling module).

    Returns:
        Configured :class:`logging.Logger` instance.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured — avoid duplicate handlers
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # --- Console handler (INFO+) ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # --- Rotating file handler (DEBUG+) ---
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        _LOG_FILE,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
