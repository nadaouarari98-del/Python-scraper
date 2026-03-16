"""
src/downloader/progress.py
--------------------------
JSON-backed progress tracker.

Records total PDFs found, downloaded, and failed in a single JSON file
so an external dashboard can poll it without touching the log files.

Schema
------
::

    {
        "total_found": 0,
        "downloaded": 0,
        "failed": 0,
        "last_updated": "2026-03-12T21:00:00Z"
    }
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict


_DEFAULT_STATUS_PATH = Path("data/logs/progress_status.json")


class ProgressStatus(TypedDict):
    """Shape of the JSON status document."""

    total_found: int
    downloaded: int
    failed: int
    last_updated: str


def _default_status() -> ProgressStatus:
    return ProgressStatus(
        total_found=0,
        downloaded=0,
        failed=0,
        last_updated="",
    )


def load_status(status_path: str | Path | None = None) -> ProgressStatus:
    """Load the current progress status from disk.

    If the file does not exist, returns a zeroed-out default.

    Args:
        status_path: Path to the JSON status file.
                     Defaults to ``data/logs/progress_status.json``.

    Returns:
        :class:`ProgressStatus` dictionary.
    """
    path = Path(status_path or _DEFAULT_STATUS_PATH)
    if not path.exists():
        return _default_status()
    with path.open(encoding="utf-8") as fh:
        try:
            return json.load(fh)
        except json.JSONDecodeError:
            return _default_status()


def update_status(
    *,
    total_found: int | None = None,
    downloaded: int | None = None,
    failed: int | None = None,
    status_path: str | Path | None = None,
) -> ProgressStatus:
    """Atomically update one or more counters and persist to disk.

    Keyword Args:
        total_found: Absolute value to set (not a delta).
        downloaded:  Absolute value to set.
        failed:      Absolute value to set.
        status_path: Override output path.

    Returns:
        Updated :class:`ProgressStatus`.
    """
    path = Path(status_path or _DEFAULT_STATUS_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)

    current = load_status(path)

    if total_found is not None:
        current["total_found"] = total_found
    if downloaded is not None:
        current["downloaded"] = downloaded
    if failed is not None:
        current["failed"] = failed

    current["last_updated"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    with path.open("w", encoding="utf-8") as fh:
        json.dump(current, fh, indent=2)

    return current


def increment_status(
    *,
    total_found: int = 0,
    downloaded: int = 0,
    failed: int = 0,
    status_path: str | Path | None = None,
) -> ProgressStatus:
    """Increment counters by the given deltas and persist.

    Args:
        total_found: Amount to add to ``total_found``.
        downloaded:  Amount to add to ``downloaded``.
        failed:      Amount to add to ``failed``.
        status_path: Override output path.

    Returns:
        Updated :class:`ProgressStatus`.
    """
    current = load_status(status_path)
    return update_status(
        total_found=current["total_found"] + total_found,
        downloaded=current["downloaded"] + downloaded,
        failed=current["failed"] + failed,
        status_path=status_path,
    )


def reset_status(status_path: str | Path | None = None) -> ProgressStatus:
    """Reset all counters to zero and persist.

    Args:
        status_path: Override output path.

    Returns:
        Zeroed :class:`ProgressStatus`.
    """
    return update_status(
        total_found=0,
        downloaded=0,
        failed=0,
        status_path=status_path,
    )
