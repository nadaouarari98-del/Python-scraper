"""
src/parser/progress.py
-----------------------
Parser-specific JSON progress tracker.

Stored at ``data/logs/parser_status.json`` so the dashboard can poll it.

Schema
------
::

    {
        "total_pdfs": 0,
        "parsed": 0,
        "failed": 0,
        "total_records": 0,
        "last_updated": "2026-03-13T15:00:00Z"
    }
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict

_DEFAULT_PATH = Path("data/logs/parser_status.json")


class ParserStatus(TypedDict):
    total_pdfs: int
    parsed: int
    failed: int
    total_records: int
    last_updated: str


def _default() -> ParserStatus:
    return ParserStatus(
        total_pdfs=0, parsed=0, failed=0,
        total_records=0, last_updated="",
    )


def load_parser_status(path: str | Path | None = None) -> ParserStatus:
    """Load current parser status from disk, returning zeros if missing."""
    p = Path(path or _DEFAULT_PATH)
    if not p.exists():
        return _default()
    try:
        with p.open(encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError:
        return _default()


def update_parser_status(
    *,
    total_pdfs: int | None = None,
    parsed: int | None = None,
    failed: int | None = None,
    total_records: int | None = None,
    path: str | Path | None = None,
) -> ParserStatus:
    """Set one or more counters to absolute values and persist."""
    p = Path(path or _DEFAULT_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    current = load_parser_status(p)
    if total_pdfs is not None:
        current["total_pdfs"] = total_pdfs
    if parsed is not None:
        current["parsed"] = parsed
    if failed is not None:
        current["failed"] = failed
    if total_records is not None:
        current["total_records"] = total_records
    current["last_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with p.open("w", encoding="utf-8") as fh:
        json.dump(current, fh, indent=2)
    return current


def increment_parser_status(
    *,
    total_pdfs: int = 0,
    parsed: int = 0,
    failed: int = 0,
    total_records: int = 0,
    path: str | Path | None = None,
) -> ParserStatus:
    """Add deltas to current counters and persist."""
    c = load_parser_status(path)
    return update_parser_status(
        total_pdfs=c["total_pdfs"] + total_pdfs,
        parsed=c["parsed"] + parsed,
        failed=c["failed"] + failed,
        total_records=c["total_records"] + total_records,
        path=path,
    )
