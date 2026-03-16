"""
src/parser/extractor_pdfplumber.py
-----------------------------------
Level-3 extractor: pdfplumber with table → regex text fallback.

pdfplumber is the most reliable for text-based PDFs. It first tries
structured table extraction; if that yields nothing, it falls back to
line-by-line text parsing using column-position heuristics and regex.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

_logger = logging.getLogger("parser.pdfplumber")

try:
    import pdfplumber  # type: ignore
    _PDFPLUMBER_AVAILABLE = True
except ImportError:
    _PDFPLUMBER_AVAILABLE = False
    _logger.warning("pdfplumber not installed — Level-3 extraction unavailable.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_useful(df: pd.DataFrame) -> bool:
    return df is not None and not df.empty and len(df.columns) >= 2 and len(df) >= 1


def _rows_to_df(rows: list[list[Any]]) -> pd.DataFrame | None:
    """Convert a list-of-rows (first row = header) to a DataFrame."""
    if not rows or len(rows) < 2:
        return None
    header = [str(h) if h is not None else f"col_{i}"
               for i, h in enumerate(rows[0])]
    data = rows[1:]
    df = pd.DataFrame(data, columns=header)
    return df if _is_useful(df) else None


def _text_to_df(text: str) -> pd.DataFrame | None:
    """
    Heuristic line-by-line parser for unstructured PDF text.

    Strategy:
    - Split into lines, skip blank/page-number lines.
    - Detect the header line by looking for known keywords.
    - Use the header line's word positions to tokenise subsequent data lines.
    - Multi-word cells are grouped by proximity.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None

    # Known header keywords (case-insensitive)
    _HEADER_KEYWORDS = re.compile(
        r"folio|investor|shareholder|name|address|demat|holding|dividend|amount|pan",
        re.IGNORECASE,
    )
    _SKIP_LINE = re.compile(
        r"^\s*\d+\s*$"                  # bare page numbers
        r"|page\s+\d+"                  # "Page 1 of 10"
        r"|^\s*[-=]{3,}\s*$"            # separator lines
        r"|printed\s+on|generated\s+on" # footers
        r"|confidential|iepf",
        re.IGNORECASE,
    )
    _AMOUNT_RE = re.compile(r"^[\d,]+(?:\.\d{1,2})?$")

    header_idx: int | None = None
    for i, line in enumerate(lines):
        if _HEADER_KEYWORDS.search(line) and len(line.split()) >= 3:
            header_idx = i
            break

    if header_idx is None:
        return None

    header_line = lines[header_idx]
    headers = re.split(r"\s{2,}", header_line.strip())
    headers = [h.strip() for h in headers if h.strip()]

    records: list[dict[str, str]] = []
    for line in lines[header_idx + 1:]:
        if _SKIP_LINE.match(line):
            continue
        # Split on 2+ whitespace characters to get cells
        cells = re.split(r"\s{2,}", line.strip())
        cells = [c.strip() for c in cells if c.strip()]
        if not cells:
            continue
        row: dict[str, str] = {}
        for j, header in enumerate(headers):
            row[header] = cells[j] if j < len(cells) else ""
        records.append(row)

    if not records:
        return None

    df = pd.DataFrame(records)
    return df if _is_useful(df) else None


# ---------------------------------------------------------------------------
# Public extractor
# ---------------------------------------------------------------------------

def extract_page_pdfplumber(pdf_path: str, page: int) -> pd.DataFrame | None:
    """Extract table data from *page* using pdfplumber.

    First tries pdfplumber's ``extract_table()``; if that returns nothing,
    falls back to raw text extraction with heuristic parsing.

    Args:
        pdf_path: Path to the PDF.
        page:     1-indexed page number.

    Returns:
        :class:`pd.DataFrame` of extracted rows, or ``None`` on failure.
    """
    if not _PDFPLUMBER_AVAILABLE:
        return None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page < 1 or page > len(pdf.pages):
                return None
            plumber_page = pdf.pages[page - 1]

            # --- Attempt 1: structured table extraction ---
            table = plumber_page.extract_table()
            if table:
                df = _rows_to_df(table)
                if _is_useful(df):
                    _logger.debug(
                        "pdfplumber table: page %d → %d rows", page, len(df)
                    )
                    return df

            # --- Attempt 2: text + heuristic parsing ---
            text = plumber_page.extract_text() or ""
            if text.strip():
                df = _text_to_df(text)
                if df is not None and _is_useful(df):
                    _logger.debug(
                        "pdfplumber text: page %d → %d rows", page, len(df)
                    )
                    return df

    except Exception as exc:  # noqa: BLE001
        _logger.debug("pdfplumber failed on page %d: %s", page, exc)

    return None


def get_page_count(pdf_path: str) -> int:
    """Return the total number of pages in the PDF.

    Args:
        pdf_path: Path to the PDF file.

    Returns:
        Page count, or 0 on error.
    """
    if not _PDFPLUMBER_AVAILABLE:
        return 0
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return len(pdf.pages)
    except Exception as exc:  # noqa: BLE001
        _logger.error("Could not open PDF %s: %s", pdf_path, exc)
        return 0


def get_page_text(pdf_path: str, page: int) -> str:
    """Return raw text of *page* (used for scanned-page detection)."""
    if not _PDFPLUMBER_AVAILABLE:
        return ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if 1 <= page <= len(pdf.pages):
                return pdf.pages[page - 1].extract_text() or ""
    except Exception:  # noqa: BLE001
        pass
    return ""
