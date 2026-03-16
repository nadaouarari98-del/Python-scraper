"""
src/parser/extractor_tabula.py
-------------------------------
Level-1 extractor: tabula-py with lattice → stream fallback.

tabula works best on PDFs with clearly drawn table borders.
"""

from __future__ import annotations

import logging

import pandas as pd

_logger = logging.getLogger("parser.tabula")

try:
    import tabula  # type: ignore
    _TABULA_AVAILABLE = True
except ImportError:
    _TABULA_AVAILABLE = False
    _logger.debug("tabula-py not installed — Level-1 extraction unavailable.")


def _is_useful(df: pd.DataFrame) -> bool:
    """Return True if the DataFrame has at least 2 columns and 1 data row."""
    return df is not None and not df.empty and len(df.columns) >= 2 and len(df) >= 1


def extract_page_tabula(pdf_path: str, page: int) -> pd.DataFrame | None:
    """Extract table from *page* using tabula-py.

    Tries ``lattice`` mode first; falls back to ``stream`` mode if the
    result is empty or has fewer than 2 columns.

    Args:
        pdf_path: Absolute or relative path to the PDF.
        page:     1-indexed page number.

    Returns:
        Concatenated :class:`pd.DataFrame` from all tables found on the page,
        or ``None`` if tabula is unavailable or extraction fails.
    """
    if not _TABULA_AVAILABLE:
        return None

    for lattice in (True, False):
        mode = "lattice" if lattice else "stream"
        try:
            tables: list[pd.DataFrame] = tabula.read_pdf(
                pdf_path,
                pages=page,
                multiple_tables=True,
                lattice=lattice,
                stream=not lattice,
                pandas_options={"header": 0},
                silent=True,
            )
            if not tables:
                _logger.debug("tabula %s: no tables on page %d", mode, page)
                continue

            combined = pd.concat(
                [t for t in tables if _is_useful(t)], ignore_index=True
            )
            if _is_useful(combined):
                _logger.debug(
                    "tabula %s: page %d → %d rows, %d cols",
                    mode, page, len(combined), len(combined.columns),
                )
                return combined

        except Exception as exc:  # noqa: BLE001
            _logger.debug("tabula %s failed on page %d: %s", mode, page, exc)

    return None
