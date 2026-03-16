"""
src/parser/extractor_camelot.py
--------------------------------
Level-2 extractor: camelot-py with lattice → stream fallback.

camelot is more accurate than tabula for complex tables but requires
the ``cv2`` (OpenCV) dependency.
"""

from __future__ import annotations

import logging

import pandas as pd

_logger = logging.getLogger("parser.camelot")

try:
    import camelot  # type: ignore
    _CAMELOT_AVAILABLE = True
except ImportError:
    _CAMELOT_AVAILABLE = False
    _logger.debug("camelot-py not installed — Level-2 extraction unavailable.")


def _is_useful(df: pd.DataFrame) -> bool:
    return df is not None and not df.empty and len(df.columns) >= 2 and len(df) >= 1


def extract_page_camelot(pdf_path: str, page: int) -> pd.DataFrame | None:
    """Extract table from *page* using camelot-py.

    Tries ``lattice`` flavour first, then ``stream``.

    Args:
        pdf_path: Path to the PDF file.
        page:     1-indexed page number.

    Returns:
        Combined :class:`pd.DataFrame` of all extracted tables, or ``None``.
    """
    if not _CAMELOT_AVAILABLE:
        return None

    for flavor in ("lattice", "stream"):
        try:
            tables = camelot.read_pdf(
                pdf_path,
                pages=str(page),
                flavor=flavor,
            )
            if not tables or len(tables) == 0:
                _logger.debug("camelot %s: no tables on page %d", flavor, page)
                continue

            frames = [t.df for t in tables if _is_useful(t.df)]
            if not frames:
                continue

            # Promote first row as header if it looks like one
            combined_frames = []
            for df in frames:
                df = df.copy()
                # Use first row as header if it contains text
                first_row = df.iloc[0].astype(str)
                if first_row.str.len().mean() > 2:
                    df.columns = first_row.values
                    df = df.iloc[1:].reset_index(drop=True)
                combined_frames.append(df)

            combined = pd.concat(combined_frames, ignore_index=True)
            if _is_useful(combined):
                _logger.debug(
                    "camelot %s: page %d → %d rows, %d cols",
                    flavor, page, len(combined), len(combined.columns),
                )
                return combined

        except Exception as exc:  # noqa: BLE001
            _logger.debug("camelot %s failed on page %d: %s", flavor, page, exc)

    return None
