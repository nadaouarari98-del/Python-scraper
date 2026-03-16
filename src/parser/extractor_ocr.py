"""
src/parser/extractor_ocr.py
----------------------------
Level-4 extractor: OCR fallback using pytesseract + pdf2image.

Used only when all text-based extractors return nothing, indicating
the page is likely a scanned image rather than a text-based PDF.

Dependencies (optional — install separately):
    pip install pytesseract pdf2image
    # Also requires Tesseract binary: https://github.com/UB-Mannheim/tesseract/wiki
    # And poppler for pdf2image on Windows.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

_logger = logging.getLogger("parser.ocr")

try:
    import pytesseract  # type: ignore
    _TESSERACT_AVAILABLE = True
except ImportError:
    _TESSERACT_AVAILABLE = False

try:
    from pdf2image import convert_from_path  # type: ignore
    _PDF2IMAGE_AVAILABLE = True
except ImportError:
    _PDF2IMAGE_AVAILABLE = False

_OCR_AVAILABLE = _TESSERACT_AVAILABLE and _PDF2IMAGE_AVAILABLE
if not _OCR_AVAILABLE:
    _logger.info(
        "OCR unavailable: install `pip install pytesseract pdf2image` "
        "and the Tesseract binary for scanned PDF support."
    )

# Minimum number of characters on a page to consider it text-based
_MIN_TEXT_CHARS = 20


def is_scanned_page(page_text: str) -> bool:
    """Return True if the page appears to be a scanned image.

    A page is treated as scanned if the extracted text is shorter than
    the threshold, meaning no useful text layer is present.

    Args:
        page_text: Text extracted from the PDF page (may be empty).

    Returns:
        ``True`` if the page appears to be scanned.
    """
    return len((page_text or "").strip()) < _MIN_TEXT_CHARS


def _image_to_df(image: Any) -> pd.DataFrame | None:
    """
    Run Tesseract on a PIL image and reconstruct tabular structure.

    Uses ``image_to_data()`` which returns word-level bounding boxes.
    Words on the same line (similar ``top`` coordinate) are grouped
    into pseudo-cells using horizontal clustering.
    """
    data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)

    # Collect (text, left, top, width) for non-empty words
    words: list[dict[str, Any]] = []
    for i, text in enumerate(data["text"]):
        text = str(text).strip()
        if text and int(data["conf"][i]) > 30:
            words.append({
                "text": text,
                "left": data["left"][i],
                "top":  data["top"][i],
                "width": data["width"][i],
            })

    if not words:
        return None

    # Group words into lines by proximity of `top` coordinate (±10 px)
    words.sort(key=lambda w: (w["top"], w["left"]))
    lines: list[list[dict]] = []
    current_line: list[dict] = [words[0]]

    for word in words[1:]:
        if abs(word["top"] - current_line[0]["top"]) <= 10:
            current_line.append(word)
        else:
            lines.append(sorted(current_line, key=lambda w: w["left"]))
            current_line = [word]
    lines.append(sorted(current_line, key=lambda w: w["left"]))

    if len(lines) < 2:
        return None

    # Treat first non-trivial line as header
    header_line = lines[0]
    header = [w["text"] for w in header_line]
    # Determine column x-boundaries from header positions
    col_rights = [w["left"] + w["width"] + 15 for w in header_line]

    def assign_col(left: int) -> int:
        for idx, right in enumerate(col_rights):
            if left <= right:
                return idx
        return len(col_rights) - 1

    records: list[dict[str, str]] = []
    for line in lines[1:]:
        row: dict[str, str] = {h: "" for h in header}
        for word in line:
            col_idx = assign_col(word["left"])
            if col_idx < len(header):
                row[header[col_idx]] = (row[header[col_idx]] + " " + word["text"]).strip()
        records.append(row)

    df = pd.DataFrame(records)
    return df if not df.empty and len(df.columns) >= 2 else None


def extract_page_ocr(pdf_path: str, page: int) -> pd.DataFrame | None:
    """Extract table from a scanned *page* using OCR.

    Converts the page to a PIL image using pdf2image, then runs
    Tesseract to reconstruct text, and groups words into rows/columns.

    Args:
        pdf_path: Path to the PDF file.
        page:     1-indexed page number.

    Returns:
        :class:`pd.DataFrame` of OCR-extracted rows, or ``None``.
    """
    if not _OCR_AVAILABLE:
        return None

    try:
        images = convert_from_path(
            pdf_path,
            first_page=page,
            last_page=page,
            dpi=300,
        )
        if not images:
            return None

        df = _image_to_df(images[0])
        if df is not None:
            _logger.info("OCR: page %d → %d rows extracted", page, len(df))
        return df

    except Exception as exc:  # noqa: BLE001
        _logger.warning("OCR failed on page %d: %s", page, exc)
        return None
