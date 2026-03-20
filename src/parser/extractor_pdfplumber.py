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


def _parse_lic_format(text: str) -> pd.DataFrame | None:
    """
    Parse LIC (Life Insurance Corporation) PDF format.
    
    LIC format sequence: Serial Number -> ID -> Name -> Address -> Pincode -> 
    Folio -> Amount -> Shares -> Date
    
    Example patterns:
    - Long digit ID: 1207780000039349 (16 digits)
    - Uppercase Name followed by Address
    - Pincode: 5 digit codes
    - Amount and Shares columns
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None
    
    records = []
    
    # Pattern to detect LIC record line: starts with optional SR_NO, then 16-digit ID
    lic_line_pattern = re.compile(
        r"^\s*(\d{1,5})\s+"  # Sr. No (optional, 1-5 digits)
        r"(\d{14,16})\s+"    # ID (14-16 digits, typically 1207780000039349)
        r"([A-Z][A-Z\s]{2,}?)\s{2,}"  # NAME (uppercase, 3+ chars)
    )
    
    pincode_pattern = re.compile(r"\b(\d{6})\b")
    amount_pattern = re.compile(r"[\d,]+(?:\.\d{2})?")
    
    for i, line in enumerate(lines):
        match = lic_line_pattern.match(line)
        if match:
            sr_no = match.group(1)
            lic_id = match.group(2)
            name = match.group(3).strip()
            
            # Try to extract remaining fields from this line and next lines
            remainder = line[match.end():].strip()
            
            # Extract address (typically next continuous text)
            address = ""
            pincode = ""
            folio = ""
            amount = ""
            shares = ""
            
            # Look for pincode in remainder
            pin_match = pincode_pattern.search(remainder)
            if pin_match:
                pincode = pin_match.group(1)
                # Address is everything before pincode
                address = remainder[:pin_match.start()].strip()
            else:
                address = remainder
            
            # Look in next few lines for folio, amount, shares
            for j in range(i + 1, min(i + 3, len(lines))):
                next_line = lines[j].strip()
                # Skip if this looks like another header/data row
                if re.match(r"^\d{14,16}\s+[A-Z]", next_line):
                    break
                
                # Try to extract folio, amount, shares from subsequent lines
                parts = next_line.split()
                for part in parts:
                    if re.match(r"^\d{8,12}$", part) and not folio:
                        folio = part
                    elif re.match(r"^[\d,]+(?:\.\d{2})?$", part) and not amount:
                        amount = part
                    elif re.match(r"^\d{1,8}$", part) and not shares:
                        shares = part
            
            record = {
                "sr_no": sr_no,
                "id": lic_id,
                "name": name,
                "address": address,
                "pincode": pincode,
                "folio_no": folio,
                "amount": amount,
                "shares": shares,
            }
            records.append(record)
    
    if records:
        df = pd.DataFrame(records)
        return df if _is_useful(df) else None
    
    return None


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
    falls back to raw text extraction with heuristic parsing, then LIC format.

    Args:
        pdf_path: Path to the PDF.
        page:     1-indexed page number.

    Returns:
        :class:`pd.DataFrame` of extracted rows, or ``None`` on failure.
    """
    if not _PDFPLUMBER_AVAILABLE:
        return None


    def map_headers(header_row):
        """Map detected headers to canonical column names using keywords."""
        header_map = {}
        header_keywords = {
            'folio_no': ['folio', 'dp id', 'client id', 'folio no', 'dpid', 'clientid'],
            'name': ['name of member', 'shareholder', 'beneficiary', 'name'],
            'amount': ['amount', 'dividend', 'unclaimed'],
            'shares': ['shares', 'no of shares', 'holding'],
            'id': ['id', 'client id', 'dp id'],
        }
        for idx, col in enumerate(header_row):
            col_l = col.lower()
            for canon, keys in header_keywords.items():
                if any(k in col_l for k in keys):
                    header_map[idx] = canon
                    break
        return header_map

    def lic_line_parser(text):
        """Parse lines for LIC format: ID followed by Name, even if on same line."""
        import re
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        records = []
        lic_pattern = re.compile(r"(\d{14,16})\s+([A-Za-z][A-Za-z\s]+)")
        for line in lines:
            m = lic_pattern.search(line)
            if m:
                lic_id = m.group(1)
                name = m.group(2).strip()
                records.append({'id': lic_id, 'name': name})
        if records:
            return pd.DataFrame(records)
        return None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page < 1 or page > len(pdf.pages):
                return None
            plumber_page = pdf.pages[page - 1]

            # --- Attempt 1: structured table extraction ---
            table = plumber_page.extract_table()
            if table and len(table) > 1:
                header_row = [str(h or '').strip() for h in table[0]]
                header_map = map_headers(header_row)
                records = []
                for row in table[1:]:
                    record = {}
                    for idx, val in enumerate(row):
                        canon = header_map.get(idx)
                        if canon:
                            record[canon] = val.strip() if isinstance(val, str) else val
                        else:
                            record[header_row[idx]] = val.strip() if isinstance(val, str) else val
                    records.append(record)
                df = pd.DataFrame(records)
                if _is_useful(df):
                    _logger.debug(
                        "pdfplumber table+header map: page %d → %d rows", page, len(df)
                    )
                    return df

            # --- Attempt 2: LIC format parsing ---
            text = plumber_page.extract_text() or ""
            if text.strip():
                df = _parse_lic_format(text)
                if df is not None and _is_useful(df):
                    _logger.debug(
                        "pdfplumber LIC format: page %d → %d rows", page, len(df)
                    )
                    return df
                # Robust fallback: scan for ID+Name on same line
                df2 = lic_line_parser(text)
                if df2 is not None and _is_useful(df2):
                    _logger.debug(
                        "pdfplumber LIC fallback: page %d → %d rows", page, len(df2)
                    )
                    return df2

            # --- Attempt 3: text + heuristic parsing ---
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
