"""
src/parser/pdf_parser.py
-------------------------
Central orchestrator for PDF → DataFrame conversion.

Per-PDF flow
------------
1. Get page count from pdfplumber
2. For each page (1-indexed):
   a. Try tabula  (lattice → stream)
   b. Try camelot (lattice → stream)
   c. Try pdfplumber (table → text heuristic)
   d. If page appears scanned and OCR is enabled: try pytesseract
   e. Log failures; never abort the whole PDF
3. Concatenate all page DataFrames
4. Merge multi-line address continuation rows
5. Normalize via normalizer.normalize_dataframe()
6. Write individual Excel + append to master
7. Update parser_status.json
8. Log per-PDF summary

Public API
----------
::

    from src.parser import parse_pdf, parse_all_pdfs

    df = parse_pdf("path/to/TechMahindra_IEPF_2017-18.pdf")
    merged = parse_all_pdfs("data/input/")
"""

from __future__ import annotations

import logging
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Suppress jpype warnings from tabula-py/jnius
warnings.filterwarnings('ignore', category=DeprecationWarning, module='.*jpype.*')
warnings.filterwarnings('ignore', message='.*jpype.*')

from .extractor_camelot import extract_page_camelot
from .extractor_ocr import extract_page_ocr, is_scanned_page
from .extractor_pdfplumber import extract_page_pdfplumber, get_page_count, get_page_text
from .extractor_tabula import extract_page_tabula
from .excel_writer import append_to_master, write_individual
from .normalizer import (
    extract_company_from_filename,
    extract_year_from_filename,
    map_column_name,
    detect_fy_columns,
    normalize_dataframe,
)
from .progress import increment_parser_status

_logger = logging.getLogger("parser.pdf_parser")

# Minimum rows a page must produce to be considered a partial success
_MIN_ROWS_FOR_SUCCESS = 1


# ---------------------------------------------------------------------------
# Address continuation merging
# ---------------------------------------------------------------------------

def _merge_address_continuations(df: pd.DataFrame) -> pd.DataFrame:
    """Merge rows that are continuations of a multi-line address field.

    A row is treated as an address continuation when its ``folio_no`` cell
    is empty/NaN and the previous row had a valid folio number.

    Args:
        df: Normalized (or partially normalized) DataFrame.

    Returns:
        DataFrame with continuation rows absorbed into the parent row.
    """
    if "folio_no" not in df.columns or "address" not in df.columns:
        return df

    rows = df.to_dict("records")
    merged: list[dict] = []
    last_valid_idx: int | None = None

    for row in rows:
        folio = str(row.get("folio_no", "")).strip()
        if folio and folio.lower() not in {"nan", "none", ""}:
            merged.append(row)
            last_valid_idx = len(merged) - 1
        else:
            # Continuation row — append non-empty text to parent address
            if last_valid_idx is not None:
                extra = " ".join(
                    str(v).strip()
                    for v in row.values()
                    if str(v).strip() and str(v).strip().lower() not in {"nan", "none"}
                )
                if extra:
                    current_addr = str(merged[last_valid_idx].get("address", "")).strip()
                    merged[last_valid_idx]["address"] = (
                        (current_addr + " " + extra).strip() if current_addr else extra
                    )
            # Otherwise discard (orphan continuation with no parent)

    return pd.DataFrame(merged).reset_index(drop=True) if merged else df


# ---------------------------------------------------------------------------
# Single-page extraction with fallback chain
# ---------------------------------------------------------------------------

def _is_title_or_header_page(pdf_path: str, page: int) -> bool:
    """Check if a page appears to be a title/header page with no data table.
    
    Returns True if page has very little text and no tables found.
    """
    try:
        # Check if page has substantial text content
        page_text = get_page_text(pdf_path, page)
        if not page_text or len(page_text.strip()) < 50:
            return True
        
        # Check if page has tables
        table_count = len(pdfplumber_find_tables(pdf_path, page))
        if table_count == 0 and len(page_text.strip()) < 500:
            # Likely a title page with minimal text and no data table
            return True
    except Exception:
        pass
    
    return False


def pdfplumber_find_tables(pdf_path: str, page: int) -> list:
    """Helper to find tables on a specific page using pdfplumber."""
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            if page <= len(pdf.pages):
                return pdf.pages[page - 1].find_tables()
    except Exception:
        pass
    return []


def _extract_page(
    pdf_path: str,
    page: int,
    enable_ocr: bool,
) -> tuple[pd.DataFrame | None, str]:
    """Try each extractor in sequence; return the first usable result.

    Args:
        pdf_path:   Path to the PDF.
        page:       1-indexed page number.
        enable_ocr: Whether to attempt OCR as last resort.

    Returns:
        ``(DataFrame | None, method_used)`` where *method_used* is a string
        describing which extractor succeeded (for logging).
    """
    # Level 1 — tabula
    df = extract_page_tabula(pdf_path, page)
    if df is not None and not df.empty:
        return df, "tabula"

    # Level 2 — camelot
    df = extract_page_camelot(pdf_path, page)
    if df is not None and not df.empty:
        return df, "camelot"

    # Level 3 — pdfplumber
    df = extract_page_pdfplumber(pdf_path, page)
    if df is not None and not df.empty:
        return df, "pdfplumber"

    # Level 4 — OCR (only for scanned pages)
    if enable_ocr:
        page_text = get_page_text(pdf_path, page)
        if is_scanned_page(page_text):
            _logger.info("Page %d appears scanned — attempting OCR.", page)
            df = extract_page_ocr(pdf_path, page)
            if df is not None and not df.empty:
                return df, "ocr"

    return None, "none"


# ---------------------------------------------------------------------------
# Public: parse a single PDF
# ---------------------------------------------------------------------------

def parse_pdf(
    filepath: str,
    output_dir: str = "data/output/parsed/",
    master_path: str = "data/output/master_shareholder_data.xlsx",
    enable_ocr: bool = True,
    skip_excel_write: bool = False,
) -> pd.DataFrame:
    """Parse a single PDF and return a normalized shareholder DataFrame.

    Args:
        filepath:         Path to the PDF file.
        output_dir:       Directory for per-company Excel output.
        master_path:      Path to the master Excel file.
        enable_ocr:       Whether to attempt OCR for scanned pages.
        skip_excel_write: If True, skip writing Excel files (useful in tests).

    Returns:
        Normalized :class:`pd.DataFrame` with canonical columns.
        Returns an empty DataFrame if no data could be extracted.
    """
    pdf_path = Path(filepath)
    if not pdf_path.exists():
        _logger.error("PDF not found: %s", filepath)
        increment_parser_status(total_pdfs=1, failed=1)
        return pd.DataFrame()

    filename = pdf_path.name
    company = extract_company_from_filename(filename)
    year = extract_year_from_filename(filename)

    _logger.info("=" * 60)
    _logger.info("Parsing PDF: %s", filename)
    _logger.info("  Company: %s | Year: %s", company, year)

    page_count = get_page_count(str(pdf_path))
    if page_count == 0:
        _logger.error("Could not determine page count for %s", filename)
        increment_parser_status(total_pdfs=1, failed=1)
        return pd.DataFrame()

    _logger.info("  Pages: %d", page_count)
    increment_parser_status(total_pdfs=1)

    # --- Process pages ---
    page_frames: list[pd.DataFrame] = []
    page_errors: list[int] = []
    method_counts: dict[str, int] = {}
    # Header row detected from page 1 — reused for headerless continuation pages
    page1_header: list[str] | None = None

    for page in range(1, page_count + 1):
        # Skip title/header pages that contain no data
        if _is_title_or_header_page(str(pdf_path), page):
            _logger.debug("Page %d/%d: skipping title/header page.", page, page_count)
            page_errors.append(page)
            continue
        
        try:
            raw_df, method = _extract_page(str(pdf_path), page, enable_ocr)
            if raw_df is not None and len(raw_df) >= _MIN_ROWS_FOR_SUCCESS:

                # --- Header injection for continuation pages ---
                # Many PDFs only print the header on page 1. On pages 2+
                # pdfplumber returns auto-numbered columns (col_0, col_1 ...).
                # If we have a known header and the current first row looks like
                # data (not column names), apply the saved header.
                if page > 1 and page1_header is not None:
                    first_row_vals = raw_df.iloc[0].astype(str).str.lower().tolist()
                    looks_like_header = any(
                        kw in " ".join(first_row_vals)
                        for kw in ("folio", "name", "holding", "dividend", "sr")
                    )
                    if not looks_like_header and len(page1_header) == len(raw_df.columns):
                        raw_df.columns = page1_header
                        _logger.debug(
                            "Page %d: injected page-1 header (%d cols).",
                            page, len(page1_header),
                        )

                # Normalize EACH page individually so all frames share
                # the same canonical column schema before concat.
                page_norm = normalize_dataframe(raw_df, company, filename, year)

                if not page_norm.empty:
                    page_frames.append(page_norm)
                    method_counts[method] = method_counts.get(method, 0) + 1
                    _logger.debug(
                        "Page %d/%d: %d records via %s",
                        page, page_count, len(page_norm), method,
                    )
                    # Save header from the first successful page for reuse
                    if page1_header is None:
                        # Use the raw column names from the page that worked
                        page1_header = list(raw_df.columns)
                else:
                    _logger.debug(
                        "Page %d/%d: extracted but normalized to 0 rows.", page, page_count
                    )
                    page_errors.append(page)
            else:
                _logger.debug("Page %d/%d: no data extracted.", page, page_count)
                page_errors.append(page)

        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "Page %d/%d FAILED (%s) — skipping.", page, page_count, exc
            )
            page_errors.append(page)

    if not page_frames:
        _logger.error("No data extracted from %s", filename)
        increment_parser_status(failed=1)
        return pd.DataFrame()

    # --- Concatenate already-normalized page frames ---
    # All frames share the same canonical columns → concat is safe.
    normalized = pd.concat(page_frames, ignore_index=True)
    _logger.info(
        "  Raw rows before address merge: %d | Methods: %s",
        len(normalized), method_counts,
    )

    # --- Merge multi-line addresses ---
    normalized = _merge_address_continuations(normalized)

    record_count = len(normalized)
    _logger.info(
        "  Records after normalization: %d | Pages failed: %s",
        record_count,
        page_errors or "none",
    )

    # --- Write Excel output ---
    if not skip_excel_write and not normalized.empty:
        try:
            write_individual(normalized, output_dir, company, year)
        except Exception as exc:  # noqa: BLE001
            _logger.error("Failed to write individual Excel: %s", exc)

        try:
            append_to_master(normalized, master_path)
        except Exception as exc:  # noqa: BLE001
            _logger.error("Failed to append to master Excel: %s", exc)

    # --- Update progress ---
    increment_parser_status(
        parsed=1,
        total_records=record_count,
    )

    _logger.info("Done: %s | %d records extracted.", filename, record_count)
    return normalized


# ---------------------------------------------------------------------------
# Public: parse a whole folder
# ---------------------------------------------------------------------------

def parse_all_pdfs(
    input_folder: str,
    output_dir: str = "data/output/parsed/",
    master_path: str = "data/output/master_shareholder_data.xlsx",
    enable_ocr: bool = True,
) -> pd.DataFrame:
    """Parse every PDF under *input_folder* and return the merged DataFrame.

    Processes one PDF at a time (memory-efficient). Each PDF is written to
    disk immediately; the returned DataFrame is the full combined result.

    Args:
        input_folder: Root folder to recursively search for ``*.pdf`` files.
        output_dir:   Directory for per-company Excel files.
        master_path:  Path to the master Excel file.
        enable_ocr:   Whether to attempt OCR for scanned pages.

    Returns:
        Merged :class:`pd.DataFrame` of all extracted records.
    """
    folder = Path(input_folder)
    if not folder.exists():
        _logger.error("Input folder does not exist: %s", input_folder)
        return pd.DataFrame()

    pdf_files = sorted(folder.rglob("*.pdf")) + sorted(folder.rglob("*.PDF"))
    pdf_files = list(dict.fromkeys(pdf_files))  # deduplicate

    if not pdf_files:
        _logger.warning("No PDF files found under: %s", input_folder)
        return pd.DataFrame()

    _logger.info("Found %d PDF file(s) to parse.", len(pdf_files))

    all_frames: list[pd.DataFrame] = []

    for i, pdf_path in enumerate(pdf_files, start=1):
        _logger.info("[%d/%d] %s", i, len(pdf_files), pdf_path.name)
        try:
            df = parse_pdf(
                str(pdf_path),
                output_dir=output_dir,
                master_path=master_path,
                enable_ocr=enable_ocr,
            )
            if not df.empty:
                all_frames.append(df)
        except Exception as exc:  # noqa: BLE001
            _logger.error("Unhandled error parsing %s: %s", pdf_path.name, exc)

    if not all_frames:
        return pd.DataFrame()

    merged = pd.concat(all_frames, ignore_index=True)
    _logger.info(
        "parse_all_pdfs complete: %d PDFs → %d total records.",
        len(pdf_files), len(merged),
    )
    return merged
