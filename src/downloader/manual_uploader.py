"""
src/downloader/manual_uploader.py
-----------------------------------
MODE 2 — Manual PDF upload from a local folder or file path.

Workflow
--------
1. Accept a path (file or directory) from CLI or function call.
2. Walk the path and collect all ``.pdf`` / ``.PDF`` files.
3. Validate each file via magic bytes (``%PDF`` header).
4. Parse ``{company_name}`` and ``{year}`` from the filename using regex.
   If they cannot be detected automatically, prompt the user or mark as
   ``unknown``.
5. Copy valid PDFs to::

       data/input/{company_slug}/{year}/{company_slug}_{year}_{original}.pdf

6. Log all actions; skip invalid files with a warning.

Public API
----------
::

    from src.downloader import upload_pdfs

    saved_paths = upload_pdfs("/path/to/my/pdfs/")
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

from .config import load_config
from .logger import get_logger
from .progress import increment_status
from .validator import is_valid_pdf

_logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

# Matches a 4-digit year (2000-2029) anywhere in the filename
_YEAR_RE = re.compile(r"(20[0-2]\d)")

# Matches common Indian company name abbreviations / patterns.
# Falls back to using the stem's first "word" segment.
_COMPANY_RE = re.compile(
    r"^([A-Za-z][A-Za-z0-9 &_\-]{2,40}?)[\s_\-]+(?:20[0-2]\d|IEPF|iepf|share|annual)",
    re.IGNORECASE,
)


def _slugify(name: str) -> str:
    """Convert a name to a lowercase, filesystem-safe slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def _parse_company_and_year(filename: str) -> tuple[str, str]:
    """Attempt to extract company name and year from *filename*.

    Args:
        filename: The original file name (without directory, with extension).

    Returns:
        ``(company_slug, year)`` — either detected or ``"unknown"``.
    """
    stem = Path(filename).stem  # strip extension

    # Year
    year_match = _YEAR_RE.search(stem)
    year = year_match.group(1) if year_match else "unknown"

    # Company
    company_match = _COMPANY_RE.match(stem.replace("_", " ").replace("-", " "))
    if company_match:
        raw_company = company_match.group(1).strip()
        company_slug = _slugify(raw_company)
    else:
        # Use the first hyphen/underscore-delimited token if ≥3 chars
        parts = re.split(r"[\s_\-]+", stem)
        candidate = parts[0] if parts and len(parts[0]) >= 3 else ""
        company_slug = _slugify(candidate) if candidate else "unknown"

    return company_slug, year


def _prompt_for_metadata(
    filename: str, company_slug: str, year: str, interactive: bool
) -> tuple[str, str]:
    """If running interactively, ask user to confirm or supply missing metadata.

    Args:
        filename:     Original filename (for display).
        company_slug: Auto-detected slug (may be ``"unknown"``).
        year:         Auto-detected year (may be ``"unknown"``).
        interactive:  Whether to prompt on stdin.

    Returns:
        ``(company_slug, year)`` — confirmed or typed by user.
    """
    if not interactive:
        return company_slug, year

    if company_slug == "unknown" or year == "unknown":
        print(f"\n  File: {filename}")
        if company_slug == "unknown":
            entered = input("    Company name not detected. Enter company name (or leave blank for 'unknown'): ").strip()
            company_slug = _slugify(entered) if entered else "unknown"
        if year == "unknown":
            entered = input("    Year not detected. Enter year (e.g. 2023, or leave blank for 'unknown'): ").strip()
            year = entered if re.match(r"^20[0-2]\d$", entered) else "unknown"

    return company_slug, year


def _collect_pdf_paths(path: str | Path) -> list[Path]:
    """Collect all ``.pdf`` files under *path* (file or directory).

    Args:
        path: A file path or directory path.

    Returns:
        Sorted list of :class:`Path` objects.

    Raises:
        FileNotFoundError: If *path* does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Path does not exist: {p}")

    if p.is_file():
        return [p]

    # Walk directory recursively
    return sorted(
        f
        for f in p.rglob("*")
        if f.is_file() and f.suffix.lower() == ".pdf"
    )


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------

def upload_pdfs(
    path: str,
    interactive: bool = True,
    config_path: str | None = None,
) -> list[str]:
    """MODE 2 — Copy local PDF files into the project's input directory.

    Validates PDFs by magic bytes, auto-detects company and year from
    filename, and copies them to ``data/input/{company}/{year}/`` with a
    standardised name.

    Args:
        path:         Path to a single PDF file or a folder containing PDFs.
        interactive:  If ``True`` (default), prompts the user on stdin when
                      company name or year cannot be auto-detected.
                      Set to ``False`` in non-interactive/batch usage.
        config_path:  Optional override for ``sources.yaml`` path.

    Returns:
        List of absolute destination paths for every successfully copied PDF.

    Example::

        saved = upload_pdfs("/home/user/downloads/iepf/", interactive=False)
    """
    cfg = load_config(config_path)
    input_dir = Path(cfg.paths.input_dir)

    candidate_paths = _collect_pdf_paths(path)
    _logger.info("Found %d candidate file(s) in '%s'", len(candidate_paths), path)

    saved_paths: list[str] = []

    for src_path in candidate_paths:
        _logger.debug("Checking: %s", src_path)

        # Magic bytes validation
        if not is_valid_pdf(src_path):
            _logger.warning(
                "Skipping (not a valid PDF by magic bytes): %s", src_path
            )
            continue

        # Detect metadata
        company_slug, year = _parse_company_and_year(src_path.name)
        company_slug, year = _prompt_for_metadata(
            src_path.name, company_slug, year, interactive
        )

        # Build destination path
        dest_dir = input_dir / company_slug / year
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest_filename = f"{company_slug}_{year}_{src_path.name}"
        # Sanitise any problematic characters
        dest_filename = re.sub(r"[^\w.\-]", "_", dest_filename)
        dest_path = dest_dir / dest_filename

        if dest_path.exists():
            _logger.info("Already exists, skipping: %s", dest_path)
            saved_paths.append(str(dest_path))
            continue

        shutil.copy2(src_path, dest_path)
        size_kb = src_path.stat().st_size / 1024
        _logger.info(
            "Copied | src=%s | dest=%s | size=%.1f KB",
            src_path.name,
            dest_path,
            size_kb,
        )
        saved_paths.append(str(dest_path))
        increment_status(downloaded=1)

    total = len(candidate_paths)
    copied = len(saved_paths)
    skipped = total - copied
    _logger.info(
        "Upload complete | total=%d | copied=%d | skipped=%d",
        total,
        copied,
        skipped,
    )

    return saved_paths
