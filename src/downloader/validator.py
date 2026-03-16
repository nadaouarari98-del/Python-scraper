"""
src/downloader/validator.py
---------------------------
PDF validation by magic bytes.

Relies on the ``%PDF`` magic signature (first 4 bytes) rather than file
extension, which avoids accepting renamed non-PDF files.
"""

from __future__ import annotations

from pathlib import Path


# The PDF magic bytes: b'%PDF'
_PDF_MAGIC = b"%PDF"
_MAGIC_LENGTH = len(_PDF_MAGIC)


def validate_pdf_bytes(data: bytes) -> bool:
    """Return ``True`` if *data* starts with the PDF magic signature.

    Args:
        data: Raw bytes (must be at least 4 bytes long).

    Returns:
        ``True`` if *data* is a valid PDF by magic bytes, ``False`` otherwise.
    """
    return data[:_MAGIC_LENGTH] == _PDF_MAGIC


def is_valid_pdf(path: str | Path) -> bool:
    """Return ``True`` if the file at *path* is a valid PDF.

    Reads only the first 4 bytes of the file to avoid loading large files
    into memory.

    Args:
        path: Filesystem path to the file.

    Returns:
        ``True`` if the file starts with ``%PDF``, ``False`` otherwise.
        Also returns ``False`` if the file does not exist or cannot be read.
    """
    file_path = Path(path)
    try:
        with file_path.open("rb") as fh:
            header = fh.read(_MAGIC_LENGTH)
        return validate_pdf_bytes(header)
    except (OSError, PermissionError):
        return False


def validate_download_bytes(data: bytes, url: str = "") -> bool:
    """Validate bytes downloaded from a URL.

    Convenience wrapper that also checks the length is non-zero.

    Args:
        data: Downloaded content bytes.
        url:  Source URL (only used in error messages; not validated here).

    Returns:
        ``True`` if the bytes represent a PDF, ``False`` otherwise.
    """
    if not data:
        return False
    return validate_pdf_bytes(data)
