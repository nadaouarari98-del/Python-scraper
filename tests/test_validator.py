"""
tests/test_validator.py
-----------------------
Tests for src/downloader/validator.py
"""

import tempfile
from pathlib import Path

import pytest

from src.downloader.validator import (
    is_valid_pdf,
    validate_download_bytes,
    validate_pdf_bytes,
)

# ---------------------------------------------------------------------------
# validate_pdf_bytes
# ---------------------------------------------------------------------------

class TestValidatePdfBytes:
    def test_valid_pdf_magic(self):
        """Standard PDF header is recognised."""
        assert validate_pdf_bytes(b"%PDF-1.4 some content") is True

    def test_valid_pdf_exact_magic(self):
        """Exactly four magic bytes are sufficient."""
        assert validate_pdf_bytes(b"%PDF") is True

    def test_invalid_empty_bytes(self):
        """Empty bytes are not a valid PDF."""
        assert validate_pdf_bytes(b"") is False

    def test_invalid_png_bytes(self):
        """PNG magic bytes are rejected."""
        assert validate_pdf_bytes(b"\x89PNG\r\n") is False

    def test_invalid_text_bytes(self):
        """Plaintext content is rejected."""
        assert validate_pdf_bytes(b"Hello, world!") is False

    def test_invalid_short_bytes(self):
        """Only 3 bytes cannot match the 4-byte magic."""
        assert validate_pdf_bytes(b"%PD") is False


# ---------------------------------------------------------------------------
# is_valid_pdf
# ---------------------------------------------------------------------------

class TestIsValidPdf:
    def test_valid_pdf_file(self, tmp_path: Path):
        """A file starting with %PDF- is accepted."""
        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"%PDF-1.7\n%%EOF")
        assert is_valid_pdf(pdf) is True

    def test_invalid_file_wrong_content(self, tmp_path: Path):
        """A .pdf extension with wrong content is rejected."""
        fake = tmp_path / "not_a_pdf.pdf"
        fake.write_bytes(b"PK\x03\x04")  # ZIP magic
        assert is_valid_pdf(fake) is False

    def test_nonexistent_file(self, tmp_path: Path):
        """Non-existent path returns False (no exception)."""
        assert is_valid_pdf(tmp_path / "missing.pdf") is False

    def test_string_path_accepted(self, tmp_path: Path):
        """String paths are accepted as well as Path objects."""
        pdf = tmp_path / "s.pdf"
        pdf.write_bytes(b"%PDF-1.4")
        assert is_valid_pdf(str(pdf)) is True


# ---------------------------------------------------------------------------
# validate_download_bytes
# ---------------------------------------------------------------------------

class TestValidateDownloadBytes:
    def test_valid_download(self):
        assert validate_download_bytes(b"%PDF-1.4 body") is True

    def test_empty_download(self):
        """Empty response is rejected."""
        assert validate_download_bytes(b"") is False

    def test_html_response(self):
        """An HTML error page masquerading as a PDF is rejected."""
        assert validate_download_bytes(b"<html><body>404</body></html>") is False
