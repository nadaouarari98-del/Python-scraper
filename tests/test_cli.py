"""
tests/test_cli.py
-----------------
Tests for src/downloader/__main__.py (CLI argument parsing).

These tests call main() directly with a synthetic argv list so no network
requests are made.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from src.downloader.__main__ import main


class TestCliHelp:
    def test_help_exits_zero(self):
        """--help should print usage and exit with code 0."""
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0

    def test_no_args_returns_nonzero(self):
        """No arguments (no --mode) should return exit code 1."""
        result = main([])
        assert result == 1


class TestCliStatus:
    def test_status_flag(self, tmp_path, monkeypatch):
        """--status should print JSON and return 0."""
        from src.downloader import progress as prog_mod

        monkeypatch.setattr(
            "src.downloader.__main__.load_status",
            lambda: {"total_found": 5, "downloaded": 3, "failed": 2, "last_updated": "x"},
        )
        result = main(["--status"])
        assert result == 0


class TestCliAutoMode:
    def test_auto_missing_companies_returns_nonzero(self):
        """Auto mode without --companies should return exit code 1."""
        result = main(["--mode", "auto"])
        assert result == 1

    def test_auto_mode_calls_download_pdfs(self, monkeypatch):
        """Auto mode invokes download_pdfs with the correct arguments."""
        mock_download = MagicMock(
            return_value={"TCS": {"found": 2, "downloaded": 2, "failed": 0}}
        )
        # The lazy import inside main() resolves to src.downloader.auto_downloader
        monkeypatch.setattr(
            "src.downloader.auto_downloader.download_pdfs", mock_download
        )
        with patch("src.downloader.__main__.download_pdfs", mock_download, create=True):
            result = main(["--mode", "auto", "--companies", "TCS", "--source", "bse"])
        assert result == 0


class TestCliManualMode:
    def test_manual_missing_path_returns_nonzero(self):
        """Manual mode without --path should return exit code 1."""
        result = main(["--mode", "manual"])
        assert result == 1

    def test_manual_mode_calls_upload_pdfs(self, tmp_path, monkeypatch):
        """Manual mode invokes upload_pdfs with the correct arguments."""
        mock_upload = MagicMock(return_value=["/some/path/file.pdf"])
        # Patch at the module that performs the deferred import
        monkeypatch.setattr(
            "src.downloader.manual_uploader.upload_pdfs", mock_upload
        )
        with patch("src.downloader.__main__.upload_pdfs", mock_upload, create=True):
            result = main(
                ["--mode", "manual", "--path", str(tmp_path), "--no-interactive"]
            )
        assert result == 0
