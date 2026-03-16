"""
tests/test_progress.py
-----------------------
Tests for src/downloader/progress.py
"""

import json
from pathlib import Path

import pytest

from src.downloader.progress import (
    increment_status,
    load_status,
    reset_status,
    update_status,
)


@pytest.fixture()
def status_file(tmp_path: Path) -> Path:
    """Return a temporary path for the progress status JSON."""
    return tmp_path / "progress_status.json"


class TestLoadStatus:
    def test_missing_file_returns_zeros(self, status_file: Path):
        """Loading from a non-existent file returns zeroed defaults."""
        status = load_status(status_file)
        assert status["total_found"] == 0
        assert status["downloaded"] == 0
        assert status["failed"] == 0

    def test_corrupted_json_returns_zeros(self, status_file: Path):
        """Corrupted JSON returns zeroed defaults gracefully."""
        status_file.write_text("}{invalid json}", encoding="utf-8")
        status = load_status(status_file)
        assert status["total_found"] == 0

    def test_valid_file_is_loaded(self, status_file: Path):
        """An existing valid file is returned correctly."""
        data = {"total_found": 5, "downloaded": 3, "failed": 2, "last_updated": "x"}
        status_file.write_text(json.dumps(data), encoding="utf-8")
        status = load_status(status_file)
        assert status["total_found"] == 5
        assert status["downloaded"] == 3
        assert status["failed"] == 2


class TestUpdateStatus:
    def test_update_creates_file(self, status_file: Path):
        update_status(total_found=10, status_path=status_file)
        assert status_file.exists()

    def test_update_values_persisted(self, status_file: Path):
        update_status(total_found=4, downloaded=3, failed=1, status_path=status_file)
        loaded = load_status(status_file)
        assert loaded["total_found"] == 4
        assert loaded["downloaded"] == 3
        assert loaded["failed"] == 1

    def test_partial_update_preserves_other_fields(self, status_file: Path):
        """Updating only one field doesn't wipe others."""
        update_status(total_found=10, downloaded=8, failed=2, status_path=status_file)
        update_status(failed=3, status_path=status_file)
        loaded = load_status(status_file)
        assert loaded["total_found"] == 10
        assert loaded["downloaded"] == 8
        assert loaded["failed"] == 3

    def test_last_updated_is_iso_format(self, status_file: Path):
        update_status(total_found=1, status_path=status_file)
        loaded = load_status(status_file)
        ts: str = loaded["last_updated"]
        assert "T" in ts and ts.endswith("Z")


class TestIncrementStatus:
    def test_increments_from_zero(self, status_file: Path):
        increment_status(downloaded=1, status_path=status_file)
        increment_status(downloaded=1, failed=1, status_path=status_file)
        loaded = load_status(status_file)
        assert loaded["downloaded"] == 2
        assert loaded["failed"] == 1


class TestResetStatus:
    def test_reset_zeroes_counters(self, status_file: Path):
        update_status(total_found=99, downloaded=50, failed=10, status_path=status_file)
        reset_status(status_file)
        loaded = load_status(status_file)
        assert loaded["total_found"] == 0
        assert loaded["downloaded"] == 0
        assert loaded["failed"] == 0
