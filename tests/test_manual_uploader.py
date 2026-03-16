"""
tests/test_manual_uploader.py
------------------------------
Tests for src/downloader/manual_uploader.py
"""

import shutil
from pathlib import Path

import pytest

from src.downloader.manual_uploader import (
    _parse_company_and_year,
    _slugify,
    _collect_pdf_paths,
    upload_pdfs,
)


# ---------------------------------------------------------------------------
# _slugify
# ---------------------------------------------------------------------------

class TestSlugify:
    def test_basic(self):
        assert _slugify("Tech Mahindra") == "tech-mahindra"

    def test_special_chars(self):
        # & and ! are stripped; result is lowercase with no leading/trailing dash
        slug = _slugify("HDFC Bank & Finance!")
        assert slug == slug.lower()
        assert not slug.startswith("-") and not slug.endswith("-")
        assert " " not in slug

    def test_already_slug(self):
        assert _slugify("reliance") == "reliance"


# ---------------------------------------------------------------------------
# _parse_company_and_year
# ---------------------------------------------------------------------------

class TestParseCompanyAndYear:
    def test_year_detected(self):
        _, year = _parse_company_and_year("IEPF_2022_Reliance.pdf")
        assert year == "2022"

    def test_no_year_returns_unknown(self):
        _, year = _parse_company_and_year("no_year_here.pdf")
        assert year == "unknown"

    def test_company_partial_detection(self):
        """Even if company can't be fully parsed, year should be extracted."""
        _, year = _parse_company_and_year("shareholding_pattern_2023.pdf")
        assert year == "2023"

    def test_fully_unknown(self):
        company, year = _parse_company_and_year("abc.pdf")
        # company should be 'abc' or 'unknown', year should be 'unknown'
        assert year == "unknown"


# ---------------------------------------------------------------------------
# _collect_pdf_paths
# ---------------------------------------------------------------------------

class TestCollectPdfPaths:
    def test_single_file(self, tmp_path: Path):
        f = tmp_path / "x.pdf"
        f.write_bytes(b"%PDF-1.4")
        result = _collect_pdf_paths(str(f))
        assert result == [f]

    def test_directory(self, tmp_path: Path):
        (tmp_path / "a.pdf").write_bytes(b"%PDF-1.4")
        (tmp_path / "b.pdf").write_bytes(b"%PDF-1.4")
        (tmp_path / "ignore.txt").write_text("text")
        result = _collect_pdf_paths(str(tmp_path))
        assert len(result) == 2
        assert all(p.suffix.lower() == ".pdf" for p in result)

    def test_nonexistent_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            _collect_pdf_paths(str(tmp_path / "no_such_dir"))

    def test_recursive(self, tmp_path: Path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "deep.pdf").write_bytes(b"%PDF-1.4")
        result = _collect_pdf_paths(str(tmp_path))
        assert len(result) == 1


# ---------------------------------------------------------------------------
# upload_pdfs (integration-style, no network)
# ---------------------------------------------------------------------------

class TestUploadPdfs:
    def test_valid_pdf_is_copied(self, tmp_path: Path, monkeypatch):
        """A valid PDF is copied into data/input/."""
        # Create a fake src PDF
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        pdf = src_dir / "tech-mahindra_2022_report.pdf"
        pdf.write_bytes(b"%PDF-1.4\n%%EOF")

        # Redirect data/input to a temp dir so we don't pollute the real src
        out_dir = tmp_path / "data" / "input"
        out_dir.mkdir(parents=True)

        # Patch load_config to point paths at tmp_path
        from src.downloader import config as cfg_module
        from src.downloader.config import DownloaderConfig, PathsConfig

        original_load = cfg_module.load_config

        def fake_load_config(path=None):
            real_cfg = original_load(path)
            real_cfg.paths.input_dir = str(out_dir)
            real_cfg.paths.logs_dir = str(tmp_path / "data" / "logs")
            real_cfg.paths.progress_status = str(
                tmp_path / "data" / "logs" / "progress_status.json"
            )
            return real_cfg

        monkeypatch.setattr(
            "src.downloader.manual_uploader.load_config", fake_load_config
        )
        monkeypatch.setattr(
            "src.downloader.manual_uploader.increment_status",
            lambda **kw: None,
        )

        saved = upload_pdfs(str(src_dir), interactive=False)
        assert len(saved) == 1
        assert Path(saved[0]).exists()

    def test_invalid_file_is_skipped(self, tmp_path: Path, monkeypatch):
        """A file with wrong bytes is not copied."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        bad = src_dir / "not_a_pdf.pdf"
        bad.write_bytes(b"PK\x03\x04 this is a zip")

        out_dir = tmp_path / "data" / "input"
        out_dir.mkdir(parents=True)

        from src.downloader.config import load_config as real_load_config

        def fake_load_config(path=None):
            real_cfg = real_load_config(path)
            real_cfg.paths.input_dir = str(out_dir)
            real_cfg.paths.progress_status = str(tmp_path / "ps.json")
            return real_cfg

        monkeypatch.setattr(
            "src.downloader.manual_uploader.load_config", fake_load_config
        )
        monkeypatch.setattr(
            "src.downloader.manual_uploader.increment_status",
            lambda **kw: None,
        )

        saved = upload_pdfs(str(src_dir), interactive=False)
        assert saved == []
