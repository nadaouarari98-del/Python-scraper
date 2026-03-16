"""
tests/test_excel_writer.py
---------------------------
Tests for src/parser/excel_writer.py
"""

from pathlib import Path

import pandas as pd
import pytest

from src.parser.excel_writer import append_to_master, write_individual


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "folio_no": ["TM00012345", "TM00023456"],
        "name": ["Rajesh Kumar Sharma", "Priya Nair"],
        "address": ["123 Main St, Mumbai", "456 Park Ave, Delhi"],
        "demat_account": ["IN300123456789", "IN300987654321"],
        "pan_number": ["ABCPD1234E", ""],
        "current_holding": [250, 100],
        "dividend_fy_2017_18": [1875.0, 750.0],
        "total_dividend": [1875.0, 750.0],
        "company_name": ["tech-mahindra", "tech-mahindra"],
        "source_file": ["TechMahindra_IEPF_2017-2018.pdf"] * 2,
        "year": ["2017-18"] * 2,
        "parsed_at": ["2026-03-13T15:00:00Z"] * 2,
    })


class TestWriteIndividual:
    def test_creates_xlsx_file(self, tmp_path: Path):
        df = _sample_df()
        result_path = write_individual(df, str(tmp_path), "tech-mahindra", "2017-18")
        assert Path(result_path).exists()
        assert result_path.endswith(".xlsx")

    def test_file_has_correct_columns(self, tmp_path: Path):
        df = _sample_df()
        result_path = write_individual(df, str(tmp_path), "tech-mahindra", "2017-18")
        read_back = pd.read_excel(result_path, engine="openpyxl")
        for col in ["folio_no", "name", "total_dividend"]:
            assert col in read_back.columns

    def test_correct_row_count(self, tmp_path: Path):
        df = _sample_df()
        result_path = write_individual(df, str(tmp_path), "tech-mahindra", "2017-18")
        read_back = pd.read_excel(result_path, engine="openpyxl")
        assert len(read_back) == 2

    def test_filename_contains_company_and_year(self, tmp_path: Path):
        df = _sample_df()
        result_path = write_individual(df, str(tmp_path), "tech-mahindra", "2017-18")
        assert "tech-mahindra" in Path(result_path).name
        assert "2017-18" in Path(result_path).name

    def test_creates_output_dir_if_missing(self, tmp_path: Path):
        nested = tmp_path / "deep" / "nested"
        df = _sample_df()
        result_path = write_individual(df, str(nested), "co", "2020-21")
        assert Path(result_path).exists()


class TestAppendToMaster:
    def test_creates_master_if_missing(self, tmp_path: Path):
        master = tmp_path / "master.xlsx"
        append_to_master(_sample_df(), str(master))
        assert master.exists()

    def test_master_has_correct_rows(self, tmp_path: Path):
        master = tmp_path / "master.xlsx"
        append_to_master(_sample_df(), str(master))
        df = pd.read_excel(master, engine="openpyxl")
        assert len(df) == 2

    def test_append_increases_row_count(self, tmp_path: Path):
        master = tmp_path / "master.xlsx"
        append_to_master(_sample_df(), str(master))
        # Second company's data
        df2 = _sample_df().copy()
        df2["folio_no"] = ["TM99991111", "TM99992222"]
        df2["source_file"] = "Reliance_2017-18.pdf"
        df2["company_name"] = "reliance"
        append_to_master(df2, str(master))
        combined = pd.read_excel(master, engine="openpyxl")
        assert len(combined) == 4

    def test_deduplication_prevents_double_append(self, tmp_path: Path):
        """Appending the same records twice should not duplicate them."""
        master = tmp_path / "master.xlsx"
        df = _sample_df()
        append_to_master(df, str(master))
        append_to_master(df, str(master))  # same data again
        combined = pd.read_excel(master, engine="openpyxl")
        assert len(combined) == 2  # still 2, not 4

    def test_empty_df_is_ignored(self, tmp_path: Path):
        master = tmp_path / "master.xlsx"
        append_to_master(pd.DataFrame(), str(master))
        assert not master.exists()  # nothing written
