"""
tests/test_merger.py
--------------------
Comprehensive tests for the merger module.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from src.processor.merger import ColumnMapper, DataNormalizer, Merger, merge_all


class TestColumnMapper:
    """Tests for the ColumnMapper class."""

    def test_exact_mapping(self):
        """Test exact column name mapping."""
        mapper = ColumnMapper()
        result = mapper.map_column("Folio No")
        assert result == "folio_no"

    def test_case_insensitive_mapping(self):
        """Test case-insensitive mapping."""
        mapper = ColumnMapper()
        result = mapper.map_column("FOLIO NO")
        assert result == "folio_no"

    def test_fuzzy_mapping(self):
        """Test fuzzy matching for similar column names."""
        mapper = ColumnMapper()
        result = mapper.map_column("Folio Number")
        assert result == "folio_no"

    def test_name_variants(self):
        """Test mapping of different name column variants."""
        mapper = ColumnMapper()
        assert mapper.map_column("Name") == "name"
        assert mapper.map_column("Shareholder Name") == "name"
        assert mapper.map_column("Investor Name") == "name"

    def test_holding_variants(self):
        """Test mapping of different holding column variants."""
        mapper = ColumnMapper()
        assert mapper.map_column("Current Holding") == "current_holding"
        assert mapper.map_column("No. of Shares") == "current_holding"
        assert mapper.map_column("Shares Held") == "current_holding"

    def test_dividend_column_detection(self):
        """Test detection of dividend columns by FY pattern."""
        mapper = ColumnMapper()
        result = mapper.map_column("Final Dividend FY 2017-18")
        assert result == "dividend_fy_2017_18"

    def test_unmapped_column(self):
        """Test handling of unmapped columns."""
        mapper = ColumnMapper()
        result = mapper.map_column("SomeRareColumn")
        assert result is None
        assert "SomeRareColumn" in mapper.unmapped_columns


class TestDataNormalizer:
    """Tests for the DataNormalizer class."""

    def test_normalize_string_basic(self):
        """Test basic string normalization."""
        result = DataNormalizer.normalize_string("  hello  world  ")
        assert result == "hello world"

    def test_normalize_string_nan(self):
        """Test string normalization with NaN."""
        result = DataNormalizer.normalize_string(pd.NA)
        assert result == ""

    def test_normalize_string_title_case(self):
        """Test title case normalization."""
        result = DataNormalizer.normalize_string("john doe", title_case=True)
        assert result == "John Doe"

    def test_normalize_folio(self):
        """Test folio normalization."""
        result = DataNormalizer.normalize_folio("  IN30123456789  ")
        assert result == "IN30123456789"

    def test_normalize_numeric_integer(self):
        """Test numeric normalization with integer."""
        result = DataNormalizer.normalize_numeric(100)
        assert result == 100

    def test_normalize_numeric_string(self):
        """Test numeric normalization with string."""
        result = DataNormalizer.normalize_numeric("1,000")
        assert result == 1000

    def test_normalize_numeric_indian_format(self):
        """Test numeric normalization with Indian format."""
        result = DataNormalizer.normalize_numeric("1,00,000")
        assert result == 100000

    def test_normalize_numeric_rupee_symbol(self):
        """Test numeric normalization with rupee symbol."""
        result = DataNormalizer.normalize_numeric("₹1,00,000")
        assert result == 100000

    def test_normalize_float_decimal(self):
        """Test float normalization with decimal."""
        result = DataNormalizer.normalize_float("280.50")
        assert result == 280.5

    def test_normalize_float_indian_format(self):
        """Test float normalization with Indian format."""
        result = DataNormalizer.normalize_float("2,80,000.50")
        assert result == 280000.5

    def test_normalize_date_iso_format(self):
        """Test date normalization with ISO format."""
        result = DataNormalizer.normalize_date("2026-03-14")
        assert result == "2026-03-14"

    def test_normalize_date_ddmmyyyy_format(self):
        """Test date normalization with DD-MM-YYYY format."""
        result = DataNormalizer.normalize_date("14-03-2026")
        assert result == "2026-03-14"


class TestMerger:
    """Tests for the Merger class."""

    def test_merger_initialization(self, tmp_path):
        """Test merger initialization."""
        input_folder = tmp_path / "input"
        output_folder = tmp_path / "output"
        input_folder.mkdir()

        merger = Merger(input_folder, output_folder)
        assert merger.input_folder == input_folder
        assert output_folder.exists()

    def test_load_no_files(self, tmp_path):
        """Test loading with no files."""
        merger = Merger(tmp_path, tmp_path)
        files = merger.load_parsed_files()
        assert files == []

    def test_normalize_columns_basic(self, tmp_path):
        """Test basic column normalization."""
        merger = Merger(tmp_path, tmp_path)
        df = pd.DataFrame(
            {
                "Folio No": ["ABC123"],
                "Shareholder Name": ["John Doe"],
                "No. of Shares": [100],
            }
        )
        normalized = merger.normalize_columns(df)
        assert "folio_no" in normalized.columns
        assert "name" in normalized.columns
        assert "current_holding" in normalized.columns

    def test_normalize_data_types(self, tmp_path):
        """Test data type normalization."""
        merger = Merger(tmp_path, tmp_path)
        df = pd.DataFrame(
            {
                "folio_no": ["  ABC123  "],
                "current_holding": ["1,000"],
                "dividend_fy_2017_18": ["280.50"],
            }
        )
        normalized = merger.normalize_data_types(df)
        assert normalized["folio_no"].iloc[0] == "ABC123"
        assert normalized["current_holding"].iloc[0] == 1000
        assert normalized["dividend_fy_2017_18"].iloc[0] == 280.5

    def test_merge_empty(self, tmp_path):
        """Test merge with no files."""
        merger = Merger(tmp_path, tmp_path)
        result = merger.merge_all()
        assert len(result) == 0

    def test_merge_single_file(self, tmp_path):
        """Test merge with single file."""
        input_folder = tmp_path / "input"
        input_folder.mkdir()

        df = pd.DataFrame(
            {
                "Folio No": ["ABC123"],
                "Shareholder Name": ["John Doe"],
                "No. of Shares": [100],
            }
        )
        df.to_excel(input_folder / "test.xlsx", index=False)

        merger = Merger(input_folder, tmp_path)
        result = merger.merge_all()

        assert len(result) > 0
        assert "folio_no" in result.columns
        assert result["sr_no"].iloc[0] == 1


class TestMergerIntegration:
    """Integration tests for the merger module."""

    def test_merge_multiple_files(self, tmp_path):
        """Test merging multiple files."""
        input_folder = tmp_path / "input"
        input_folder.mkdir()

        # Create first file
        df1 = pd.DataFrame(
            {
                "Folio No": ["ABC123"],
                "Shareholder Name": ["John Doe"],
                "No. of Shares": [100],
            }
        )
        df1.to_excel(input_folder / "file1.xlsx", index=False)

        # Create second file
        df2 = pd.DataFrame(
            {
                "Folio Number": ["XYZ789"],
                "Investor Name": ["Jane Smith"],
                "Shares Held": [200],
            }
        )
        df2.to_excel(input_folder / "file2.xlsx", index=False)

        merger = Merger(input_folder, tmp_path)
        result = merger.merge_all()

        assert len(result) == 2
        assert result["sr_no"].tolist() == [1, 2]
        assert "source_file" in result.columns
        assert set(result["source_file"]) == {"file1.xlsx", "file2.xlsx"}

    def test_merge_with_dividends(self, tmp_path):
        """Test merging with dividend columns."""
        input_folder = tmp_path / "input"
        input_folder.mkdir()

        df = pd.DataFrame(
            {
                "Folio No": ["ABC123"],
                "Shareholder Name": ["John Doe"],
                "No. of Shares": [100],
                "Dividend FY 2017-18": [280.5],
                "Dividend FY 2018-19": [300.0],
            }
        )
        df.to_excel(input_folder / "test.xlsx", index=False)

        merger = Merger(input_folder, tmp_path)
        result = merger.merge_all()

        assert "dividend_fy_2017_18" in result.columns
        assert "dividend_fy_2018_19" in result.columns

    def test_save_outputs(self, tmp_path):
        """Test saving outputs to multiple formats."""
        input_folder = tmp_path / "input"
        output_folder = tmp_path / "output"
        input_folder.mkdir()
        output_folder.mkdir()

        df = pd.DataFrame(
            {
                "Folio No": ["ABC123"],
                "Shareholder Name": ["John Doe"],
                "No. of Shares": [100],
            }
        )
        df.to_excel(input_folder / "test.xlsx", index=False)

        merger = Merger(input_folder, output_folder)
        merged = merger.merge_all()
        merger.save_outputs(merged)

        assert (output_folder / "master_merged.xlsx").exists()
        assert (output_folder / "master_merged.csv").exists()
        # SQLite file might not be created if sqlalchemy is not installed

    def test_update_progress(self, tmp_path):
        """Test progress update."""
        input_folder = tmp_path / "input"
        output_folder = tmp_path / "output"
        input_folder.mkdir()
        output_folder.mkdir()

        df = pd.DataFrame(
            {
                "Folio No": ["ABC123"],
                "Shareholder Name": ["John Doe"],
                "No. of Shares": [100],
            }
        )
        df.to_excel(input_folder / "test.xlsx", index=False)

        merger = Merger(input_folder, output_folder)
        merged = merger.merge_all()
        merger.update_progress()

        progress_file = output_folder.parent / "progress_status.json"
        if progress_file.exists():
            with open(progress_file) as f:
                progress = json.load(f)
            assert "merger" in progress


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
