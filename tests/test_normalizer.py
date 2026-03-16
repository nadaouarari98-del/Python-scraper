"""
tests/test_normalizer.py
-------------------------
Tests for src/parser/normalizer.py
"""

import pytest
import pandas as pd

from src.parser.normalizer import (
    clean_name,
    clean_address,
    parse_amount,
    map_column_name,
    detect_fy_columns,
    extract_year_from_filename,
    extract_company_from_filename,
    normalize_dataframe,
)


class TestCleanName:
    def test_title_case(self):
        assert clean_name("RAJESH KUMAR sharma") == "Rajesh Kumar Sharma"

    def test_collapses_whitespace(self):
        assert clean_name("  PRIYA   NAIR  ") == "Priya Nair"

    def test_none_returns_empty(self):
        assert clean_name(None) == ""

    def test_float_nan_returns_empty(self):
        import math
        assert clean_name(float("nan")) == ""

    def test_number_string(self):
        result = clean_name("12345")
        assert result == "12345"


class TestCleanAddress:
    def test_joins_parts(self):
        result = clean_address(["123 Main St", "Mumbai", "400001"])
        assert "123 Main St" in result
        assert "Mumbai" in result

    def test_skips_none(self):
        result = clean_address([None, "Mumbai", None])
        assert result == "Mumbai"

    def test_collapses_whitespace(self):
        result = clean_address(["  A    B  "])
        assert "  " not in result

    def test_empty_list(self):
        assert clean_address([]) == ""


class TestParseAmount:
    def test_indian_format_with_commas(self):
        assert parse_amount("1,23,456.78") == pytest.approx(123456.78)

    def test_rupee_symbol(self):
        assert parse_amount("₹750.00") == pytest.approx(750.0)

    def test_plain_integer(self):
        assert parse_amount("250") == pytest.approx(250.0)

    def test_empty_string(self):
        assert parse_amount("") == 0.0

    def test_none(self):
        assert parse_amount(None) == 0.0

    def test_nan(self):
        assert parse_amount(float("nan")) == 0.0

    def test_zero_string(self):
        assert parse_amount("0.00") == 0.0

    def test_with_spaces(self):
        assert parse_amount("  1,875.00  ") == pytest.approx(1875.0)


class TestMapColumnName:
    def test_folio_variants(self):
        assert map_column_name("Folio No") == "folio_no"
        assert map_column_name("FOLIO NUMBER") == "folio_no"
        assert map_column_name("Register Folio") == "folio_no"

    def test_name_variants(self):
        assert map_column_name("Name of Shareholder") == "name"
        assert map_column_name("Investor Name") == "name"

    def test_address(self):
        assert map_column_name("Registered Address") == "address"

    def test_demat(self):
        assert map_column_name("DP ID-Client ID") == "demat_account"

    def test_pan(self):
        assert map_column_name("PAN No") == "pan_number"

    def test_current_holding(self):
        assert map_column_name("Current Holding") == "current_holding"
        assert map_column_name("No. of Shares") == "current_holding"

    def test_unrecognised_returns_none(self):
        assert map_column_name("Completely Unknown Column xyz") is None


class TestDetectFyColumns:
    def test_detects_fy_column(self):
        cols = ["Folio No", "Name", "Dividend Amount FY 2017-18", "Current Holding"]
        result = detect_fy_columns(cols)
        assert len(result) == 1
        assert "Dividend Amount FY 2017-18" in result
        assert result["Dividend Amount FY 2017-18"] == "dividend_fy_2017_18"

    def test_multiple_fy_columns(self):
        cols = ["Amount 2015-16", "Amount 2016-17", "Amount 2017-18"]
        result = detect_fy_columns(cols)
        assert len(result) == 3

    def test_no_fy_columns(self):
        assert detect_fy_columns(["Folio No", "Name", "Address"]) == {}


class TestExtractYearFromFilename:
    def test_standard_fy_format(self):
        assert extract_year_from_filename("TechMahindra_IEPF_2017-2018.pdf") == "2017-18"

    def test_short_fy_format(self):
        assert extract_year_from_filename("iepf-data-fy-2017-18.pdf") == "2017-18"

    def test_no_year_returns_unknown(self):
        assert extract_year_from_filename("some_document.pdf") == "unknown"


class TestExtractCompanyFromFilename:
    def test_tech_mahindra(self):
        slug = extract_company_from_filename("TechMahindra_IEPF_2017-2018.pdf")
        assert "tech" in slug or "mahindra" in slug

    def test_slug_is_lowercase(self):
        slug = extract_company_from_filename("RelIANCE_dividend_2019.pdf")
        assert slug == slug.lower()


class TestNormalizeDataframe:
    def _raw_df(self):
        return pd.DataFrame({
            "Folio No": ["TM00012345", "TM00023456"],
            "Name of Shareholder": ["RAJESH KUMAR", "PRIYA NAIR"],
            "Registered Address": ["123 Main St Mumbai", "456 Park Ave Delhi"],
            "DP ID-Client ID": ["IN300123456789", "IN300987654321"],
            "Current Holding": ["250", "100"],
            "Dividend Amount FY 2017-18": ["1,875.00", "750.00"],
        })

    def test_returns_dataframe(self):
        df = normalize_dataframe(self._raw_df(), "tech-mahindra", "test.pdf", "2017-18")
        assert isinstance(df, pd.DataFrame)

    def test_required_columns_present(self):
        df = normalize_dataframe(self._raw_df(), "tech-mahindra", "test.pdf", "2017-18")
        for col in ["folio_no", "name", "address", "current_holding", "total_dividend",
                    "company_name", "source_file", "year", "parsed_at"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_name_is_title_case(self):
        df = normalize_dataframe(self._raw_df(), "tech-mahindra", "test.pdf", "2017-18")
        assert df["name"].iloc[0] == "Rajesh Kumar"

    def test_total_dividend_calculated(self):
        df = normalize_dataframe(self._raw_df(), "tech-mahindra", "test.pdf", "2017-18")
        assert df["total_dividend"].iloc[0] == pytest.approx(1875.0)

    def test_metadata_columns_set(self):
        df = normalize_dataframe(self._raw_df(), "tech-mahindra", "test.pdf", "2017-18")
        assert (df["company_name"] == "tech-mahindra").all()
        assert (df["source_file"] == "test.pdf").all()
        assert (df["year"] == "2017-18").all()

    def test_empty_input_returns_empty(self):
        result = normalize_dataframe(pd.DataFrame(), "co", "f.pdf", "2020-21")
        assert isinstance(result, pd.DataFrame)

    def test_header_echo_rows_removed(self):
        """Rows where folio_no == 'Folio No' (header repeats) are dropped."""
        df = self._raw_df().copy()
        header_echo = pd.DataFrame({
            "Folio No": ["Folio No"],
            "Name of Shareholder": ["Name of Shareholder"],
            "Registered Address": ["Address"],
            "DP ID-Client ID": ["DP ID"],
            "Current Holding": ["Current Holding"],
            "Dividend Amount FY 2017-18": ["Amount"],
        })
        combined = pd.concat([df, header_echo], ignore_index=True)
        result = normalize_dataframe(combined, "co", "f.pdf", "2017-18")
        folio_values = result["folio_no"].str.lower().tolist()
        assert "folio no" not in folio_values
