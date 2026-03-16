"""
src/processor/merger/merger.py
------------------------------
Merge all individually parsed Excel files into a unified master dataset with
consistent column alignment, data type normalization, and metadata enrichment.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import yaml
from rapidfuzz import fuzz

from src.parser.excel_writer import safe_excel_write

logger = logging.getLogger(__name__)

# Indian number format regex: 1,00,000 or 1,00,00,000
INDIAN_NUMBER_PATTERN = re.compile(r"^\d{1,3}(?:,\d{2})*(?:\.\d+)?$")
# Rupee symbol
RUPEE_SYMBOL = "₹"


class ColumnMapper:
    """Maps variant column names to canonical schema using fuzzy matching."""

    def __init__(self, mappings_file: Optional[Path] = None):
        """
        Initialize the column mapper.

        Args:
            mappings_file: Path to YAML file with column aliases.
                          If None, uses default location.
        """
        if mappings_file is None:
            mappings_file = Path(__file__).parent.parent / "column_mappings.yaml"

        self.mappings_file = mappings_file
        self.canonical_mappings = self._load_mappings()
        self.reverse_mapping = self._build_reverse_mapping()
        self.unmapped_columns: Set[str] = set()

    def _load_mappings(self) -> Dict[str, List[str]]:
        """Load canonical to aliases mapping from YAML."""
        if not self.mappings_file.exists():
            logger.warning(f"Mappings file not found: {self.mappings_file}")
            return {}

        with open(self.mappings_file, "r") as f:
            raw_mappings = yaml.safe_load(f) or {}

        # Filter out special entries (like dividend_amounts with pattern)
        mappings = {}
        for key, value in raw_mappings.items():
            if isinstance(value, list) and value and not isinstance(value[0], dict):
                mappings[key] = value

        return mappings

    def _build_reverse_mapping(self) -> Dict[str, str]:
        """Build a reverse mapping: alias → canonical name."""
        reverse = {}
        for canonical, aliases in self.canonical_mappings.items():
            for alias in aliases:
                reverse[alias.lower()] = canonical
        return reverse

    def map_column(self, raw_column: str, threshold: float = 80) -> Optional[str]:
        """
        Map a raw column name to canonical name using fuzzy matching.

        Args:
            raw_column: The column name from the source file
            threshold: Minimum match score (0-100) for fuzzy matching

        Returns:
            Canonical column name, or None if no good match found
        """
        # Exact match in reverse mapping (case-insensitive)
        lower_col = raw_column.lower()
        if lower_col in self.reverse_mapping:
            return self.reverse_mapping[lower_col]

        # Check if column is already in canonical form (e.g., final_dividend_fy_2017_18)
        if re.match(r"^(final_|interim_)?dividend_fy_\d{4}_\d{2}$", lower_col):
            # Already canonical dividend column
            return lower_col if lower_col.startswith("dividend") else f"dividend_fy_{re.search(r'(\d{4}_\d{2})', lower_col).group(1)}"
        
        # Check if column is already canonical (e.g., folio_no, name, address)
        canonical_names = {"folio_no", "name", "address", "demat_account", "pan_number", 
                          "current_holding", "state", "pincode", "country", "company_name", 
                          "source_file", "year", "sr_no", "total_dividend", "date_processed"}
        if lower_col in canonical_names or lower_col in self.canonical_mappings:
            return lower_col

        # Special handling for dividend columns (regex pattern) - CHECK FIRST
        # This must come before fuzzy matching to avoid "Dividend FY 2017-18" matching to "year"
        if re.search(r"(?:dividend|div|amount|unpaid|unclaimed)", lower_col, re.IGNORECASE):
            # Check for FY pattern
            fy_match = re.search(
                r"(\d{4})[-–](\d{2,4})", raw_column, re.IGNORECASE
            )
            if fy_match:
                fy_start = fy_match.group(1)
                fy_end = fy_match.group(2)
                # Keep as-is: 2017-18 becomes 2017_18, not 2017_2018
                # Only convert if explicitly 4 digits already (which we won't in this case)
                return f"dividend_fy_{fy_start}_{fy_end}"

        # Fuzzy match against all aliases
        best_match = None
        best_score = 0

        for canonical, aliases in self.canonical_mappings.items():
            for alias in aliases:
                score = fuzz.token_set_ratio(lower_col, alias.lower())
                if score > best_score:
                    best_score = score
                    best_match = canonical

        if best_score >= threshold:
            return best_match

        # No match found
        self.unmapped_columns.add(raw_column)
        return None


class DataNormalizer:
    """Normalize data types and values to consistent format."""

    @staticmethod
    def normalize_string(value: Any, title_case: bool = False) -> str:
        """
        Normalize string value: handle NaN, strip, collapse spaces, deduplicate.

        Args:
            value: Raw value from DataFrame
            title_case: Whether to convert to title case (for names)

        Returns:
            Normalized string
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        
        # Handle pd.NA specifically
        if pd.isna(value):
            return ""

        text = str(value).strip()
        # Collapse multiple spaces
        text = re.sub(r"\s+", " ", text)

        if title_case:
            # Remove special characters except . and -
            text = re.sub(r"[^\w\s.'-]", "", text, flags=re.UNICODE)
            text = text.title()

        return text

    @staticmethod
    def normalize_folio(value: Any) -> str:
        """Normalize folio number: uppercase, remove spaces."""
        text = DataNormalizer.normalize_string(value)
        return text.upper().replace(" ", "")

    @staticmethod
    def normalize_numeric(value: Any) -> int:
        """
        Normalize numeric value: handle commas, Indian format, etc.

        Args:
            value: Raw value (string or number)

        Returns:
            Integer value, or 0 if unable to parse
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 0

        if isinstance(value, (int, float)):
            return int(float(value))

        text = str(value).strip()

        # Remove rupee symbol and spaces
        text = text.replace(RUPEE_SYMBOL, "").replace(" ", "")

        # Handle Indian format: 1,00,000
        if INDIAN_NUMBER_PATTERN.match(text):
            text = text.replace(",", "")

        # Remove other commas
        text = text.replace(",", "")

        try:
            # Try parsing as float first (handles decimals), then convert to int
            val = float(text)
            return int(val) if val == int(val) else int(val)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse numeric value: {value}")
            return 0

    @staticmethod
    def normalize_float(value: Any) -> float:
        """
        Normalize float value: handle commas, Indian format, rupee symbol.

        Args:
            value: Raw value (string or number)

        Returns:
            Float value, or 0.0 if unable to parse
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 0.0

        if isinstance(value, (int, float)):
            return float(value)

        text = str(value).strip()

        # Remove rupee symbol and spaces
        text = text.replace(RUPEE_SYMBOL, "").replace(" ", "")

        # Handle Indian format: 1,00,000
        if INDIAN_NUMBER_PATTERN.match(text):
            text = text.replace(",", "")

        # Remove other commas
        text = text.replace(",", "")

        try:
            return float(text)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse float value: {value}")
            return 0.0

    @staticmethod
    def normalize_date(value: Any) -> str:
        """
        Normalize date value to ISO format (YYYY-MM-DD).

        Args:
            value: Raw value (various date formats)

        Returns:
            ISO format date string, or empty string if unable to parse
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""

        if isinstance(value, str):
            # Try common formats
            for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y"]:
                try:
                    dt = datetime.strptime(value.strip(), fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue

        if isinstance(value, (pd.Timestamp, datetime)):
            return value.strftime("%Y-%m-%d")

        logger.warning(f"Could not parse date value: {value}")
        return ""


class Merger:
    """Main merger class that orchestrates the merge process."""

    def __init__(self, input_folder: Path, output_folder: Path):
        """
        Initialize the merger.

        Args:
            input_folder: Path to folder containing parsed Excel files
            output_folder: Path to folder for output files
        """
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(parents=True, exist_ok=True)

        self.column_mapper = ColumnMapper()
        self.stats = {
            "files_processed": 0,
            "total_records": 0,
            "column_mapping_issues": [],
            "data_errors": [],
        }

    def load_parsed_files(self) -> List[Tuple[Path, pd.DataFrame]]:
        """
        Load all Excel files from input folder.

        Returns:
            List of (file_path, dataframe) tuples
        """
        files = list(self.input_folder.glob("*.xlsx"))
        logger.info(f"Found {len(files)} Excel files in {self.input_folder}")

        loaded = []
        for file_path in files:
            try:
                df = pd.read_excel(file_path)
                loaded.append((file_path, df))
                logger.info(f"Loaded {file_path.name}: {len(df)} records")
            except Exception as e:
                logger.error(f"Failed to load {file_path.name}: {e}")
                self.stats["data_errors"].append(
                    {"file": file_path.name, "error": str(e)}
                )

        self.stats["files_processed"] = len(loaded)
        return loaded

    def normalize_columns(
        self, df: pd.DataFrame, file_path: Optional[Path] = None
    ) -> pd.DataFrame:
        """
        Normalize column names in a DataFrame, preserving known schema columns.

        Args:
            df: Input DataFrame
            file_path: Optional file path for logging

        Returns:
            DataFrame with normalized column names (preserving schema columns)
        """
        # REQUIREMENT 1: Preserve all columns that are part of the 8-group schema
        # even if they're not in the column_mapper
        PRESERVED_SCHEMA_COLUMNS = {
            'first_name', 'middle_name', 'last_name', 'full_name', 'father_husband_name',
            'address_line1', 'address_line2', 'share_type',
            'mobile_number', 'email_id', 'contact_source', 'verification_status',
            'crm_push_status', 'crm_push_date', 'email_sent', 'email_sent_date'
        }
        
        new_columns = {}
        cols_to_drop = []

        for raw_col in df.columns:
            canonical_col = self.column_mapper.map_column(raw_col, threshold=80)

            if canonical_col is None:
                # Check if this is a preserved schema column
                if raw_col.lower() in PRESERVED_SCHEMA_COLUMNS:
                    # Keep it as-is (already canonical)
                    new_columns[raw_col] = raw_col
                else:
                    # Drop unmapped columns (non-schema)
                    cols_to_drop.append(raw_col)
                    self.stats["column_mapping_issues"].append(
                        {
                            "file": str(file_path),
                            "raw_column": raw_col,
                            "action": "dropped (unmapped)",
                        }
                    )
            else:
                new_columns[raw_col] = canonical_col

        # Drop only non-schema unmapped columns
        df = df.drop(columns=cols_to_drop, errors='ignore')

        # Handle duplicate canonical names by appending suffix
        seen = {}
        final_columns = {}
        for raw_col, canonical in new_columns.items():
            if canonical not in seen:
                seen[canonical] = 0
                final_columns[raw_col] = canonical
            else:
                seen[canonical] += 1
                final_columns[raw_col] = f"{canonical}_{seen[canonical]}"

        return df.rename(columns=final_columns)

    def normalize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize data types for known columns.

        Args:
            df: DataFrame with normalized column names

        Returns:
            DataFrame with normalized data types
        """
        normalizer = DataNormalizer()

        # Define normalization rules by column name
        string_cols = [
            "folio_no",
            "name",
            "address",
            "demat_account",
            "pan_number",
            "state",
            "country",
            "company_name",
            "source_file",
        ]
        numeric_int_cols = ["current_holding", "sr_no"]
        numeric_float_cols = [col for col in df.columns if col.startswith("dividend_fy_")]
        date_cols = ["parsed_at", "year"]

        # Apply normalizations
        for col in df.columns:
            if col in string_cols and col in df.columns:
                is_name = col == "name"
                df[col] = df[col].apply(
                    lambda x: normalizer.normalize_string(x, title_case=is_name)
                )
            elif col == "folio_no" and col in df.columns:
                df[col] = df[col].apply(normalizer.normalize_folio)
            elif col in numeric_int_cols and col in df.columns:
                df[col] = df[col].apply(normalizer.normalize_numeric)
            elif col in numeric_float_cols:
                df[col] = df[col].apply(normalizer.normalize_float)
            elif col in date_cols and col in df.columns:
                df[col] = df[col].apply(normalizer.normalize_date)

        return df

    def merge_all(self) -> pd.DataFrame:
        """
        Load, normalize, and merge all parsed Excel files.

        Returns:
            Merged DataFrame with all records
        """
        loaded_files = self.load_parsed_files()

        if not loaded_files:
            logger.error("No files loaded!")
            return pd.DataFrame()

        # Normalize each file
        normalized_dfs = []
        for file_path, df in loaded_files:
            # Only set source_file if it's not already set (preserve original PDF filename)
            if "source_file" not in df.columns or df["source_file"].isna().all():
                df["source_file"] = file_path.name
            df["original_row_number"] = range(1, len(df) + 1)

            # Normalize columns
            df = self.normalize_columns(df, file_path)

            # Normalize data types
            df = self.normalize_data_types(df)

            normalized_dfs.append(df)

        # Merge all DataFrames
        # Use pd.concat with sort=False to preserve column order, then reorder
        merged = pd.concat(normalized_dfs, ignore_index=True, sort=True)

        # Consolidate duplicate dividend columns (e.g., dividend_fy_2017_18 and dividend_fy_2017_18_1)
        # For each base dividend column, take the max of all variants
        dividend_base_cols = set()
        for col in merged.columns:
            if col.startswith("dividend_fy_"):
                # Strip _1, _2, etc. suffix to get base name
                base = col.split('_')[0:4]  # dividend_fy_YYYY_YY
                base = '_'.join(base)
                dividend_base_cols.add(base)

        for base_col in sorted(dividend_base_cols):
            # Find all variants of this column
            variants = [col for col in merged.columns if col.startswith(base_col)]
            if len(variants) > 1:
                # Consolidate: take max of non-null values
                merged[base_col] = merged[variants].fillna(0).max(axis=1)
                # Drop the extra variants
                merged = merged.drop(columns=[v for v in variants if v != base_col])

        # === Remove duplicate name columns (name_1, name_2, name_3, etc.) ===
        duplicate_name_cols = [col for col in merged.columns if col.startswith('name_') and col[5:].isdigit()]
        merged = merged.drop(columns=duplicate_name_cols, errors='ignore')

        # === Remove other duplicate columns (current_holding_1, etc.) ===
        # Pattern: canonical_name + underscore + number
        duplicates_to_drop = []
        for col in merged.columns:
            # Match pattern like current_holding_1, address_1, etc.
            if '_' in col:
                parts = col.rsplit('_', 1)
                if len(parts) == 2 and parts[1].isdigit():
                    base = parts[0]
                    if base in merged.columns:
                        # This is a duplicate of base column, mark for removal
                        duplicates_to_drop.append(col)
        
        merged = merged.drop(columns=duplicates_to_drop, errors='ignore')

        # Reorder columns: standard columns first, then dividend columns, then others
        standard_cols = [
            "sr_no",
            "folio_no",
            "name",
            "address",
            "state",
            "pincode",
            "country",
            "current_holding",
            "demat_account",
            "pan_number",
            "company_name",
            "source_file",
            "year",
            "parsed_at",
            "original_row_number",
        ]

        dividend_cols = sorted(
            [col for col in merged.columns if col.startswith("dividend_fy_")]
        )
        other_cols = [
            col
            for col in merged.columns
            if col not in standard_cols and col not in dividend_cols
        ]

        column_order = (
            [col for col in standard_cols if col in merged.columns]
            + dividend_cols
            + other_cols
        )
        merged = merged[column_order]

        # Add serial numbers
        merged["sr_no"] = range(1, len(merged) + 1)

        # Calculate total_dividend if we have dividend columns
        if dividend_cols:
            # Only sum actual dividend_fy_* columns (not total_dividend itself)
            dividend_value_cols = [c for c in dividend_cols if not c.endswith('_1')]
            merged['total_dividend'] = merged[dividend_value_cols].fillna(0).sum(axis=1).astype(int)

        # Fill missing values
        for col in merged.columns:
            if col.startswith("dividend_") or col == "total_dividend":
                merged[col] = merged[col].fillna(0).astype(int)
            elif col in ["sr_no", "current_holding"]:
                merged[col] = merged[col].fillna(0).astype(int)
            else:
                dtype_str = str(merged[col].dtype)
                if 'float' in dtype_str or 'int' in dtype_str:
                    merged[col] = merged[col].fillna(0)
                else:
                    merged[col] = merged[col].fillna("").astype(str).replace({"nan": "", "<NA>": ""})

        # === REQUIREMENT 2: Standardize column order per specification ===
        merged = self._standardize_column_order(merged)
        
        # === REQUIREMENT 1: Replace all NaN with empty string/0 ===
        for col in merged.columns:
            dtype_str = str(merged[col].dtype)
            if col.startswith("dividend_") or col == "total_dividend" or col in ["sr_no", "current_holding"]:
                merged[col] = merged[col].fillna(0)
            elif 'float' in dtype_str or 'int' in dtype_str:
                merged[col] = merged[col].fillna(0)
            else:
                # Handles object, StringDtype, and any other string-like dtype
                merged[col] = merged[col].fillna("").astype(str).replace({"nan": "", "<NA>": ""})

        self.stats["total_records"] = len(merged)
        return merged
    
    def _standardize_column_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """REQUIREMENT 2: Standardize column order - never crashes on missing columns."""
        COLUMN_GROUPS = [
            ["sr_no", "folio_no", "demat_account_no", "first_name", "middle_name",
             "last_name", "full_name", "father_husband_name"],
            ["address_line1", "address_line2", "city", "state", "pin_code", "country"],
            ["current_holding", "share_type"],
            ["company_name", "source_pdf", "financial_year_of_pdf", "date_processed"],
            ["mobile_number", "email_id", "contact_source", "verification_status"],
            ["crm_push_status", "crm_push_date", "email_sent_status", "email_sent_date"],
        ]
        ordered = []
        for group in COLUMN_GROUPS:
            for col in group:
                if col in df.columns and col not in ordered:
                    ordered.append(col)
        fy_cols = sorted([c for c in df.columns if c.startswith("fy_") and c not in ordered])
        ordered += fy_cols
        extras = [c for c in df.columns if c not in ordered]
        ordered += extras
        return df[ordered]

    def save_outputs(self, df: pd.DataFrame) -> None:
        """
        Save merged DataFrame to Excel, CSV, and SQLite.

        Args:
            df: Merged DataFrame
        """
        from src.parser.excel_writer import create_multisheet_excel
        
        df_master = df.copy()
        
        # REQUIREMENT 1: Clean all NaN values before writing
        # Convert all string-like columns (object or StringDtype) to plain object
        # dtype so NaN fills work correctly and Excel round-trips cleanly.
        for col in df_master.columns:
            dtype_str = str(df_master[col].dtype)
            if dtype_str == "object" or dtype_str.startswith("string") or dtype_str == "StringDtype":
                df_master[col] = df_master[col].fillna("").astype(str).replace("nan", "").replace("<NA>", "")
            elif "float" in dtype_str or "int" in dtype_str:
                df_master[col] = df_master[col].fillna(0)
            else:
                # Fallback: try converting to str to surface any hidden NaN
                try:
                    df_master[col] = df_master[col].fillna("").astype(str).replace("nan", "").replace("<NA>", "")
                except Exception:
                    df_master[col] = df_master[col].fillna("")
        
        # Excel - REQUIREMENT 3: Multi-sheet (ALL_COMPANIES + company sheets)
        excel_path = self.output_folder / "master_merged.xlsx"
        try:
            create_multisheet_excel(df_master, str(excel_path))
            logger.info(f"Saved multi-sheet Excel: {excel_path}")
        except Exception as e:
            logger.error(f"Failed to save Excel: {e}")
            self.stats["data_errors"].append(
                {"operation": "save_excel", "error": str(e)}
            )

        # CSV
        csv_path = self.output_folder / "master_merged.csv"
        try:
            df_master.to_csv(csv_path, index=False)
            logger.info(f"Saved CSV: {csv_path}")
        except Exception as e:
            logger.error(f"Failed to save CSV: {e}")
            self.stats["data_errors"].append(
                {"operation": "save_csv", "error": str(e)}
            )

        # SQLite
        try:
            from sqlalchemy import create_engine

            db_path = self.output_folder / "pipeline.db"
            engine = create_engine(f"sqlite:///{db_path}")
            df_master.to_sql("shareholders", engine, if_exists="replace", index=False)
            logger.info(f"Saved SQLite: {db_path}")
        except ImportError:
            logger.warning("SQLAlchemy not installed, skipping SQLite save")
        except Exception as e:
            logger.error(f"Failed to save SQLite: {e}")
            self.stats["data_errors"].append(
                {"operation": "save_sqlite", "error": str(e)}
            )

    def update_progress(self) -> None:
        """Update progress JSON for dashboard."""
        progress_file = self.output_folder.parent / "progress_status.json"

        try:
            if progress_file.exists():
                with open(progress_file) as f:
                    progress = json.load(f)
            else:
                progress = {}

            progress["merger"] = {
                "timestamp": datetime.now().isoformat(),
                "files_processed": self.stats["files_processed"],
                "total_records": self.stats["total_records"],
                "column_mapping_issues": len(self.stats["column_mapping_issues"]),
                "data_errors": len(self.stats["data_errors"]),
                "unmapped_columns": list(self.column_mapper.unmapped_columns),
            }

            with open(progress_file, "w") as f:
                json.dump(progress, f, indent=2)

            logger.info(f"Updated progress: {progress_file}")
        except Exception as e:
            logger.error(f"Failed to update progress: {e}")

    def log_summary(self) -> None:
        """Log a summary of the merge operation."""
        logger.info("\n" + "=" * 70)
        logger.info("MERGE SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Total records: {self.stats['total_records']}")
        logger.info(
            f"Column mapping issues: {len(self.stats['column_mapping_issues'])}"
        )
        logger.info(f"Data errors: {len(self.stats['data_errors'])}")
        logger.info(
            f"Unmapped columns: {len(self.column_mapper.unmapped_columns)}"
        )

        if self.column_mapper.unmapped_columns:
            logger.warning(
                f"Unmapped columns: {', '.join(self.column_mapper.unmapped_columns)}"
            )

        if self.stats["column_mapping_issues"]:
            logger.warning("Column mapping issues:")
            for issue in self.stats["column_mapping_issues"][:5]:
                logger.warning(f"  {issue}")

        logger.info("=" * 70)


def merge_all(
    input_folder: str, output_folder: Optional[str] = None
) -> pd.DataFrame:
    """
    Public API: Load, normalize, and merge all parsed Excel files.

    Args:
        input_folder: Path to folder containing parsed Excel files
        output_folder: Path to output folder (defaults to parent of input)

    Returns:
        Merged DataFrame
    """
    input_path = Path(input_folder)
    if output_folder is None:
        output_path = input_path.parent
    else:
        output_path = Path(output_folder)

    merger = Merger(input_path, output_path)
    merged_df = merger.merge_all()
    merger.save_outputs(merged_df)
    merger.update_progress()
    merger.log_summary()

    return merged_df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Public API: Normalize column names in a DataFrame.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with normalized column names
    """
    merger = Merger(Path("."), Path("."))
    return merger.normalize_columns(df)
