"""
src/processor/merger.py
-----------------------
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
            mappings_file = Path(__file__).parent / "column_mappings.yaml"

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

        # Special handling for dividend columns (regex pattern)
        if re.search(r"(?:dividend|div|amount|unpaid|unclaimed)", lower_col, re.IGNORECASE):
            # Check for FY pattern
            fy_match = re.search(
                r"(\d{4})[-–](\d{2,4})", raw_column, re.IGNORECASE
            )
            if fy_match:
                fy_start = fy_match.group(1)
                fy_end = fy_match.group(2)
                # Normalize 2-digit year to 4-digit
                if len(fy_end) == 2:
                    fy_end = "20" + fy_end if int(fy_end) < 50 else "19" + fy_end
                return f"dividend_fy_{fy_start}_{fy_end}"

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
        Normalize column names in a DataFrame.

        Args:
            df: Input DataFrame
            file_path: Optional file path for logging

        Returns:
            DataFrame with normalized column names
        """
        new_columns = {}

        for raw_col in df.columns:
            canonical_col = self.column_mapper.map_column(raw_col, threshold=80)

            if canonical_col is None:
                # Could not map this column
                new_col_name = f"unmapped_{raw_col.lower().replace(' ', '_')}"
                new_columns[raw_col] = new_col_name
                self.stats["column_mapping_issues"].append(
                    {
                        "file": str(file_path),
                        "raw_column": raw_col,
                        "action": "stored as unmapped",
                    }
                )
            else:
                new_columns[raw_col] = canonical_col

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
            # Add metadata columns
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

        # Fill missing values
        for col in merged.columns:
            if col.startswith("dividend_"):
                merged[col] = merged[col].fillna(0.0)
            elif col in ["sr_no", "current_holding"]:
                merged[col] = merged[col].fillna(0).astype(int)
            else:
                merged[col] = merged[col].fillna("")

        self.stats["total_records"] = len(merged)
        return merged

    def save_outputs(self, df: pd.DataFrame) -> None:
        """
        Save merged DataFrame to Excel, CSV, and SQLite.

        Args:
            df: Merged DataFrame
        """
        # Excel
        excel_path = self.output_folder / "master_merged.xlsx"
        try:
            df.to_excel(excel_path, index=False, engine="openpyxl")
            logger.info(f"Saved Excel: {excel_path}")
        except Exception as e:
            logger.error(f"Failed to save Excel: {e}")
            self.stats["data_errors"].append(
                {"operation": "save_excel", "error": str(e)}
            )

        # CSV
        csv_path = self.output_folder / "master_merged.csv"
        try:
            df.to_csv(csv_path, index=False)
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
            # Convert to absolute path and ensure proper forward slashes for SQLite URL
            db_path_str = str(db_path.absolute()).replace("\\", "/")
            engine = create_engine(f"sqlite:///{db_path_str}")
            df.to_sql("shareholders", engine, if_exists="replace", index=False)
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
