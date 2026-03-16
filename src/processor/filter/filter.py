"""
Filter module for the shareholder-pipeline project.

Apply value-based filters to isolate high-value shareholder records.
Supports multiple filter presets and configurable thresholds.
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional, Dict, Tuple, List
from datetime import datetime
from pathlib import Path
import sqlite3
from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean, DateTime
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import select

from src.parser.excel_writer import safe_excel_write

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()


class FilteredRecord(Base):
    """SQLAlchemy ORM model for filtered records."""
    __tablename__ = 'shareholders_filtered'
    
    folio_no = Column(String, primary_key=True)
    name = Column(String)
    address = Column(String)
    demat_account = Column(String)
    current_holding = Column(Integer)
    total_dividend = Column(Float)
    investor_type = Column(String)
    is_high_value = Column(Boolean, default=False)
    filter_preset_used = Column(String)
    filter_matched_criteria = Column(String)  # Comma-separated criteria met
    filtered_at = Column(DateTime, default=datetime.utcnow)


class Filter:
    """
    Value-based filter for shareholder records.
    
    Supports:
    - Filtering by minimum current holding (shares)
    - Filtering by minimum total dividend amount
    - Filtering by minimum single-year dividend
    - Filtering by investor type
    - AND/OR logic for combining criteria
    - Preset configurations for quick switching
    """
    
    def __init__(self, presets: Dict = None, verbose: bool = True):
        """
        Initialize Filter with optional presets.
        
        Args:
            presets: Dictionary of filter presets {name: {criteria}}
            verbose: Enable detailed logging
        """
        self.presets = presets or {}
        self.verbose = verbose
        self.statistics = {
            'total_input': 0,
            'total_filtered': 0,
            'high_value_count': 0,
            'filter_preset_used': None,
            'criteria_matched': {}
        }
        
    def apply_filter(
        self,
        df: pd.DataFrame,
        min_current_holding: int = 0,
        min_total_dividend: float = 0,
        min_single_year_dividend: float = 0,
        investor_type: Optional[str] = None,
        logic: str = "or"
    ) -> pd.DataFrame:
        """
        Apply value-based filter to shareholder records.
        
        Args:
            df: Input DataFrame with shareholder records
            min_current_holding: Minimum shares to hold
            min_total_dividend: Minimum total dividend across all years
            min_single_year_dividend: Minimum dividend in any single year
            investor_type: Filter by investor type (e.g., "Promoter", "HNI")
            logic: "or" (any criterion) or "and" (all criteria)
        
        Returns:
            Filtered DataFrame with is_high_value and filter_preset_used columns
        """
        self.statistics['total_input'] = len(df)
        
        if self.verbose:
            logger.info(f"Applying filter with logic='{logic}'")
            logger.info(f"  - min_current_holding: {min_current_holding}")
            logger.info(f"  - min_total_dividend: {min_total_dividend}")
            logger.info(f"  - min_single_year_dividend: {min_single_year_dividend}")
            logger.info(f"  - investor_type: {investor_type}")
        
        # Initialize filter columns
        df_filtered = df.copy()
        df_filtered['is_high_value'] = False
        df_filtered['filter_preset_used'] = None
        df_filtered['filter_matched_criteria'] = ''
        
        # Build list of criteria to check
        criteria_checks = []
        
        # Criterion 1: Current holding
        if min_current_holding > 0:
            holding_col = self._find_column(df_filtered, ['current_holding', 'holding', 'shares_held', 'no_of_shares'])
            if holding_col:
                mask_holding = df_filtered[holding_col].fillna(0).astype(float) >= min_current_holding
                criteria_checks.append(('current_holding_' + str(min_current_holding), mask_holding))
        
        # Criterion 2: Total dividend
        if min_total_dividend > 0:
            dividend_col = self._find_column(df_filtered, ['total_dividend', 'total_amount', 'cumulative_dividend', 'dividend_amount'])
            if dividend_col:
                mask_dividend = df_filtered[dividend_col].fillna(0).astype(float) >= min_total_dividend
                criteria_checks.append(('total_dividend_' + str(min_total_dividend), mask_dividend))
        
        # Criterion 3: Single year dividend
        if min_single_year_dividend > 0:
            # Look for year columns (e.g., dividend_2023, dividend_2022, etc.)
            year_cols = [col for col in df_filtered.columns if 'dividend' in col.lower() or 'amount' in col.lower()]
            year_cols = [col for col in year_cols if any(str(year) in col for year in range(2000, 2030))]
            
            if year_cols:
                # Max dividend in any year >= threshold
                max_year_dividend = df_filtered[year_cols].fillna(0).astype(float).max(axis=1)
                mask_year = max_year_dividend >= min_single_year_dividend
                criteria_checks.append(('single_year_dividend_' + str(min_single_year_dividend), mask_year))
        
        # Criterion 4: Investor type
        if investor_type:
            investor_col = self._find_column(df_filtered, ['investor_type', 'type', 'category'])
            if investor_col:
                mask_investor = df_filtered[investor_col].fillna('').str.upper() == investor_type.upper()
                criteria_checks.append(('investor_type_' + investor_type, mask_investor))
        
        # Combine criteria based on logic
        if criteria_checks:
            if logic.lower() == "and":
                # All criteria must be true
                combined_mask = pd.Series([True] * len(df_filtered), index=df_filtered.index)
                for criteria_name, mask in criteria_checks:
                    combined_mask = combined_mask & mask
            else:  # "or" logic (default)
                # At least one criterion must be true
                combined_mask = pd.Series([False] * len(df_filtered), index=df_filtered.index)
                for criteria_name, mask in criteria_checks:
                    combined_mask = combined_mask | mask
            
            # Apply filter
            df_filtered.loc[combined_mask, 'is_high_value'] = True
            
            # Track matched criteria for each record
            for idx in df_filtered[combined_mask].index:
                matched = []
                for criteria_name, mask in criteria_checks:
                    if mask[idx]:
                        matched.append(criteria_name)
                df_filtered.loc[idx, 'filter_matched_criteria'] = ','.join(matched)
            
            self.statistics['total_filtered'] = combined_mask.sum()
            self.statistics['high_value_count'] = combined_mask.sum()
        else:
            if self.verbose:
                logger.warning("No criteria provided - no records filtered")
            self.statistics['total_filtered'] = 0
            self.statistics['high_value_count'] = 0
        
        # Update criteria statistics
        for criteria_name, mask in criteria_checks:
            self.statistics['criteria_matched'][criteria_name] = mask.sum()
        
        if self.verbose:
            logger.info(f"Filter results:")
            logger.info(f"  - Total input: {self.statistics['total_input']}")
            logger.info(f"  - High-value records: {self.statistics['high_value_count']}")
            logger.info(f"  - Retention rate: {100 * self.statistics['high_value_count'] / max(1, self.statistics['total_input']):.1f}%")
        
        return df_filtered
    
    def apply_preset(
        self,
        df: pd.DataFrame,
        preset_name: str
    ) -> pd.DataFrame:
        """
        Apply a predefined filter preset.
        
        Args:
            df: Input DataFrame
            preset_name: Name of preset from config
        
        Returns:
            Filtered DataFrame with is_high_value column
        
        Raises:
            ValueError: If preset not found
        """
        if preset_name not in self.presets:
            raise ValueError(f"Preset '{preset_name}' not found. Available: {list(self.presets.keys())}")
        
        preset = self.presets[preset_name]
        if self.verbose:
            logger.info(f"Applying preset: {preset_name}")
        
        df_filtered = self.apply_filter(
            df,
            min_current_holding=preset.get('min_current_holding', 0),
            min_total_dividend=preset.get('min_total_dividend', 0),
            min_single_year_dividend=preset.get('min_single_year_dividend', 0),
            investor_type=preset.get('investor_type'),
            logic=preset.get('logic', 'or')
        )
        
        df_filtered['filter_preset_used'] = preset_name
        self.statistics['filter_preset_used'] = preset_name
        
        return df_filtered
    
    def save_filtered_records(
        self,
        df: pd.DataFrame,
        output_dir: str = "data/output/"
    ) -> Dict[str, str]:
        """
        Save filtered records to Excel and CSV.
        
        Args:
            df: Filtered DataFrame
            output_dir: Directory to save output files
        
        Returns:
            Dictionary with output file paths
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Filter to only high-value records
        df_high_value = df[df['is_high_value'] == True].copy()
        
        # Save Excel
        excel_file = output_path / "master_filtered.xlsx"
        safe_excel_write(df_high_value, str(excel_file), index=False)
        
        # Apply Excel formatting (column widths, headers)
        try:
            from src.parser.excel_writer import format_excel_output
            format_excel_output(str(excel_file), df_high_value)
        except Exception as e:
            logger.warning(f"Could not apply Excel formatting: {e}")
        
        if self.verbose:
            logger.info(f"Saved high-value records to {excel_file}")
        
        # Save CSV
        csv_file = output_path / "master_filtered.csv"
        df_high_value.to_csv(csv_file, index=False)
        if self.verbose:
            logger.info(f"Saved high-value records to {csv_file}")
        
        # REQUIREMENT 5: Save to SQLite database
        try:
            from src.processor.database import get_database
            db = get_database(Path(output_dir).parent.parent)
            stats = db.insert_or_update_shareholders(df_high_value)
            logger.info(f"Database update: {stats['inserted']} inserted, {stats['updated']} updated")
        except Exception as e:
            logger.error(f"Failed to update database: {e}")
        
        return {
            'excel': str(excel_file),
            'csv': str(csv_file)
        }
    
    def update_database(
        self,
        df: pd.DataFrame,
        db_path: str = "data/pipeline.db"
    ) -> None:
        """
        Update SQLite database with filtered records.
        
        Args:
            df: DataFrame with is_high_value column
            db_path: Path to SQLite database
        """
        engine = create_engine(f'sqlite:///{db_path}')
        
        try:
            # Create tables
            Base.metadata.create_all(engine)
            
            with Session(engine) as session:
                # Clear existing filtered records
                session.query(FilteredRecord).delete()
                session.commit()
                
                # Insert filtered records
                records_to_add = []
                for _, row in df.iterrows():
                    if row['is_high_value']:
                        # Skip duplicates by checking if folio_no already exists
                        existing = session.query(FilteredRecord).filter(
                            FilteredRecord.folio_no == str(row.get('folio_no', ''))
                        ).first()
                        
                        if existing:
                            # Update existing record
                            existing.name = str(row.get('name', ''))
                            existing.address = str(row.get('address', ''))
                            existing.demat_account = str(row.get('demat_account', ''))
                            existing.current_holding = int(row.get('current_holding', 0)) if pd.notna(row.get('current_holding')) else 0
                            existing.total_dividend = float(row.get('total_dividend', 0)) if pd.notna(row.get('total_dividend')) else 0
                            existing.investor_type = str(row.get('investor_type', ''))
                            existing.is_high_value = True
                            existing.filter_preset_used = str(row.get('filter_preset_used', ''))
                            existing.filter_matched_criteria = str(row.get('filter_matched_criteria', ''))
                        else:
                            # Create new record
                            filtered_record = FilteredRecord(
                                folio_no=str(row.get('folio_no', '')),
                                name=str(row.get('name', '')),
                                address=str(row.get('address', '')),
                                demat_account=str(row.get('demat_account', '')),
                                current_holding=int(row.get('current_holding', 0)) if pd.notna(row.get('current_holding')) else 0,
                                total_dividend=float(row.get('total_dividend', 0)) if pd.notna(row.get('total_dividend')) else 0,
                                investor_type=str(row.get('investor_type', '')),
                                is_high_value=True,
                                filter_preset_used=str(row.get('filter_preset_used', '')),
                                filter_matched_criteria=str(row.get('filter_matched_criteria', ''))
                            )
                            records_to_add.append(filtered_record)
                
                session.add_all(records_to_add)
                session.commit()
                
                if self.verbose:
                    logger.info(f"Updated SQLite database: {len(records_to_add)} new filtered records added")
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            # Don't raise - filtering should not fail if database update fails
    
    def update_progress(
        self,
        progress_file: str = "data/progress_status.json"
    ) -> None:
        """
        Update progress JSON with filter statistics.
        
        Args:
            progress_file: Path to progress JSON file
        """
        import json
        
        progress_path = Path(progress_file)
        progress_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # Load existing progress
            if progress_path.exists():
                with open(progress_path, 'r') as f:
                    progress = json.load(f)
            else:
                progress = {}
            
            # Convert numpy types to Python native types
            total_input = int(self.statistics['total_input'])
            high_value = int(self.statistics['high_value_count'])
            
            # Update with filter statistics
            progress['filter'] = {
                'total_input': total_input,
                'filtered_high_value': high_value,
                'filter_preset': self.statistics['filter_preset_used'],
                'hit_rate': f"{100 * high_value / max(1, total_input):.1f}%",
                'timestamp': datetime.now().isoformat()
            }
            
            with open(progress_path, 'w') as f:
                json.dump(progress, f, indent=2)
            
            if self.verbose:
                logger.info(f"Updated progress file: {progress_file}")
        except Exception as e:
            logger.error(f"Error updating progress file: {e}")
    
    def get_statistics(self) -> Dict:
        """Return filter statistics."""
        return self.statistics.copy()
    
    def _find_column(self, df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
        """
        Find a column by possible names (case-insensitive).
        
        Args:
            df: DataFrame to search
            possible_names: List of possible column names
        
        Returns:
            Column name if found, None otherwise
        """
        df_cols_lower = {col.lower(): col for col in df.columns}
        for name in possible_names:
            if name.lower() in df_cols_lower:
                return df_cols_lower[name.lower()]
        return None


def apply_filter(
    df: pd.DataFrame,
    min_current_holding: int = 0,
    min_total_dividend: float = 0,
    min_single_year_dividend: float = 0,
    investor_type: Optional[str] = None,
    logic: str = "or",
    verbose: bool = True
) -> pd.DataFrame:
    """
    Public API: Apply value-based filter to shareholder records.
    
    Args:
        df: Input DataFrame with shareholder records
        min_current_holding: Minimum shares to hold
        min_total_dividend: Minimum total dividend
        min_single_year_dividend: Minimum single-year dividend
        investor_type: Filter by investor type
        logic: "or" (any criterion) or "and" (all criteria)
        verbose: Enable detailed logging
    
    Returns:
        Filtered DataFrame with is_high_value and filter_preset_used columns
    """
    filter_obj = Filter(verbose=verbose)
    return filter_obj.apply_filter(
        df,
        min_current_holding=min_current_holding,
        min_total_dividend=min_total_dividend,
        min_single_year_dividend=min_single_year_dividend,
        investor_type=investor_type,
        logic=logic
    )


def apply_preset(
    df: pd.DataFrame,
    presets: Dict,
    preset_name: str,
    verbose: bool = True
) -> pd.DataFrame:
    """
    Public API: Apply a predefined filter preset.
    
    Args:
        df: Input DataFrame
        presets: Dictionary of filter presets
        preset_name: Name of preset to apply
        verbose: Enable detailed logging
    
    Returns:
        Filtered DataFrame with is_high_value column
    
    Raises:
        ValueError: If preset not found
    """
    filter_obj = Filter(presets=presets, verbose=verbose)
    return filter_obj.apply_preset(df, preset_name)
