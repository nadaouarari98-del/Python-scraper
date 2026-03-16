"""
src/processor/merger/
---------------------
Merger module for the shareholder-pipeline project.

This module merges all individually parsed Excel files into a unified master
dataset with consistent column alignment, data type normalization, and metadata
enrichment.

Main classes:
    - ColumnMapper: Maps variant column names to canonical schema
    - DataNormalizer: Normalizes data types and values
    - Merger: Orchestrates the full merge process

Public API:
    - merge_all(input_folder, output_folder) -> pd.DataFrame
    - normalize_columns(df) -> pd.DataFrame
"""

from .merger import ColumnMapper, DataNormalizer, Merger, merge_all, normalize_columns

__all__ = [
    "ColumnMapper",
    "DataNormalizer",
    "Merger",
    "merge_all",
    "normalize_columns",
]

__version__ = "1.0.0"
