"""
src/processor/deduplicator/deduplicator.py
------------------------------------------
Remove duplicate entries from merged master shareholder dataset using:
  1. Fuzzy matching on Name + Address (configurable threshold, default 85%)
  2. Exact matching on Demat Account Number

Exposes:
  - deduplicate(df, threshold=85) -> (clean_df, removed_df)
  - find_duplicates(df, threshold=85) -> duplicate_pairs_df
"""

import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Tuple, Dict, List, Optional, Set

import pandas as pd
import numpy as np
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


class Deduplicator:
    """
    Remove duplicates from shareholder dataset using fuzzy matching and exact matching.
    
    Rules:
      1. Fuzzy: name_similarity >= threshold AND address_similarity >= threshold
      2. Exact: demat_account matches exactly
    
    When duplicates found:
      - Keep record with most complete data (fewest NaN/empty fields)
      - If tied, keep most recent record
      - Removed records tracked for audit
    """

    def __init__(self, threshold: int = 85, verbose: bool = True):
        """
        Initialize deduplicator.
        
        Args:
            threshold: Fuzzy matching threshold (0-100), default 85%
            verbose: Enable detailed logging
        """
        self.threshold = max(0, min(100, threshold))  # Clamp 0-100
        self.verbose = verbose
        
        # Statistics tracking
        self.stats = {
            "total_before": 0,
            "duplicates_found": 0,
            "exact_demat_matches": 0,
            "fuzzy_name_address_matches": 0,
            "total_after": 0,
            "data_completeness_kept": 0,
            "recency_kept": 0,
        }
        
        # Duplicate tracking
        self.duplicate_pairs: List[Dict] = []
        self.removed_indices: Set[int] = set()

    def deduplicate(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Remove duplicates from dataset.
        
        Returns:
            (clean_df, removed_df): Clean dataset and removed records with reasons
        """
        if df.empty:
            return df.copy(), pd.DataFrame()

        self.stats["total_before"] = len(df)
        df = df.reset_index(drop=True)

        # Find duplicates using both methods
        self._find_demat_duplicates(df)
        self._find_fuzzy_duplicates(df)

        # Remove duplicates, keeping the best record
        clean_df = self._apply_deduplication(df)

        self.stats["total_after"] = len(clean_df)
        self.stats["duplicates_found"] = len(self.removed_indices)

        # Build removed records dataframe with reasons
        removed_df = self._build_removed_records_df(df)

        if self.verbose:
            self._log_statistics()

        return clean_df, removed_df

    def find_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Find duplicates without removing them (for review).
        
        Returns:
            DataFrame of duplicate pairs with match details
        """
        if df.empty:
            return pd.DataFrame()

        df = df.reset_index(drop=True)

        # Find duplicates
        self._find_demat_duplicates(df)
        self._find_fuzzy_duplicates(df)

        # Convert duplicate_pairs to DataFrame
        if not self.duplicate_pairs:
            return pd.DataFrame()

        result_df = pd.DataFrame(self.duplicate_pairs)
        return result_df.sort_values("similarity_score", ascending=False)

    def _find_demat_duplicates(self, df: pd.DataFrame) -> None:
        """Find exact duplicates by demat_account."""
        if "demat_account" not in df.columns:
            return

        # Normalize demat accounts
        demat_normalized = df["demat_account"].fillna("").str.strip().str.upper()

        # Group by demat account
        for demat, group_indices in demat_normalized[demat_normalized != ""].groupby(
            demat_normalized
        ).groups.items():
            if len(group_indices) > 1:
                # Multiple records with same demat account
                indices = list(group_indices)
                for i, primary_idx in enumerate(indices):
                    for secondary_idx in indices[i + 1 :]:
                        self.duplicate_pairs.append(
                            {
                                "primary_idx": primary_idx,
                                "secondary_idx": secondary_idx,
                                "match_type": "exact_demat",
                                "similarity_score": 100.0,
                                "field": "demat_account",
                                "value_1": df.loc[primary_idx, "demat_account"],
                                "value_2": df.loc[secondary_idx, "demat_account"],
                            }
                        )
                self.stats["exact_demat_matches"] += len(indices) - 1

    def _find_fuzzy_duplicates(self, df: pd.DataFrame) -> None:
        """
        Find fuzzy duplicates using name + address matching.
        
        Optimization: Only compare records with same first 3 characters of name.
        """
        if "name" not in df.columns or "address" not in df.columns:
            return

        # Prepare data: normalize strings, skip empty names
        names = df["name"].fillna("").str.strip().str.lower()
        addresses = df["address"].fillna("").str.strip().str.lower()

        # Filter to records with both name and address
        valid_mask = (names != "") & (addresses != "")
        valid_indices = df[valid_mask].index.tolist()

        if len(valid_indices) < 2:
            return

        # Bucket by first 3 characters of name for optimization
        name_buckets: Dict[str, List[int]] = {}
        for idx in valid_indices:
            bucket_key = names[idx][:3] if len(names[idx]) >= 3 else names[idx]
            if bucket_key not in name_buckets:
                name_buckets[bucket_key] = []
            name_buckets[bucket_key].append(idx)

        # Compare within each bucket
        for bucket_indices in name_buckets.values():
            if len(bucket_indices) < 2:
                continue

            for i, primary_idx in enumerate(bucket_indices):
                for secondary_idx in bucket_indices[i + 1 :]:
                    if primary_idx in self.removed_indices or secondary_idx in self.removed_indices:
                        continue

                    name_sim = fuzz.token_sort_ratio(names[primary_idx], names[secondary_idx])
                    addr_sim = fuzz.token_sort_ratio(addresses[primary_idx], addresses[secondary_idx])

                    # Both must meet threshold
                    if name_sim >= self.threshold and addr_sim >= self.threshold:
                        # Use average similarity
                        avg_sim = (name_sim + addr_sim) / 2.0

                        self.duplicate_pairs.append(
                            {
                                "primary_idx": primary_idx,
                                "secondary_idx": secondary_idx,
                                "match_type": "fuzzy_name_address",
                                "similarity_score": avg_sim,
                                "name_similarity": name_sim,
                                "address_similarity": addr_sim,
                                "field": "name + address",
                            }
                        )
                        self.stats["fuzzy_name_address_matches"] += 1

    def _apply_deduplication(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply deduplication rules: keep best record, remove others."""
        # Build a mapping of which records to keep/remove
        keep_record = {idx: True for idx in range(len(df))}

        # Process each duplicate pair
        for pair in self.duplicate_pairs:
            primary_idx = pair["primary_idx"]
            secondary_idx = pair["secondary_idx"]

            # Skip if either already marked for removal
            if primary_idx not in keep_record or secondary_idx not in keep_record:
                continue

            if not keep_record[primary_idx] or not keep_record[secondary_idx]:
                continue

            # Decide which to keep
            keep_idx = self._choose_best_record(df, primary_idx, secondary_idx)
            remove_idx = secondary_idx if keep_idx == primary_idx else primary_idx

            keep_record[remove_idx] = False
            self.removed_indices.add(remove_idx)

        # Apply removal
        clean_df = df[df.index.map(lambda idx: keep_record.get(idx, True))].copy()
        return clean_df.reset_index(drop=True)

    def _choose_best_record(self, df: pd.DataFrame, idx1: int, idx2: int) -> int:
        """
        Choose which record to keep (the other will be removed).
        
        Priority:
          1. Record with most complete data (fewest NaN/empty fields)
          2. Record with most recent timestamp/year
          3. First occurrence (idx1)
        
        Returns: Index of record to keep
        """
        # Count null/empty fields
        completeness1 = self._calculate_completeness(df.iloc[idx1])
        completeness2 = self._calculate_completeness(df.iloc[idx2])

        if completeness1 != completeness2:
            return idx1 if completeness1 > completeness2 else idx2

        # If tied, use recency (most recent timestamp/year)
        recency1 = self._get_recency_score(df.iloc[idx1])
        recency2 = self._get_recency_score(df.iloc[idx2])

        if recency1 != recency2:
            self.stats["recency_kept"] += 1
            return idx1 if recency1 > recency2 else idx2

        # Default: keep first occurrence
        self.stats["data_completeness_kept"] += 1
        return idx1

    def _calculate_completeness(self, record: pd.Series) -> int:
        """
        Calculate how complete a record is (inverse of NaN count).
        
        Returns: Number of non-empty fields
        """
        non_empty = record.notna().sum() - record[record.notna()].apply(
            lambda x: str(x).strip() == ""
        ).sum()
        return int(non_empty)

    def _get_recency_score(self, record: pd.Series) -> float:
        """
        Get recency score for a record.
        
        Looks for: parsed_at, year, date_field columns
        Returns: Unix timestamp or year as float
        """
        # Try parsed_at timestamp first
        if "parsed_at" in record and pd.notna(record["parsed_at"]):
            try:
                if isinstance(record["parsed_at"], str):
                    dt = pd.to_datetime(record["parsed_at"])
                else:
                    dt = record["parsed_at"]
                return dt.timestamp()
            except Exception:
                pass

        # Try year field
        if "year" in record and pd.notna(record["year"]):
            try:
                return float(record["year"])
            except Exception:
                pass

        # Try any date-like column
        for col in record.index:
            if "date" in col.lower() or "year" in col.lower():
                try:
                    val = pd.to_datetime(record[col])
                    if isinstance(val, pd.Timestamp):
                        return val.timestamp()
                except Exception:
                    pass

        return 0.0

    def _build_removed_records_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build dataframe of removed records with removal reasons."""
        if not self.removed_indices:
            return pd.DataFrame()

        removed_list = []

        for removed_idx in sorted(self.removed_indices):
            # Find which duplicate pair this belongs to
            reason = "Duplicate removed"
            similarity = None
            match_type = None

            for pair in self.duplicate_pairs:
                if pair["secondary_idx"] == removed_idx or pair["primary_idx"] == removed_idx:
                    match_type = pair["match_type"]
                    similarity = pair.get("similarity_score", pair.get("name_similarity", "N/A"))
                    break

            removed_record = df.iloc[removed_idx].to_dict()
            removed_record["removal_reason"] = reason
            removed_record["match_type"] = match_type
            removed_record["similarity_score"] = similarity
            removed_record["removed_at"] = datetime.now().isoformat()

            removed_list.append(removed_record)

        if not removed_list:
            return pd.DataFrame()

        return pd.DataFrame(removed_list)

    def _log_statistics(self) -> None:
        """Log deduplication statistics."""
        logger.info("\n" + "=" * 70)
        logger.info("DEDUPLICATION STATISTICS")
        logger.info("=" * 70)
        logger.info(f"Total records before: {self.stats['total_before']}")
        logger.info(f"Total duplicates found: {self.stats['duplicates_found']}")
        logger.info(f"  - Exact demat matches: {self.stats['exact_demat_matches']}")
        logger.info(f"  - Fuzzy name+address matches: {self.stats['fuzzy_name_address_matches']}")
        logger.info(f"Total records after: {self.stats['total_after']}")
        logger.info(f"Retention rate: {100 * self.stats['total_after'] / max(1, self.stats['total_before']):.1f}%")
        logger.info(f"Fuzzy threshold: {self.threshold}%")
        logger.info("=" * 70)

    def get_statistics(self) -> Dict:
        """Return deduplication statistics as dict."""
        return self.stats.copy()


def deduplicate(
    df: pd.DataFrame, threshold: int = 85, verbose: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Public API: Remove duplicates from DataFrame.
    
    Args:
        df: Input DataFrame with shareholder records
        threshold: Fuzzy matching threshold (0-100), default 85
        verbose: Enable logging
        
    Returns:
        (clean_df, removed_df): Clean dataset and removed records
    """
    dedup = Deduplicator(threshold=threshold, verbose=verbose)
    return dedup.deduplicate(df)


def find_duplicates(
    df: pd.DataFrame, threshold: int = 85, verbose: bool = True
) -> pd.DataFrame:
    """
    Public API: Find duplicates without removing them.
    
    Args:
        df: Input DataFrame
        threshold: Fuzzy matching threshold (0-100)
        verbose: Enable logging
        
    Returns:
        DataFrame of duplicate pairs with match details
    """
    dedup = Deduplicator(threshold=threshold, verbose=verbose)
    return dedup.find_duplicates(df)
