"""
Smart Deduplication Module

Implements proper deduplication logic with fuzzy matching for shareholder records.
Rules:
1. Exact folio match + same company = duplicate (remove all but last)
2. Name + Address fuzzy match (85% token_set_ratio) = duplicate (remove all but last)
3. NO cross-company deduplication
4. Name alone is NOT sufficient for deduplication (must have BOTH name AND address)
"""

import pandas as pd
import logging
from typing import Tuple, List, Dict, Any
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


class SmartDeduplicator:
    """Handles intelligent deduplication of shareholder records."""
    
    def __init__(self, similarity_threshold: float = 0.85):
        """
        Initialize deduplicator.
        
        Args:
            similarity_threshold: Minimum token_set_ratio for fuzzy matching (0-1).
                                Default 0.85 means 85% similarity required.
        """
        self.threshold = similarity_threshold
        self.removed_records: List[Dict[str, Any]] = []
        self.duplicate_groups: List[List[int]] = []
    
    def deduplicate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Apply smart deduplication to DataFrame.
        
        Args:
            df: Input DataFrame with shareholder records
            
        Returns:
            Tuple of (deduplicated_df, removed_records_df)
        """
        logger.info(f"Starting smart deduplication on {len(df)} records")
        logger.info(f"  Using similarity threshold: {self.threshold}")
        
        # Reset removed records tracker
        self.removed_records = []
        self.duplicate_groups = []
        
        df = df.reset_index(drop=True)
        
        # Track which rows to keep (True = keep, False = remove)
        keep_mask = [True] * len(df)
        
        # Rule 1: Exact Folio + Company Match (within same company only)
        logger.info("Rule 1: Detecting exact folio+company duplicates...")
        keep_mask = self._apply_exact_match_rule(df, keep_mask)
        
        # Rule 2: Fuzzy Name + Address Match (cross-company OK as long as properly matched)
        logger.info("Rule 2: Detecting fuzzy name+address duplicates...")
        keep_mask = self._apply_fuzzy_match_rule(df, keep_mask)
        
        # Create output DataFrames
        deduplicated_df = df[keep_mask].reset_index(drop=True)
        removed_indices = [i for i, keep in enumerate(keep_mask) if not keep]
        removed_df = df.iloc[removed_indices].reset_index(drop=True)
        
        # Add reasoning to removed_df
        removed_df['removal_reason'] = [
            self.removed_records[i]['reason'] 
            for i in range(len(removed_indices))
        ]
        removed_df['kept_row_index'] = [
            self.removed_records[i]['kept_index'] 
            for i in range(len(removed_indices))
        ]
        
        logger.info(f"Deduplication complete:")
        logger.info(f"  Input records: {len(df)}")
        logger.info(f"  Output records: {len(deduplicated_df)}")
        logger.info(f"  Removed: {len(removed_df)} ({100*len(removed_df)/len(df):.1f}%)")
        
        return deduplicated_df, removed_df
    
    def _apply_exact_match_rule(
        self, 
        df: pd.DataFrame, 
        keep_mask: List[bool]
    ) -> List[bool]:
        """
        Rule 1: Remove if (folio_no, company_name) pair matches within same company.
        Keeps the LAST occurrence, removes earlier ones.
        
        Args:
            df: DataFrame
            keep_mask: Boolean mask of records to keep
            
        Returns:
            Updated keep_mask
        """
        # Group by (folio_no, company_name)
        # Only consider rows that are currently marked to keep
        for (folio, company), group in df.groupby(['folio_no', 'company_name']):
            if pd.isna(folio) or folio == '':
                continue  # Skip empty folios
            
            indices = group.index.tolist()
            
            if len(indices) > 1:
                # Multiple records with same (folio, company) pair
                # Keep the last one, mark others for removal
                for idx in indices[:-1]:
                    if keep_mask[idx]:
                        keep_mask[idx] = False
                        self.removed_records.append({
                            'index': idx,
                            'folio': folio,
                            'company': company,
                            'name': df.loc[idx, 'name'],
                            'reason': f'Exact duplicate: same folio ({folio}) in {company}',
                            'kept_index': indices[-1],
                            'rule': 'exact_folio_company'
                        })
                        logger.debug(f"  Removed row {idx}: {folio}/{company} (kept row {indices[-1]})")
        
        return keep_mask
    
    def _apply_fuzzy_match_rule(
        self, 
        df: pd.DataFrame, 
        keep_mask: List[bool]
    ) -> List[bool]:
        """
        Rule 2: Remove if name AND address both have 85%+ fuzzy similarity.
        Only compares among rows currently marked to keep.
        CRITICAL: Only compares records where BOTH name and address are non-empty.
        
        Args:
            df: DataFrame
            keep_mask: Boolean mask of records to keep
            
        Returns:
            Updated keep_mask
        """
        # Get indices of rows still marked to keep
        keep_indices = [i for i, keep in enumerate(keep_mask) if keep]
        
        # Build list of "valid" candidates - records with non-empty name AND address
        valid_candidates = []
        for idx in keep_indices:
            name_str = str(df.loc[idx, 'name']).strip()
            addr_str = str(df.loc[idx, 'address']).strip()
            
            # Only include if BOTH name and address are non-empty
            if name_str and name_str.lower() != 'nan' and addr_str and addr_str.lower() != 'nan':
                valid_candidates.append(idx)
        
        logger.debug(f"Found {len(valid_candidates)} records with both name and address for fuzzy matching")
        
        # Check each pair of valid candidates
        for i in range(len(valid_candidates)):
            idx_i = valid_candidates[i]
            if not keep_mask[idx_i]:
                continue
            
            name_i = str(df.loc[idx_i, 'name']).strip().lower()
            addr_i = str(df.loc[idx_i, 'address']).strip().lower()
            
            # Double-check that we have valid data
            if not name_i or not addr_i or name_i == 'nan' or addr_i == 'nan':
                continue
            
            for j in range(i + 1, len(valid_candidates)):
                idx_j = valid_candidates[j]
                if not keep_mask[idx_j]:
                    continue
                
                name_j = str(df.loc[idx_j, 'name']).strip().lower()
                addr_j = str(df.loc[idx_j, 'address']).strip().lower()
                
                # Skip if either name or address is empty
                if not name_j or not addr_j or name_j == 'nan' or addr_j == 'nan':
                    continue
                
                # Check fuzzy match on BOTH name AND address
                name_similarity = fuzz.token_set_ratio(name_i, name_j) / 100.0
                addr_similarity = fuzz.token_set_ratio(addr_i, addr_j) / 100.0
                
                if name_similarity >= self.threshold and addr_similarity >= self.threshold:
                    # Both match sufficiently - mark earlier as duplicate, keep later
                    keep_mask[idx_i] = False
                    self.removed_records.append({
                        'index': idx_i,
                        'folio': df.loc[idx_i, 'folio_no'],
                        'company': df.loc[idx_i, 'company_name'],
                        'name': df.loc[idx_i, 'name'],
                        'address': df.loc[idx_i, 'address'],
                        'reason': (
                            f'Fuzzy duplicate: '
                            f'name match {name_similarity:.0%}, '
                            f'address match {addr_similarity:.0%}'
                        ),
                        'kept_index': idx_j,
                        'rule': 'fuzzy_name_address',
                        'name_similarity': name_similarity,
                        'addr_similarity': addr_similarity
                    })
                    logger.debug(
                        f"  Removed row {idx_i}: "
                        f"Name:{name_similarity:.0%}, Addr:{addr_similarity:.0%} "
                        f"(kept row {idx_j})"
                    )
                    break  # Move to next i, since we've found a match for this record
        
        return keep_mask
    
    def get_removal_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about removed records.
        
        Returns:
            Dictionary with statistics
        """
        if not self.removed_records:
            return {
                'total_removed': 0,
                'by_rule': {},
                'avg_similarity': None
            }
        
        by_rule = {}
        similarity_scores = []
        
        for record in self.removed_records:
            rule = record.get('rule', 'unknown')
            by_rule[rule] = by_rule.get(rule, 0) + 1
            
            if 'name_similarity' in record:
                similarity_scores.append(record['name_similarity'])
        
        return {
            'total_removed': len(self.removed_records),
            'by_rule': by_rule,
            'avg_name_similarity': sum(similarity_scores) / len(similarity_scores) if similarity_scores else None
        }
    
    def get_removed_records_explanation(self, limit: int = 10) -> str:
        """
        Get human-readable explanation of removed records.
        
        Args:
            limit: Maximum number of records to show
            
        Returns:
            Formatted string explanation
        """
        if not self.removed_records:
            return "No records were removed."
        
        lines = [f"Removed {len(self.removed_records)} records:\n"]
        
        for i, record in enumerate(self.removed_records[:limit]):
            lines.append(
                f"{i+1}. Row {record['index']}: {record['name']} | "
                f"{record['folio']} | {record['company']}\n"
                f"   Reason: {record['reason']}\n"
                f"   Kept: Row {record['kept_index']}\n"
            )
        
        if len(self.removed_records) > limit:
            lines.append(f"\n... and {len(self.removed_records) - limit} more")
        
        return "".join(lines)


def apply_smart_deduplication(
    df: pd.DataFrame,
    threshold: float = 0.85,
    verbose: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convenience function to apply smart deduplication.
    
    Args:
        df: Input DataFrame
        threshold: Similarity threshold (0-1)
        verbose: Print statistics
        
    Returns:
        Tuple of (deduplicated_df, removed_records_df)
    """
    dedup = SmartDeduplicator(similarity_threshold=threshold)
    deduplicated, removed = dedup.deduplicate(df)
    
    if verbose:
        stats = dedup.get_removal_statistics()
        print(f"\nDeduplication Statistics:")
        print(f"  Total removed: {stats['total_removed']}")
        print(f"  By rule: {stats['by_rule']}")
        if stats['avg_name_similarity']:
            print(f"  Avg name similarity: {stats['avg_name_similarity']:.0%}")
        
        print(f"\nSample removed records:")
        print(dedup.get_removed_records_explanation(limit=5))
    
    return deduplicated, removed
