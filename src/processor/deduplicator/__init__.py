"""
src/processor/deduplicator/__init__.py
-------------------------------------
Shareholder deduplication module.

Remove duplicates from merged master dataset using fuzzy matching and exact matching.
"""

from .deduplicator import (
    Deduplicator,
    deduplicate,
    find_duplicates,
)

__all__ = ["Deduplicator", "deduplicate", "find_duplicates"]
