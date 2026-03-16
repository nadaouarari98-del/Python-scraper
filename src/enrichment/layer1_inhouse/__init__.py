"""
Package initialization for Layer 1 in-house contact search module.

Exports the main Layer1InhouseSearch class and public API functions.
"""

from .layer1_inhouse import Layer1InhouseSearch, search_inhouse_batch

__all__ = [
    'Layer1InhouseSearch',
    'search_inhouse_batch'
]
