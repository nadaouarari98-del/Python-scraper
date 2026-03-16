"""
Package initialization for filter module.

Exports the main Filter class and public API functions.
"""

from .filter import Filter, apply_filter, apply_preset

__all__ = [
    'Filter',
    'apply_filter',
    'apply_preset'
]
