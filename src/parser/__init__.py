"""
shareholder-pipeline parser
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Public API surface for the parser module.
"""

from .pdf_parser import parse_pdf, parse_all_pdfs

__all__ = ["parse_pdf", "parse_all_pdfs"]
