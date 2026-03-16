"""
shareholder-pipeline downloader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Public API surface for the downloader module.
"""

from .auto_downloader import download_pdfs
from .manual_uploader import upload_pdfs

__all__ = ["download_pdfs", "upload_pdfs"]
