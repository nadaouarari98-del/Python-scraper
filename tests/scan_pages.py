#!/usr/bin/env python3
import pdfplumber
from pathlib import Path

pdf_path = Path('tests/sample_pdfs/Details-of-Unpaid-Unclaimed-Dividend-31.03.2021.pdf')

with pdfplumber.open(pdf_path) as pdf:
    print('PDF Pages Analysis:')
    for i in [0, 1, 2, 18, 19, 20, 21]:
        if i < len(pdf.pages):
            page = pdf.pages[i]
            tables = page.find_tables()
            text = page.extract_text()
            preview = text[:50] if text else '[EMPTY]'
            print(f'Page {i+1:2d}: tables={len(tables):2d}, text_len={len(text or "")}, preview={preview}')
