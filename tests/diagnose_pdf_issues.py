#!/usr/bin/env python3
"""
Diagnose PDF extraction issues by inspecting structure and content.
"""

import pdfplumber
from pathlib import Path

SAMPLE_DIR = Path('tests/sample_pdfs')

print("="*80)
print("PROBLEM 1: details-of-shareholders-dividend-outstanding-for-7-consecutive-years.pdf")
print("="*80)

pdf1_path = SAMPLE_DIR / 'details-of-shareholders-dividend-outstanding-for-7-consecutive-years.pdf'

if pdf1_path.exists():
    with pdfplumber.open(pdf1_path) as pdf:
        print(f"\nTotal pages: {len(pdf.pages)}")
        
        # Inspect first page
        for page_num in [0, 1]:
            print(f"\n{'='*80}")
            print(f"PAGE {page_num + 1}")
            print('='*80)
            page = pdf.pages[page_num]
            
            # Raw text
            print(f"\nRAW TEXT (first 500 chars):")
            text = page.extract_text()
            print(text[:500] if text else "[NO TEXT EXTRACTED]")
            
            # Tables
            tables = page.find_tables()
            print(f"\nTABLES FOUND: {len(tables)}")
            for t_idx, table_obj in enumerate(tables[:2]):  # First 2 tables
                table = table_obj.extract()
                print(f"\n  Table {t_idx + 1}:")
                print(f"    Rows: {len(table)}")
                print(f"    Columns: {len(table[0]) if table else 0}")
                if table:
                    print(f"    Header row: {table[0][:5]}")
                    if len(table) > 1:
                        print(f"    First data row: {table[1][:5]}")
            
            # Extract table
            print(f"\nEXTRACT_TABLE() result:")
            extracted = page.extract_table()
            if extracted:
                print(f"  Type: {type(extracted)}")
                print(f"  Shape: {len(extracted)} rows x {len(extracted[0]) if extracted else 0} cols")
                print(f"  Header: {extracted[0][:5] if extracted else 'N/A'}")
                if len(extracted) > 1:
                    print(f"  Row 1: {extracted[1][:5]}")
            else:
                print("  [No table extracted]")
else:
    print(f"File not found: {pdf1_path}")

print("\n\n" + "="*80)
print("PROBLEM 2: Details-of-Unpaid-Unclaimed-Dividend-31.03.2021.pdf")
print("="*80)

pdf2_path = SAMPLE_DIR / 'Details-of-Unpaid-Unclaimed-Dividend-31.03.2021.pdf'

if pdf2_path.exists():
    with pdfplumber.open(pdf2_path) as pdf:
        print(f"\nTotal pages: {len(pdf.pages)}")
        
        # Inspect pages 1, 20, and 21
        for page_num in [0, 19, 20]:
            if page_num >= len(pdf.pages):
                continue
                
            print(f"\n{'='*80}")
            print(f"PAGE {page_num + 1}")
            print('='*80)
            page = pdf.pages[page_num]
            
            # Raw text
            print(f"\nRAW TEXT (first 500 chars):")
            text = page.extract_text()
            print(text[:500] if text else "[NO TEXT EXTRACTED]")
            
            # Tables
            print(f"\nTABLES FOUND: {len(page.tables)}")
            for t_idx, table in enumerate(page.tables[:2]):
                print(f"\n  Table {t_idx + 1}:")
                print(f"    Rows: {len(table)}")
                print(f"    Columns: {len(table[0]) if table else 0}")
                if table:
                    print(f"    Header row: {table[0][:5]}")
                    if len(table) > 1:
                        print(f"    First data row: {table[1][:5]}")
            
            # Extract table
            print(f"\nEXTRACT_TABLE() result:")
            extracted = page.extract_table()
            if extracted:
                print(f"  Rows: {len(extracted)}")
                print(f"  Header: {extracted[0][:5] if extracted else 'N/A'}")
                if len(extracted) > 1:
                    print(f"  Row 1: {extracted[1][:5]}")
            else:
                print("  [No table extracted]")
else:
    print(f"File not found: {pdf2_path}")

print("\n" + "="*80)
