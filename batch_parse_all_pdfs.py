#!/usr/bin/env python3
"""
Master batch parser: parse all PDFs in tests/sample_pdfs and create unified master Excel.
"""

from pathlib import Path
import sys
import logging
from datetime import datetime

import pandas as pd

sys.path.insert(0, str(Path.cwd()))

from src.parser.pdf_parser import parse_pdf
from src.parser.normalizer import normalize_to_unified_master, UNIFIED_MASTER_COLUMNS

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger(__name__)

SAMPLE_PDFS_DIR = Path('tests/sample_pdfs')
OUTPUT_DIR = Path('data/output')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

master_records = []
parse_results = []

sample_pdfs = sorted(SAMPLE_PDFS_DIR.glob('*.pdf'))
logger.info(f"Found {len(sample_pdfs)} PDFs to parse")

for i, pdf_path in enumerate(sample_pdfs, 1):
    logger.info(f"\n[{i}/{len(sample_pdfs)}] Processing: {pdf_path.name}")
    
    try:
        # Parse PDF — returns a normalized DataFrame directly
        df = parse_pdf(
            str(pdf_path),
            output_dir=str(OUTPUT_DIR / 'parsed'),
            master_path=str(OUTPUT_DIR / 'master_shareholder_data.xlsx'),
            skip_excel_write=False  # Let parse_pdf handle Excel writing
        )
        
        if df is None or df.empty:
            logger.warning(f"  No records extracted from {pdf_path.name}")
            parse_results.append({
                'pdf': pdf_path.name,
                'status': 'NO_RECORDS',
                'record_count': 0,
                'error': ''
            })
            continue
        logger.info(f"  Extracted {len(df)} raw records")
        
        # DataFrame already normalized by parse_pdf() — just accumulate
        logger.info(f"  Columns: {list(df.columns)[:5]}...")
        master_records.append(df)
        parse_results.append({
            'pdf': pdf_path.name,
            'status': 'SUCCESS',
            'record_count': len(df),
            'error': ''
        })
        
    except Exception as e:
        logger.error(f"  ERROR: {str(e)[:150]}")
        parse_results.append({
            'pdf': pdf_path.name,
            'status': 'ERROR',
            'record_count': 0,
            'error': str(e)[:200]
        })

# Combine all records
if master_records:
    logger.info(f"\n{'='*80}")
    logger.info("Combining {0} DataFrames...".format(len(master_records)))
    
    # Combine with union of all columns
    master_df = pd.concat(master_records, ignore_index=True, sort=False)
    
    logger.info(f"Raw combined shape: {master_df.shape}")
    logger.info(f"Raw columns: {list(master_df.columns)[:5]}...")
    
    # Normalize to unified master schema
    logger.info("Normalizing to unified master schema...")
    master_df = normalize_to_unified_master(master_df)
    
    logger.info(f"\nUnified schema:")
    logger.info(f"  Rows: {len(master_df)}")
    logger.info(f"  Columns: {len(master_df.columns)}")
    logger.info(f"  Column names: {list(master_df.columns)}")
    
    # Save master Excel
    output_path = OUTPUT_DIR / 'master_shareholder_data_unified.xlsx'
    
    # Retry logic for file lock
    max_retries = 3
    for attempt in range(max_retries):
        try:
            master_df.to_excel(output_path, index=False, engine='openpyxl')
            logger.info(f"\nSaved unified master to: {output_path}")
            break
        except PermissionError as e:
            if attempt < max_retries - 1:
                import time
                logger.warning(f"File locked, retrying in 3 seconds... (attempt {attempt+1}/{max_retries})")
                time.sleep(3)
            else:
                logger.error(f"Failed to save after {max_retries} attempts: {e}")
                raise
    
    # Summary
    logger.info(f"\n{'='*80}")
    logger.info("PARSE SUMMARY")
    logger.info('='*80)
    for result in parse_results:
        status_icon = "OK" if result['status'] == 'SUCCESS' else 'FAIL'
        logger.info(f"[{status_icon}] {result['pdf']}: {result['record_count']} records")
    
    logger.info(f"\nTotal records in master: {len(master_df)}")
    logger.info(f"Unique companies: {master_df['company_name'].nunique()}")
    logger.info(f"Unique source files: {master_df['source_pdf'].nunique()}")
    
else:
    logger.error("No records extracted from any PDF")

logger.info("\nDone!")
