#!/usr/bin/env python3
"""
Comprehensive PDF analysis script.
Reads each PDF, identifies column structures, and creates unified schema mappings.
"""

from pathlib import Path
import pdfplumber
import json

# Standard output paths
SCHEMA_FILE = Path('data/output/pdf_schema_analysis.json')
SCHEMA_FILE.parent.mkdir(parents=True, exist_ok=True)

sample_pdfs = sorted(Path('tests/sample_pdfs').glob('*.pdf'))
print(f"\n{'='*80}")
print(f"ANALYZING {len(sample_pdfs)} SAMPLE PDFs")
print('='*80)

analysis = {}

for pdf_path in sample_pdfs:
    print(f"\n{pdf_path.name}")
    print('-' * 80)
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_tables = []
            all_columns = set()
            
            print(f"  Pages: {len(pdf.pages)}")
            
            # Scan ALL pages for tables (not just first 3)
            for page_idx, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                if tables:
                    for table_idx, table in enumerate(tables):
                        if not table or len(table) < 2:
                            continue
                        
                        # Get header (first row)
                        header = table[0]
                        columns = [str(c).strip() if c else f'_col{i}' for i, c in enumerate(header)]
                        all_columns.update(col for col in columns if col)
                        
                        # Store table info
                        all_tables.append({
                            'page': page_idx + 1,
                            'table': table_idx,
                            'rows': len(table) - 1,
                            'cols': len(columns),
                            'header': columns,
                        })
                        
                        print(f"    Page {page_idx+1}, Table {table_idx+1}: {len(table)-1} rows × {len(columns)} cols")
            
            # Analyze columns
            print(f"\n  Total tables found: {len(all_tables)}")
            print(f"  Unique columns across all tables: {len(all_columns)}")
            
            # Smart column naming - find core non-numeric columns
            core_cols = []
            for col in sorted(all_columns):
                if col and not col[0].isdigit() and not col[0] == '_':
                    core_cols.append(col)
            
            print(f"\n  Core column names ({len(core_cols)}):")
            for i, col in enumerate(core_cols[:20], 1):
                print(f"    {i:2d}. {col[:60]}")
            if len(core_cols) > 20:
                print(f"        ... and {len(core_cols)-20} more")
            
            analysis[pdf_path.name] = {
                'pages': len(pdf.pages),
                'tables': len(all_tables),
                'all_columns': sorted(all_columns),
                'core_columns': core_cols,
                'table_info': all_tables,
            }
    
    except Exception as e:
        print(f"  ERROR: {str(e)[:100]}")
        analysis[pdf_path.name] = {'error': str(e)[:100]}

# Save analysis
print(f"\n{'='*80}")
print("SAVING ANALYSIS...")
print('='*80)

with open(SCHEMA_FILE, 'w', encoding='utf-8') as f:
    json.dump(analysis, f, indent=2)
print(f"Saved to: {SCHEMA_FILE}")

# Print summary
print(f"\n{'='*80}")
print("SCHEMA PATTERNS DISCOVERED")
print('='*80)

for pdf_name, data in analysis.items():
    if 'error' not in data:
        print(f"\n{pdf_name}")
        print(f"  Columns: {len(data['all_columns'])} total, {len(data['core_columns'])} core")
        if data['core_columns']:
            print(f"  Key fields: {', '.join(data['core_columns'][:5])}")

print(f"\n{'='*80}")
print("UNIFIED SCHEMA TARGET")
print('='*80)

unified_schema = [
    'sr_no', 'folio_no', 'demat_no', 'name', 'address',
    'current_holding', 'total_dividend',
    'fy_2017_18_final', 'fy_2018_19_final', 
    'fy_2019_20_interim', 'fy_2019_20_final',
    'fy_2020_21_interim', 'fy_2020_21_final',
    'fy_2021_22_interim', 'fy_2021_22_final',
    'fy_2022_23_interim', 'fy_2022_23_final',
    'fy_2023_24_interim', 'fy_2023_24_final',
    'fy_2024_25_interim',
    'company_name', 'source_pdf', 'financial_year', 'date_processed',
    'mobile', 'email', 'contact_source', 'verification_status', 'crm_push_status', 'email_sent_status'
]

print(f"Target unified schema ({len(unified_schema)} fields):")
for i, field in enumerate(unified_schema, 1):
    print(f"  {i:2d}. {field}")

print("\nDone!")
