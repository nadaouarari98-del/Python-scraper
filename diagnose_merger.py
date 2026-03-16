#!/usr/bin/env python3
"""
Diagnose the merger deduplication issue.
Shows:
1. How many records in source parsed files
2. How deduplication is affecting records
3. Which records are being treated as duplicates
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

import pandas as pd
from rapidfuzz import fuzz

# Count records in parsed files
parsed_dir = Path("data/output/parsed")
total_records = 0
files_data = []

print("\n" + "=" * 80)
print("SOURCE PARSED FILES ANALYSIS")
print("=" * 80)

for xlsx_file in sorted(parsed_dir.glob("*.xlsx")):
    try:
        df = pd.read_excel(xlsx_file)
        total_records += len(df)
        files_data.append({
            'file': xlsx_file.name,
            'count': len(df),
            'df': df
        })
        print(f"  {xlsx_file.name:50s} {len(df):6d} records")
    except Exception as e:
        print(f"  ERROR reading {xlsx_file.name}: {e}")

print(f"\n  TOTAL RECORDS (all parsed files): {total_records}")

# Now simulate the aggressive deduplication
print("\n" + "=" * 80)
print("AGGRESSIVE DEDUPLICATION (current bug: folio_no + source_file)")
print("=" * 80)

all_records = []
for file_info in files_data:
    df = file_info['df'].copy()
    df['source_file_parsed'] = file_info['file']
    all_records.append(df)

combined = pd.concat(all_records, ignore_index=True, sort=True)
print(f"  Before deduplication: {len(combined)} records")

# This is what the current code does: drop_duplicates on (folio_no, source_file)
dedup_cols = ['folio_no', 'source_file_parsed']
valid_dedup = [c for c in dedup_cols if c in combined.columns]
if valid_dedup:
    combined_dedup = combined.drop_duplicates(subset=valid_dedup, keep='last')
    removed = len(combined) - len(combined_dedup)
    print(f"  After dedup on {valid_dedup}: {len(combined_dedup)} records")
    print(f"  RECORDS REMOVED: {removed} ({100*removed/len(combined):.1f}%)")

# Now show what CORRECT deduplication should be
print("\n" + "=" * 80)
print("CORRECT DEDUPLICATION RULES:")
print("=" * 80)
print("""
  Rule 1: Remove duplicate ONLY if Folio matches exactly
          (same person, same company, appears twice in same or different files)
  
  Rule 2: OR if Name AND Address match with 85% fuzzy similarity
          (likely same person but name spelled differently)
  
  Rule 3: DO NOT deduplicate across different companies
          (same person can be shareholder in multiple companies)
  
  Rule 4: DO NOT deduplicate if only name matches
          (many people have same name)
""")

# Analyze what should be deduplicated
print("\n" + "=" * 80)
print("DETAILED ANALYSIS: Finding actual duplicates")
print("=" * 80)

# Group by folio_no to find true duplicates (same person)
folio_duplicates = combined.groupby('folio_no').filter(lambda x: len(x) > 1)
if len(folio_duplicates) > 0:
    print(f"\n  Records with DUPLICATE FOLIO (same person, likely duplicate): {len(folio_duplicates)}")
    
    # Group to show unique folios that appear multiple times
    dup_folios = folio_duplicates['folio_no'].unique()
    print(f"  Unique folios that appear multiple times: {len(dup_folios)}")
    
    # Show a sample
    print(f"\n  Sample of duplicate folio entries:")
    for folio in sorted(dup_folios)[:5]:
        entries = folio_duplicates[folio_duplicates['folio_no'] == folio]
        print(f"\n    Folio: {folio}")
        for idx, row in entries.iterrows():
            name = str(row.get('name', 'N/A'))
            company = str(row.get('company_name', 'N/A'))
            source = str(row.get('source_file_parsed', 'N/A'))
            print(f"      - {name:30s} | {company:20s} | {source}")

# Show records with same name + address
print(f"\n\n  Cross-company analysis (same name in multiple companies):")
name_company_combos = combined.groupby(['name', 'company_name']).size()
multi_company = combined['name'].value_counts()
multi_company_names = multi_company[multi_company > 1].index.tolist()
print(f"    Names appearing in multiple companies: {len(multi_company_names)}")

if multi_company_names:
    print(f"\n    Sample names appearing multiple times (across different sources):")
    for name in sorted(multi_company_names)[:5]:
        entries = combined[combined['name'] == name]
        companies = entries['company_name'].unique()
        print(f"      {name:30s} appears in {len(companies)} sources")
        for company in companies:
            count = len(entries[entries['company_name'] == company])
            print(f"        - {company}: {count}")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print(f"""
  Original records (all parsed files):     {total_records:,}
  After aggressive dedup (folio+file):     {len(combined_dedup):,}
  Records lost:                            {total_records - len(combined_dedup):,}
  
  This is WRONG because:
  - Aggressive dedup treats each file independently
  - Same person in different sources = duplicate = deleted
  - This loses legitimate multi-company shareholders
  
  Fix needed: Only deduplicate on folio_no WITHIN a company,
  allowing same person to appear in multiple companies.
""")
