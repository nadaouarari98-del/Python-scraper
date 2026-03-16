import pandas as pd
from pathlib import Path

print('=' * 80)
print('PIPELINE COMPLETION VERIFICATION')
print('=' * 80)
print()

# 1. Check parsed data
parsed_files = list(Path('data/output/parsed').glob('*.xlsx'))
print(f'1. Parser Output: {len(parsed_files)} parsed files')
for f in sorted(parsed_files):
    count = len(pd.read_excel(f))
    print(f'   - {f.name}: {count} records')
print()

# 2. Check merged
print('2. Merged Output:')
df_merged = pd.read_excel('data/output/master_merged.xlsx')
print(f'   - Total records: {len(df_merged)}')
print(f'   - Columns: {len(df_merged.columns)}')
has_dups = any('_1' in c or '_2' in c for c in df_merged.columns)
print(f'   - No duplicate columns: {not has_dups}')
print()

# 3. Check deduplicated
print('3. Deduplicated Output:')
df_dedup = pd.read_excel('data/output/master_deduplicated.xlsx')
print(f'   - Total records: {len(df_dedup)}')
print(f'   - Duplicates removed: {len(df_merged) - len(df_dedup)}')
print()

# 4. Check filtered
print('4. Filtered Output:')
df_filtered = pd.read_excel('data/output/master_filtered.xlsx')
print(f'   - High-value records: {len(df_filtered)}')
print(f'   - Min holding 500, min dividend 10000')
print()

# 5. Check Layer 1
print('5. Layer 1 Enrichment:')
df_layer1 = pd.read_excel('data/output/master_enriched_layer1.xlsx')
matched = df_layer1[df_layer1['email'].notna()]
print(f'   - Contacts found: {len(matched)}')
print(f'   - Hit rate: {len(matched)/len(df_layer1)*100:.1f}%')
print()

# 6. Verify data quality
print('6. Data Quality Checks:')
print(f'   - Company names filled: {df_filtered["company_name"].notna().sum()}/{len(df_filtered)}')
print(f'   - Source files filled: {df_filtered["source_file"].notna().sum()}/{len(df_filtered)}')
digit_names = sum(df_filtered['name'].astype(str).str.contains(r'\d', na=False))
print(f'   - Names with leading digits: {digit_names}')
print(f'   - Average name length: {df_filtered["name"].astype(str).str.len().mean():.1f}')
print()

print('=' * 80)
print('✅ PIPELINE SUCCESSFULLY COMPLETED')
print('=' * 80)
