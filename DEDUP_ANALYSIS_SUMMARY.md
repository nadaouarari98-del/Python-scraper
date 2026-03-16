# Data Deduplication Analysis Summary

## Current Data Status

### File: `master_shareholder_data_unified.xlsx` (Parsing Output)
- **Records**: 1,449
- **Unique Folios**: 1,437
- **Unique (Folio, Company) pairs**: 1,437
- **True Duplicates** (exact folio+company match): 12

### File: `master_merged.xlsx` (Merger Output)  
- **Records**: 2,903
- **Unique Folios**: 1,400 (approx)
- **Source**: Combines 7 parsed Excel files across multiple companies

## Problem Identified

### Current Deduplication Logic (AGGRESSIVE - TOO MANY REMOVED)
**Location**: `src/parser/excel_writer.py` line 160
```python
combined.drop_duplicates(subset=['folio_no', 'source_file'], keep='last')
```

**Issue**: This deduplicates on `(folio_no, source_file)` pair, which treats:
- Same folio number in **DIFFERENT source files** = DUPLICATE (WRONG!)
- This loses legitimate multi-company shareholders

**Example**: 
- Shareholder John Doe has folio 12345 in TechMahindra (2017)
- Same shareholder has different folio 67890 in IEPF dividend (2017)
- These should be considered duplicates if fuzzy match confirms, but current logic doesn't catch this

### What Should Happen (Per User Specification)
Remove duplicates ONLY if:

**Rule 1**: Exact Folio Match + Same Company = Duplicate
- If `folio_no` matches exactly AND `company_name` is the same
- Action: Keep 1, remove others

**Rule 2**: Name + Address Fuzzy Match (85% similarity) = Duplicate
- If `name` and `address` both match at 85% token_set_ratio
- Action: Keep latest record, remove others

**Rule 3**: NO Cross-Company Deduplication
- Different companies = NEVER a duplicate, even if folio/name match

**Rule 4**: Name Alone = NOT a duplicate
- Must have BOTH name AND address fuzzy match (not name alone)

## Test Data

### Sample Analysis: 1,449 → 1,437 with Aggressive Folio+Company Dedup
```
True duplicates found: 12 records
Removed records:
- Records with identical (folio_no, company_name) pairs
- These are genuine duplicates within same company
```

### Merged Dataset: 2,903 records
- Preserves multi-company shareholders
- Includes records from 7 different data sources
- Need to implement proper smart dedup on this too

## Next Steps

1. **Fix excel_writer.py** (line 155-160)
   - Remove or modify aggressive drop_duplicates
   - Add proper dedup logic post-processing

2. **Implement Proper Deduplication Function**
   - Create new function: `smart_deduplicate(df, threshold=0.85)`
   - Use rapidfuzz.fuzz.token_set_ratio for fuzzy matching
   - Track which records were removed and why

3. **Apply to Both Outputs**
   - Apply to unified master (1,449)
   - Apply to merged output (2,903)

4. **Validation**
   - Expected result after smart dedup: ~1,400-1,430 records (remove only true duplicates)
   - Sample output showing removed records with explanations
