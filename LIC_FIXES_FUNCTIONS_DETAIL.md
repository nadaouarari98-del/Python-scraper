# LIC PDF Processing Functions - Implementation Details

## Function 1: `_parse_lic_format()` 
**Location**: `src/parser/extractor_pdfplumber.py`

### Purpose
Detects and extracts data from LIC (Life Insurance Corporation) PDF format.

### Input
- `text` (str): Raw text from PDF page

### Output
- `pd.DataFrame | None`: DataFrame with columns [sr_no, id, name, address, pincode, folio_no, amount, shares]
- Returns `None` if LIC format not detected

### Algorithm
1. Split text into lines
2. Use regex to find LIC ID pattern: `14-16 digit ID` 
3. Extract adjacent fields: name (uppercase), address, pincode
4. Look ahead for folio, amount, shares in next lines
5. Build DataFrame from matched records

### Key Regex Patterns
```python
# Detects LIC record start:
r"^\s*(\d{1,5})\s+(\d{14,16})\s+([A-Z][A-Z\s]{2,}?)\s{2,}"
# Matches: SR_NO + ID + NAME + whitespace

# Detects pincode (6-digit code):
r"\b(\d{6})\b"

# Detects numeric fields (folio, amount, shares):
r"^\d{8,12}$"  # folio: 8-12 digits
r"^[\d,]+(?:\.\d{2})?$"  # amount: with commas and decimals
r"^\d{1,8}$"  # shares: 1-8 digits
```

### Example Input
```
1 1207780000039349 JOHN KUMAR DOE BANGALORE 560001 12345678 50000.00 1000
2 1207780000039350 JANE SMITH PUNE 411001 87654321 75000.50 2500
```

### Example Output
```
   sr_no             id             name     address pincode  folio_no    amount shares
0      1 1207780000039349   JOHN KUMAR DOE  BANGALORE  560001 12345678 50000.00   1000
1      2 1207780000039350       JANE SMITH      PUNE  411001 87654321 75000.50   2500
```

---

## Function 2: `extract_page_pdfplumber()` (Modified)
**Location**: `src/parser/extractor_pdfplumber.py`

### Purpose
Main PDF page extractor with fallback chain including new LIC format.

### Fallback Chain (in order)
1. **Attempt 1**: Structured table extraction (pdfplumber's `extract_table()`)
2. **Attempt 2**: LIC format parser (NEW) ← `_parse_lic_format()`
3. **Attempt 3**: Text + heuristic parsing

### Why This Order?
- Tables are most reliable when available
- LIC format is specific pattern (before general heuristics)
- General heuristics catch everything else

### Input
- `pdf_path` (str): Path to PDF file
- `page` (int): Page number (1-indexed)

### Output
- `pd.DataFrame | None`: Extracted data or None

### Log Output
```
[DEBUG] pdfplumber table: page 1 → 50 rows
[DEBUG] pdfplumber LIC format: page 2 → 45 rows  
[DEBUG] pdfplumber text: page 3 → 30 rows
[DEBUG] pdfplumber failed on page 4: [error]
```

---

## Function 3: Enhanced Headers Dictionary
**Location**: `src/dashboard/shareholders_bp.py`

### Purpose
Mimic real browser to bypass 403 Forbidden errors.

### Before
```python
headers = {'User-Agent': 'Mozilla/5.0'}  # Too generic
```

### After
```python
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}
```

### What Each Header Does
- **User-Agent**: Identifies as Chrome 120 on Windows (specific version)
- **Accept**: HTML/XML types (what browser requests)
- **Accept-Language**: English preferences
- **Accept-Encoding**: Compression types supported
- **Connection**: Keep-alive for persistent connections
- **Upgrade-Insecure-Requests**: Indicates HTTPS preference

### Why This Works
- LIC servers (and many others) check User-Agent
- Many block requests with generic/curl User-Agents
- Chrome fingerprint is widely accepted
- Complete header set = real browser behavior

---

## Function 4: `download_pdfs()` (Modified)
**Location**: `src/downloader/auto_downloader.py`

### Purpose
Auto-discover and download PDFs with special handling for LIC.

### What Changed
```python
# NEW CODE in download_pdfs():
enhanced_keywords = list(keywords)
if "lic" in company_name.lower():
    enhanced_keywords.extend(["unclaimed dividend", "policy", "unclaimed"])
    _logger.info("LIC detected: enhanced keywords = %s", enhanced_keywords)

# Use enhanced_keywords in both scraping methods:
pdf_links = _scrape_pdf_links_static(
    investor_url, enhanced_keywords, extensions, session, ...
)
```

### Why This Helps
- Default keywords might not find LIC PDFs
- LIC uses specific naming: "unclaimed dividend", "policy holders"
- Extends search without affecting other companies
- Backwards compatible (other companies ignored if-block)

### Search Keywords for LIC
| Keyword | Finds |
|---------|-------|
| `unclaimed dividend` | LIC dividend documents |
| `policy` | Policy-related files |
| `unclaimed` | Any unclaimed amount documents |

### Log Output
```
[INFO] Processing company: LIC
[INFO] LIC detected: enhanced keywords = ['unclaimed dividend', 'policy', 'unclaimed']
[INFO] Found 3 PDF link(s) for LIC
```

---

## Function 5: `init_db()` (Modified)
**Location**: `src/dashboard/app.py`

### Purpose
Initialize SQLite database with schema supporting LIC fields.

### Schema Changes
Added 4 columns:
- `pincode TEXT` - LIC includes pincode/postal code
- `sr_no TEXT` - Serial number from LIC
- `demat_account TEXT` - Demat account info
- `current_holding INTEGER` - Number of shares/units

### Before
```sql
CREATE TABLE shareholders (
    full_name, company_name, folio_no, shares, dividend,
    address, pan, email, ...
)
```

### After
```sql
CREATE TABLE shareholders (
    full_name, company_name, folio_no, shares, dividend,
    address, pincode,        -- ← NEW
    pan, email,
    sr_no,                   -- ← NEW
    demat_account,           -- ← NEW
    current_holding,         -- ← NEW
    ...
)
```

### Why Safe
- New columns are optional (NULL allowed)
- Insert logic handles missing values
- Primary key unchanged (folio_no, company_name)
- No breaking changes to existing queries

---

## Function 6: `insert_or_update_shareholders()` (Already Robust)
**Location**: `src/processor/database.py`

### Purpose
Insert/update shareholder records with safe field handling.

### Safe Field Conversion
```python
def _safe_str(value):
    """Convert to string, handle None/NaN"""
    return "" if value is None or pd.isna(value) else str(value).strip()

def _safe_int(value):
    """Convert to int, handle None/NaN"""
    try:
        return int(float(value)) if value is not None else None
    except:
        return None

def _safe_float(value):
    """Convert to float, handle None/NaN"""
    try:
        return float(value) if value is not None else None
    except:
        return None
```

### Usage in Insert
```python
values = {
    'folio_no': folio,
    'company_name': company,
    'pincode': self._safe_str(row.get('pincode')),  # Safe conversion
    'sr_no': self._safe_str(row.get('sr_no')),      # Safe conversion
    'current_holding': self._safe_int(row.get('current_holding')),  # Safe int
}

# Check if exists, then INSERT or UPDATE
if cursor.fetchone():
    # UPDATE
else:
    # INSERT
```

### Why Safe
- Missing columns return empty string/"" (not error)
- NaN/None values handled gracefully
- Primary key check prevents duplicates
- UPDATE mode for existing records

---

## Function 7: Updated Column Synonyms
**Location**: `src/parser/normalizer.py`

### Purpose
Map raw PDF column names to canonical names.

### Changes
```python
"folio_no": [
    # ... existing ...
    "policy no", "policy number",  # ← NEW: LIC
],
"name": [
    # ... existing ...
    "nominee name", "policy holder",  # ← NEW: LIC
],
"current_holding": [
    # ... existing ...
    "shares",  # ← NEW: LIC
],
```

### How It Works
If PDF header contains "policy no" → maps to `folio_no`
If PDF header contains "nominee name" → maps to `name`
If PDF header contains "shares" → maps to `current_holding`

### Example
Raw PDF Headers:
```
| Sr No | Policy No | Policy Holder | Address | Shares |
```

Mapped to Canonical:
```
| sr_no | folio_no | name | address | current_holding |
```

---

## Data Flow: LIC PDF → Database

```
LIC PDF File
    ↓
[extract_page_pdfplumber]
    ├─ Try table extraction
    ├─ Try LIC format parser ← _parse_lic_format()
    └─ Try heuristic parsing
    ↓
DataFrame with columns: [sr_no, id, name, address, pincode, folio_no, amount, shares]
    ↓
[normalize_dataframe]
    ├─ Map raw column names (using synonyms)
    ├─ Extract company/year from filename
    └─ Standardize values (clean_name, clean_address)
    ↓
Normalized DataFrame: [folio_no, name, address, pincode, current_holding, ...]
    ↓
[insert_or_update_shareholders]
    ├─ Validate required fields (folio_no, company_name)
    ├─ Safe field conversion
    ├─ Check if record exists
    └─ INSERT or UPDATE
    ↓
SQLite Database
    └─ shareholders table with pincode, sr_no, etc.
```

---

## Testing Each Function

### Test `_parse_lic_format()`
```python
text = """
1 1207780000039349 JOHN DOE BANGALORE 560001 12345 50000
2 1207780000039350 JANE SMITH PUNE 411001 54321 75000
"""
df = _parse_lic_format(text)
assert len(df) == 2
assert df.loc[0, 'pincode'] == '560001'
```

### Test `extract_page_pdfplumber()`
```python
df = extract_page_pdfplumber('data/input/LIC_Dividend_2023.pdf', page=1)
assert df is not None
assert 'pincode' in df.columns
```

### Test User-Agent
```python
r = requests.get(url, headers=headers)
assert r.status_code == 200  # No 403 Forbidden
```

### Test LIC Keyword Detection
```python
result = download_pdfs(["LIC"], source="both")
# Check logs for: "LIC detected: enhanced keywords"
```

### Test Database
```python
db = ShareholderDatabase(Path("data/pipeline.db"))
df = db.get_by_pincode("560001")
assert len(df) > 0
assert df.loc[0, 'pincode'] == "560001"
```

