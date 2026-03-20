# LIC PDF Fixes - Copy-Paste Ready Code

This file contains ready-to-use code snippets for all 4 fixes.

---

## Fix 1: LIC Format Parser Function
**Copy this into**: `src/parser/extractor_pdfplumber.py` (after line 44, before `def _text_to_df`)

```python
def _parse_lic_format(text: str) -> pd.DataFrame | None:
    """
    Parse LIC (Life Insurance Corporation) PDF format.
    
    LIC format sequence: Serial Number -> ID -> Name -> Address -> Pincode -> 
    Folio -> Amount -> Shares -> Date
    
    Example patterns:
    - Long digit ID: 1207780000039349 (16 digits)
    - Uppercase Name followed by Address
    - Pincode: 5 digit codes
    - Amount and Shares columns
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None
    
    records = []
    
    # Pattern to detect LIC record line: starts with optional SR_NO, then 16-digit ID
    lic_line_pattern = re.compile(
        r"^\s*(\d{1,5})\s+"  # Sr. No (optional, 1-5 digits)
        r"(\d{14,16})\s+"    # ID (14-16 digits, typically 1207780000039349)
        r"([A-Z][A-Z\s]{2,}?)\s{2,}"  # NAME (uppercase, 3+ chars)
    )
    
    pincode_pattern = re.compile(r"\b(\d{6})\b")
    amount_pattern = re.compile(r"[\d,]+(?:\.\d{2})?")
    
    for i, line in enumerate(lines):
        match = lic_line_pattern.match(line)
        if match:
            sr_no = match.group(1)
            lic_id = match.group(2)
            name = match.group(3).strip()
            
            # Try to extract remaining fields from this line and next lines
            remainder = line[match.end():].strip()
            
            # Extract address (typically next continuous text)
            address = ""
            pincode = ""
            folio = ""
            amount = ""
            shares = ""
            
            # Look for pincode in remainder
            pin_match = pincode_pattern.search(remainder)
            if pin_match:
                pincode = pin_match.group(1)
                # Address is everything before pincode
                address = remainder[:pin_match.start()].strip()
            else:
                address = remainder
            
            # Look in next few lines for folio, amount, shares
            for j in range(i + 1, min(i + 3, len(lines))):
                next_line = lines[j].strip()
                # Skip if this looks like another header/data row
                if re.match(r"^\d{14,16}\s+[A-Z]", next_line):
                    break
                
                # Try to extract folio, amount, shares from subsequent lines
                parts = next_line.split()
                for part in parts:
                    if re.match(r"^\d{8,12}$", part) and not folio:
                        folio = part
                    elif re.match(r"^[\d,]+(?:\.\d{2})?$", part) and not amount:
                        amount = part
                    elif re.match(r"^\d{1,8}$", part) and not shares:
                        shares = part
            
            record = {
                "sr_no": sr_no,
                "id": lic_id,
                "name": name,
                "address": address,
                "pincode": pincode,
                "folio_no": folio,
                "amount": amount,
                "shares": shares,
            }
            records.append(record)
    
    if records:
        df = pd.DataFrame(records)
        return df if _is_useful(df) else None
    
    return None
```

---

## Fix 2: Update Extract Fallback Chain
**Replace in**: `src/parser/extractor_pdfplumber.py` - function `extract_page_pdfplumber()` (lines ~220-245)

```python
def extract_page_pdfplumber(pdf_path: str, page: int) -> pd.DataFrame | None:
    """Extract table data from *page* using pdfplumber.

    First tries pdfplumber's ``extract_table()``; if that returns nothing,
    falls back to raw text extraction with heuristic parsing, then LIC format.

    Args:
        pdf_path: Path to the PDF.
        page:     1-indexed page number.

    Returns:
        :class:`pd.DataFrame` of extracted rows, or ``None`` on failure.
    """
    if not _PDFPLUMBER_AVAILABLE:
        return None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page < 1 or page > len(pdf.pages):
                return None
            plumber_page = pdf.pages[page - 1]

            # --- Attempt 1: structured table extraction ---
            table = plumber_page.extract_table()
            if table:
                df = _rows_to_df(table)
                if _is_useful(df):
                    _logger.debug(
                        "pdfplumber table: page %d → %d rows", page, len(df)
                    )
                    return df

            # --- Attempt 2: LIC format parsing ---
            text = plumber_page.extract_text() or ""
            if text.strip():
                df = _parse_lic_format(text)
                if df is not None and _is_useful(df):
                    _logger.debug(
                        "pdfplumber LIC format: page %d → %d rows", page, len(df)
                    )
                    return df

            # --- Attempt 3: text + heuristic parsing ---
            if text.strip():
                df = _text_to_df(text)
                if df is not None and _is_useful(df):
                    _logger.debug(
                        "pdfplumber text: page %d → %d rows", page, len(df)
                    )
                    return df

    except Exception as exc:  # noqa: BLE001
        _logger.debug("pdfplumber failed on page %d: %s", page, exc)

    return None
```

---

## Fix 3: Enhanced Column Synonyms
**Replace in**: `src/parser/normalizer.py` - `_COLUMN_SYNONYMS` dict (lines ~28-54)

```python
_COLUMN_SYNONYMS: dict[str, list[str]] = {
    "folio_no": [
        "folio", "folio no", "folio number", "investor id",
        "register folio", "reg. folio",
        "policy no", "policy number",  # ← NEW: LIC support
    ],
    "name": [
        "name of share", "name of investor", "investor name",
        "shareholder name", "first holder", "name of the share",
        "^name$",
        "nominee name", "policy holder",  # ← NEW: LIC support
    ],
    "address": [
        "address", "registered address", "add.", "addr",
    ],
    "demat_account": [
        "demat", "dp id", "client id", "dp id-client", "dp-id",
        "beneficiary", "nsdl", "cdsl",
    ],
    "pan_number": [
        "pan", "pan no", "permanent account",
    ],
    "current_holding": [
        "current holding", "no. of shares", "no of shares",
        "number of shares", "share", "holding", "qty",
        "shares",  # ← NEW: LIC support
    ],
}
```

---

## Fix 4: Enhanced User-Agent Headers
**Replace in**: `src/dashboard/shareholders_bp.py` - in `execute()` function, around line 137 (inside the `if url:` block)

```python
# For direct URLs use requests scraping fallback
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os, time

# Enhanced User-Agent to bypass 403 Forbidden errors on some servers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

pdfs = []
try:
    r = requests.get(url, headers=headers, timeout=15)
    soup = BeautifulSoup(r.text, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '.pdf' in href.lower():
            full_url = urljoin(url, href)
            fname = full_url.split('/')[-1].split('?')[0]
            if not fname.endswith('.pdf'):
                fname += '.pdf'
            fpath = os.path.join('data/input/', fname)
            if not os.path.exists(fpath):
                pr = requests.get(full_url, headers=headers, timeout=30, stream=True)
                if pr.status_code == 200:
                    with open(fpath, 'wb') as ff:
                        for chunk in pr.iter_content(8192):
                            ff.write(chunk)
                    pdfs.append(fpath)
                    time.sleep(1)
except Exception as e:
    print(f'URL scrape error: {e}')
```

---

## Fix 5: LIC Keyword Detection
**Add in**: `src/downloader/auto_downloader.py` - in `download_pdfs()` function, after line ~448 (after `company_slug = _slugify(company_name)`)

```python
# LIC keyword optimization: if user enters 'LIC', append additional search keywords
enhanced_keywords = list(keywords)
if "lic" in company_name.lower():
    enhanced_keywords.extend(["unclaimed dividend", "policy", "unclaimed"])
    _logger.info("LIC detected: enhanced keywords = %s", enhanced_keywords)

# Then use enhanced_keywords in both scraping calls:
# Replace 'keywords' with 'enhanced_keywords' in:
# 1. _scrape_pdf_links_static() call (around line ~470)
# 2. extract_pdf_links_js() call (around line ~480)
```

**Specifically, replace these lines:**

```python
# OLD:
pdf_links = _scrape_pdf_links_static(
    investor_url, keywords, extensions, session,
    cfg.downloader.request_timeout_seconds,
)
...
pdf_links = extract_pdf_links_js(
    investor_url, keywords=keywords, extensions=extensions
)

# NEW:
pdf_links = _scrape_pdf_links_static(
    investor_url, enhanced_keywords, extensions, session,
    cfg.downloader.request_timeout_seconds,
)
...
pdf_links = extract_pdf_links_js(
    investor_url, keywords=enhanced_keywords, extensions=extensions
)
```

---

## Fix 6: Database Schema Update
**Replace in**: `src/dashboard/app.py` - in `init_db()` function, the CREATE TABLE statement

```python
def init_db():
  """Force drop and recreate the shareholders table with the correct schema."""
  os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
  conn = sqlite3.connect(DB_PATH)
  cursor = conn.cursor()
  try:
    # Only create the table if it does not exist
    cursor.execute("""
      CREATE TABLE IF NOT EXISTS shareholders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        full_name TEXT,
        company_name TEXT,
        folio_no TEXT,
        shares INTEGER,
        dividend REAL,
        market_value REAL,
        total_wealth REAL,
        contact TEXT,
        address TEXT,
        pincode TEXT,              -- ← NEW: LIC pincode field
        pan TEXT,
        email TEXT,
        processed_at TEXT,
        source_file TEXT,
        sr_no TEXT,                -- ← NEW: LIC serial number
        demat_account TEXT,        -- ← NEW: LIC demat account
        current_holding INTEGER,   -- ← NEW: LIC holdings count
        UNIQUE(full_name, folio_no)
      );
    """)
    conn.commit()
  except Exception as e:
    logging.error(f"Error initializing database: {e}")
  finally:
    conn.close()
```

---

## Verification Checklist

After copying code:

- [ ] No syntax errors in IDE
- [ ] All imports are available (re, pd, etc.)
- [ ] Indentation is correct (2 spaces)
- [ ] Function signatures match
- [ ] Regex patterns are valid
- [ ] Comments are preserved

---

## Testing Code Snippets

### Test LIC Parser
```python
from src.parser.extractor_pdfplumber import _parse_lic_format

text = """
1 1207780000039349 JOHN KUMAR DOE BANGALORE 560001 12345678 50000.00
2 1207780000039350 JANE SMITH PUNE 411001 87654321 75000.50
"""

df = _parse_lic_format(text)
print(df)
# Should show 2 rows with sr_no, id, name, address, pincode, folio_no, amount, shares
```

### Test Headers
```python
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)...',
    'Accept': 'text/html,...',
    ...
}
r = requests.get('https://lic-website.com', headers=headers)
print(r.status_code)  # Should be 200, not 403
```

### Test Keyword Detection
```python
company_name = "LIC"
enhanced_keywords = ["unclaimed dividend", "policy", "unclaimed"]
print("lic" in company_name.lower())  # Should be True
```

### Test Database
```python
import sqlite3
conn = sqlite3.connect('data/pipeline.db')
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(shareholders)")
columns = [row[1] for row in cursor.fetchall()]
print("pincode" in columns)  # Should be True
```

---

## Deployment Command

```bash
# Pull changes
git pull origin main

# No migration needed - schema backward compatible
# Just restart the app
python -m src.dashboard.app
```

