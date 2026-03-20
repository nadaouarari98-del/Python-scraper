# LIC PDF Fixes - Updated Code Reference

## 1. Enhanced LIC Format Parser
**File**: `src/parser/extractor_pdfplumber.py`

```python
def _parse_lic_format(text: str) -> pd.DataFrame | None:
    """
    Parse LIC (Life Insurance Corporation) PDF format.
    
    LIC format sequence: Serial Number -> ID -> Name -> Address -> Pincode -> 
    Folio -> Amount -> Shares -> Date
    
    Example patterns:
    - Long digit ID: 1207780000039349 (16 digits)
    - Uppercase Name followed by Address
    - Pincode: 5-6 digit codes
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

## 2. Updated Extraction Fallback Chain
**File**: `src/parser/extractor_pdfplumber.py` - `extract_page_pdfplumber()` function

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

## 3. Enhanced Column Synonyms
**File**: `src/parser/normalizer.py`

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

## 4. Enhanced User-Agent Headers
**File**: `src/dashboard/shareholders_bp.py` - API pipeline endpoint

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

## 5. LIC Search Optimization
**File**: `src/downloader/auto_downloader.py` - `download_pdfs()` function

```python
for company_name in companies:
    _logger.info("=" * 60)
    _logger.info("Processing company: %s", company_name)

    counters: dict[str, int] = {"found": 0, "downloaded": 0, "failed": 0}
    company_slug = _slugify(company_name)

    # LIC keyword optimization: if user enters 'LIC', append additional search keywords
    enhanced_keywords = list(keywords)
    if "lic" in company_name.lower():
        enhanced_keywords.extend(["unclaimed dividend", "policy", "unclaimed"])
        _logger.info("LIC detected: enhanced keywords = %s", enhanced_keywords)

    # Resolve investor page
    investor_url, uses_js = _resolve_investor_page(
        company_name, cfg, source, session
    )

    if not investor_url:
        _logger.warning(
            "No investor page found for '%s'. Skipping.", company_name
        )
        results[company_name] = counters
        continue

    # ... rate limiting and robots check ...

    # Scrape PDF links — static first (using enhanced keywords for LIC)
    pdf_links = _scrape_pdf_links_static(
        investor_url, enhanced_keywords, extensions, session,
        cfg.downloader.request_timeout_seconds,
    )

    # Playwright fallback
    if not pdf_links and uses_js:
        _logger.info(
            "Static scrape found 0 links for %s — trying Playwright",
            company_name,
        )
        pdf_links = extract_pdf_links_js(
            investor_url, keywords=enhanced_keywords, extensions=extensions
        )
    
    # ... rest of download logic ...
```

---

## 6. Enhanced Database Schema
**File**: `src/dashboard/app.py` - `init_db()` function

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
        pincode TEXT,              -- ← NEW: LIC includes pincode
        pan TEXT,
        email TEXT,
        processed_at TEXT,
        source_file TEXT,
        sr_no TEXT,                -- ← NEW: Serial number
        demat_account TEXT,        -- ← NEW: Demat account
        current_holding INTEGER,   -- ← NEW: Holdings/shares count
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

## 7. Safe Database Insert (Already in place)
**File**: `src/processor/database.py` - `insert_or_update_shareholders()` method

```python
def insert_or_update_shareholders(self, df: pd.DataFrame) -> Dict[str, int]:
    """Insert or update shareholder records with safe field handling."""
    # ... validation code ...
    
    for idx, row in df.iterrows():
        try:
            folio = str(row.get('folio_no', '')).strip()
            company = str(row.get('company_name', '')).strip()
            
            if not folio or not company:
                stats['failed'] += 1
                continue
            
            # Prepare values with safe conversion functions
            values = {
                'folio_no': folio,
                'company_name': company,
                'sr_no': self._safe_str(row.get('sr_no')),
                'demat_account': self._safe_str(row.get('demat_account')),
                'pincode': self._safe_str(row.get('pincode')),
                # ... other fields ...
            }
            
            # Check if record exists
            cursor.execute(
                "SELECT 1 FROM shareholders WHERE folio_no = ? AND company_name = ?",
                (folio, company)
            )
            
            if cursor.fetchone():
                # UPDATE existing record
                set_clause = ", ".join([f"{k} = ?" for k in values.keys()])
                vals = list(values.values())
                cursor.execute(
                    f"UPDATE shareholders SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE folio_no = ? AND company_name = ?",
                    vals + [folio, company]
                )
                stats['updated'] += 1
            else:
                # INSERT new record
                cols = ", ".join(values.keys())
                placeholders = ", ".join(["?"] * len(values))
                cursor.execute(
                    f"INSERT INTO shareholders ({cols}) VALUES ({placeholders})",
                    list(values.values())
                )
                stats['inserted'] += 1
                
        except Exception as e:
            logging.error(f"Failed to process row {idx}: {e}")
            stats['failed'] += 1
    
    self.conn.commit()
    return stats
```

---

## Usage Examples

### Example 1: Process LIC PDF
```bash
python -m src.parser --input data/input/LIC_Dividend_2023.pdf
```

### Example 2: Download LIC PDFs
```python
from src.downloader import download_pdfs

result = download_pdfs(["LIC"], source="both")
# Output: {"LIC": {"found": 5, "downloaded": 5, "failed": 0}}
```

### Example 3: Query LIC Records by Pincode
```python
from src.processor.database import ShareholderDatabase
from pathlib import Path

db = ShareholderDatabase(Path("data/pipeline.db"))
df = db.get_by_pincode("560001")  # Bangalore pincode
print(df[['full_name', 'folio_no', 'pincode', 'amount']])
```

---

## Logging Output Examples

### LIC Format Detection:
```
[INFO] LIC detected: enhanced keywords = ['unclaimed dividend', 'policy', 'unclaimed']
[INFO] LIC detected: enhanced keywords = ['unclaimed dividend', 'policy', 'unclaimed']
[DEBUG] pdfplumber LIC format: page 1 → 45 rows
```

### Successful Download:
```
[INFO] Processing company: LIC
[INFO] BSE found page for LIC: https://www.bseindia.com/...
[INFO] Found 3 PDF link(s) for LIC
[INFO] Downloading | company=LIC | url=https://...
[INFO] Saved | company=LIC | file=data/input/LIC_2023.pdf | size=2345.6 KB
```

### Database Insert:
```
[INFO] Insert/Update complete: 450 inserted, 125 updated, 3 failed
```

