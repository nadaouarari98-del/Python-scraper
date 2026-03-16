# shareholder-pipeline / downloader

Automated downloader and processor for **IEPF unclaimed dividend** and **shareholding pattern** PDFs from BSE/NSE-listed company investor pages.

---

## Quick Start

```powershell
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Install Playwright browsers (needed for JS-rendered pages)
playwright install chromium
```

---

## Modes

### Mode 1 — Auto-download

Discovers and downloads IEPF/shareholding PDFs from company investor pages.

```powershell
# Download for specific companies (BSE + NSE)
python -m src.downloader --mode auto --companies "Tech Mahindra,Reliance,TCS"

# BSE only
python -m src.downloader --mode auto --companies "TCS" --source bse

# With a custom config path
python -m src.downloader --mode auto --companies "Infosys" --config path/to/sources.yaml
```

### Mode 2 — Manual upload

Copies local PDF files into the project's input directory with standardised naming.

```powershell
# Upload a folder of PDFs (interactive — prompts for unknown company/year)
python -m src.downloader --mode manual --path C:\Downloads\iepf_pdfs\

# Upload a single file
python -m src.downloader --mode manual --path C:\Downloads\TCS_IEPF_2023.pdf

# Batch mode — no prompts, marks unknown fields
python -m src.downloader --mode manual --path C:\Downloads\iepf_pdfs\ --no-interactive
```

### Show Progress

```powershell
python -m src.downloader --status
```

Sample output:
```json
{
  "total_found": 12,
  "downloaded": 10,
  "failed": 2,
  "last_updated": "2026-03-12T21:00:00Z"
}
```

---

## Python API

```python
from src.downloader import download_pdfs, upload_pdfs

# Mode 1
results = download_pdfs(["Tech Mahindra", "TCS"], source="both")
# { "Tech Mahindra": { "found": 4, "downloaded": 4, "failed": 0 }, ... }

# Mode 2
saved = upload_pdfs("/path/to/pdfs/", interactive=False)
# ["/path/to/data/input/tech-mahindra/2023/tech-mahindra_2023_file.pdf", ...]
```

---

## Configuration

Edit **`config/sources.yaml`** to:

- Add new company investor page URLs under `known_companies`
- Change BSE/NSE search endpoints
- Tune rate limits, retries, and timeouts

---

## Output Structure

```
data/
├── input/
│   └── {company-slug}/
│       └── {year}/
│           └── {company}_{year}_{original}.pdf
└── logs/
    ├── downloader.log          ← full debug log (rotating, 5 MB × 3)
    ├── failed_downloads.log    ← one line per failed URL
    └── progress_status.json    ← dashboard-readable counters
```

---

## Running Tests

```powershell
pytest tests/ -v
```

---

## Tech Stack

| Library | Purpose |
|---|---|
| `requests` | HTTP downloads |
| `beautifulsoup4` + `lxml` | HTML parsing |
| `playwright` | JS-rendered page fallback |
| `pyyaml` | Config loading |
| `tenacity` | Retry with exponential backoff |
| `tqdm` | Progress bars |
