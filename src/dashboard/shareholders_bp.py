from flask import Blueprint, request, jsonify, send_file
import math
import os
import io
import json
from datetime import datetime
import pandas as pd

shareholders_bp = Blueprint('shareholders', __name__)

@shareholders_bp.route('/api/get-records')
def api_get_records():
    try:
        df = _load_df()
        records = df.fillna('').astype(str).to_dict('records')
        return jsonify({'records': records, 'total': len(records)})
    except Exception as e:
        return jsonify({'error': str(e), 'records': []}), 500

def _load_df():
    prices_file = 'data/output/master_merged_with_prices.xlsx'
    merged_file = 'data/output/master_merged.xlsx'
    filepath = prices_file if os.path.exists(prices_file) else merged_file
    return pd.read_excel(filepath, sheet_name=0, na_filter=False)

def _filter_df(df, search, company, min_wealth):
    if search:
        s = search.lower()
        mask = (
            df.get('full_name', pd.Series(dtype=str)).astype(str).str.lower().str.contains(s, na=False) |
            df.get('folio_no', pd.Series(dtype=str)).astype(str).str.lower().str.contains(s, na=False) |
            df.get('company_name', pd.Series(dtype=str)).astype(str).str.lower().str.contains(s, na=False)
        )
        df = df[mask]
    if company:
        df = df[df.get('company_name', pd.Series(dtype=str)).astype(str) == company]
    if min_wealth > 0 and 'total_wealth' in df.columns:
        df = df[pd.to_numeric(df['total_wealth'], errors='coerce').fillna(0) >= min_wealth]
    return df

@shareholders_bp.route('/api/shareholders')
def api_shareholders():
    sort = request.args.get('sort', 'full_name')
    order = request.args.get('order', 'asc')
    search = request.args.get('search', '')
    company = request.args.get('company', '')
    min_wealth = request.args.get('min_wealth', 0, type=float)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    only_new = request.args.get('only_new', '0') == '1'
    try:
        df = _load_df()
    except Exception as e:
        return jsonify({"error": str(e), "records": [], "total": 0, "page": 1, "pages": 1, "companies": []})
    df = _filter_df(df, search, company, min_wealth)
    if only_new:
        # Filter to *latest upload batch* (not "all PDFs ever uploaded").
        # This manifest is written by `/api/upload` right after saving files.
        manifest_path = 'data/output/last_upload_manifest.json'
        upload_files: set[str] = set()

        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, 'r', encoding='utf-8') as fh:
                    manifest = json.load(fh) or {}
                upload_files = set(manifest.get('pdf_files', []) or [])
            except Exception:
                upload_files = set()

        # If there's no manifest (or it's empty), show no records.
        # This matches the "hide any existing data" behavior.
        if 'source_file' in df.columns:
            df = df[
                df['source_file']
                .fillna('')
                .astype(str)
                .apply(lambda x: os.path.basename(x) in upload_files)
            ]
        else:
            df = df.iloc[0:0]
    # Apply sorting
    if sort and sort in df.columns:
        try:
            numeric_sort = pd.to_numeric(df[sort], errors='coerce')
            if numeric_sort.notna().sum() > len(df) * 0.3:
                df = df.assign(_sort=numeric_sort).sort_values('_sort', ascending=(order=='asc')).drop('_sort', axis=1)
            else:
                df = df.sort_values(sort, ascending=(order=='asc'), key=lambda x: x.astype(str).str.lower())
        except Exception as e:
            print(f'Sort error on {sort}: {e}')
    total = len(df)
    pages = max(1, math.ceil(total / per_page))
    start = (page - 1) * per_page
    page_df = df.iloc[start:start+per_page]
    cols = ['full_name','company_name','folio_no','current_holding','total_dividend','market_value','total_wealth','contact_number']
    available = [c for c in cols if c in page_df.columns]
    records = page_df[available].fillna('').astype(str).to_dict('records')
    companies = sorted(df['company_name'].astype(str).unique().tolist()) if 'company_name' in df.columns else []
    return jsonify({"records": records, "total": total, "page": page, "pages": pages, "companies": companies})

@shareholders_bp.route('/api/shareholders/download')
def api_shareholders_download():
    search = request.args.get('search', '')
    company = request.args.get('company', '')
    min_wealth = request.args.get('min_wealth', 0, type=float)
    try:
        df = _load_df()
        df = _filter_df(df, search, company, min_wealth)
        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        return send_file(output, download_name='shareholders_filtered.xlsx',
                        as_attachment=True,
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return jsonify({"error": str(e)})
_pipeline_status = {
    "running": False, "step": "Idle", "message": "",
    "progress": 0, "pdfs_found": 0, "records": 0
}

@shareholders_bp.route('/api/pipeline/run', methods=['POST'])
def api_run_pipeline():
    global _pipeline_status
    from flask import request as req
    import threading, subprocess, os
    if _pipeline_status['running']:
        return jsonify({"error": "Pipeline already running. Please wait."})
    data = req.get_json() or {}
    url = data.get('url', '')
    company = data.get('company', '')

    def execute():
        global _pipeline_status
        import requests, os, time, re
        from urllib.parse import urlparse
        input_root = 'data/input'
        manifest_path = 'data/output/last_upload_manifest.json'

        def list_input_pdf_basenames() -> set[str]:
            basenames: set[str] = set()
            if not os.path.exists(input_root):
                return basenames
            for root, _dirs, files in os.walk(input_root):
                for fname in files:
                    if fname.lower().endswith('.pdf'):
                        basenames.add(fname)
            return basenames

        def slugify(name: str) -> str:
            slug = name.lower().strip()
            slug = re.sub(r"[^a-z0-9]+", "-", slug)
            return slug.strip("-")

        def search_pdfs_serper(company_url, api_key, keywords=None, max_results=10):
            # Query Serper.dev API for PDF links
            search_query = f'site:{company_url} filetype:pdf "IEPF" "Shareholder"'
            url = 'https://google.serper.dev/search'
            headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
            data = {"q": search_query, "num": max_results}
            try:
                resp = requests.post(url, headers=headers, json=data, timeout=20)
                if resp.status_code != 200:
                    _logger = print
                    _logger(f"Serper API error: {resp.status_code} {resp.text}")
                    return []
                results = resp.json()
                pdf_links = []
                for item in results.get("organic", []):
                    link = item.get("link", "")
                    if link.lower().endswith(".pdf"):
                        pdf_links.append(link)
                return pdf_links
            except Exception as e:
                print(f"Serper API exception: {e}")
                return []

        def download_pdf_with_headers(pdf_url, output_dir, user_agent, proxies=None):
            os.makedirs(output_dir, exist_ok=True)
            filename = pdf_url.split('/')[-1].split('?')[0]
            if not filename.lower().endswith('.pdf'):
                filename += '.pdf'
            filename = filename.replace('%20', '_').replace(' ', '_')[:100]
            filepath = os.path.join(output_dir, filename)
            if os.path.exists(filepath):
                return filepath
            headers = {'User-Agent': user_agent}
            try:
                r = requests.get(pdf_url, headers=headers, timeout=30, stream=True, proxies=proxies)
                if r.status_code == 403:
                    print(f"403 Forbidden for {pdf_url}")
                if r.status_code == 404:
                    print(f"404 Not Found for {pdf_url}")
                r.raise_for_status()
                content_type = r.headers.get('content-type', '')
                if 'pdf' not in content_type.lower() and 'octet' not in content_type.lower():
                    print(f"Non-PDF content for {pdf_url}: {content_type}")
                    return None
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                time.sleep(1)
                return filepath
            except Exception as e:
                print(f"Download error for {pdf_url}: {e}")
                return None

        # --- Begin new logic ---
        _pipeline_status = {"running": True, "step": "Searching",
                           "message": f"Finding PDFs for {company or url}...",
                           "progress": 5, "pdfs_found": 0, "records": 0}
        before_pdfs = list_input_pdf_basenames()
        pdfs = []
        SERPER_API_KEY = os.environ.get("SERPER_API_KEY")
        USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
        PROXY_URL = os.environ.get("PDF_PROXY_URL")  # e.g. http://user:pass@proxyhost:port
        proxies = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

        if url:
            # Extract domain for site search
            parsed = urlparse(url)
            domain = parsed.netloc
            if not SERPER_API_KEY:
                print("Missing SERPER_API_KEY env variable for PDF search.")
                pdf_links = []
            else:
                pdf_links = search_pdfs_serper(domain, SERPER_API_KEY)
            if not pdf_links:
                print(f"No PDFs found via Serper for {domain}. Logging response.")
            for pdf_url in pdf_links:
                path = download_pdf_with_headers(pdf_url, input_root, USER_AGENT, proxies=proxies)
                if path:
                    pdfs.append(path)
        elif company:
            # Use known company URL or fallback to Serper
            from src.downloader import find_company_url
            company_url = find_company_url(company)
            if not company_url:
                print(f'No known URL for {company}, using Serper search.')
                company_url = company
            parsed = urlparse(company_url)
            domain = parsed.netloc
            if not SERPER_API_KEY:
                print("Missing SERPER_API_KEY env variable for PDF search.")
                pdf_links = []
            else:
                pdf_links = search_pdfs_serper(domain, SERPER_API_KEY)
            if not pdf_links:
                print(f"No PDFs found via Serper for {domain}. Logging response.")
            for pdf_url in pdf_links:
                path = download_pdf_with_headers(pdf_url, input_root, USER_AGENT, proxies=proxies)
                if path:
                    pdfs.append(path)
        else:
            pdfs = []

        after_pdfs = list_input_pdf_basenames()
        new_pdfs = sorted(after_pdfs - before_pdfs)
        if company:
            target_slug = slugify(company)
            pdfs = sorted([f for f in after_pdfs if f.startswith(f"{target_slug}_")])
        else:
            pdfs = new_pdfs

        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        with open(manifest_path, 'w', encoding='utf-8') as fh:
            json.dump(
                {'pdf_files': pdfs, 'created_at': datetime.now().isoformat()},
                fh,
                ensure_ascii=False,
            )
        _pipeline_status.update({"step": "Downloaded",
                                "message": f"Downloaded {len(pdfs)} PDFs",
                                "progress": 25, "pdfs_found": len(pdfs)})
        if not pdfs:
            _pipeline_status = {"running": False, "step": "No PDFs found",
                               "message": "No PDFs found via search API. Try pasting the direct investor page URL.",
                               "progress": 0, "pdfs_found": 0, "records": 0}
            return
        # --- Continue with pipeline ---
        import subprocess
        _pipeline_status.update({"step": "Parsing PDFs",
                                "message": "Extracting shareholder records...",
                                "progress": 40})
        subprocess.run(['python', '-m', 'src.parser', '--input', 'data/input/'],
                      capture_output=True, cwd=os.getcwd())
        _pipeline_status.update({"step": "Merging",
                                "message": "Merging all records into master dataset...",
                                "progress": 60})
        subprocess.run(['python', '-m', 'src.processor.merger'],
                      capture_output=True, cwd=os.getcwd())
        _pipeline_status.update({"step": "Deduplicating",
                                "message": "Removing duplicate records...",
                                "progress": 75})
        subprocess.run(['python', '-m', 'src.processor.deduplicator'],
                      capture_output=True, cwd=os.getcwd())
        _pipeline_status.update({"step": "Market Prices",
                                "message": "Fetching NSE/BSE market prices...",
                                "progress": 90})
        subprocess.run(['python', '-m', 'src.enrichment.market_price',
                       '--input', 'data/output/master_merged.xlsx'],
                      capture_output=True, cwd=os.getcwd())
        try:
            import pandas as pd
            df = pd.read_excel('data/output/master_merged.xlsx', sheet_name=0, na_filter=False)
            record_count = len(df)
        except:
            record_count = 0
        _pipeline_status = {"running": False, "step": "Complete",
                           "message": f"Done! {record_count} shareholder records processed.",
                           "progress": 100, "pdfs_found": len(pdfs), "records": record_count}
    threading.Thread(target=execute, daemon=True).start()
    return jsonify({"status": "started"})

@shareholders_bp.route('/api/pipeline/status')
def api_pipeline_status():
    return jsonify(_pipeline_status)

@shareholders_bp.route('/api/upload', methods=['POST'])
def api_upload_pdf():
    from flask import request as req
    import threading, subprocess, os
    if 'file' not in req.files:
        return jsonify({"error": "No file provided"})
    files = req.files.getlist('file')
    saved = []
    upload_dir = 'data/input/uploads/'
    os.makedirs(upload_dir, exist_ok=True)
    for f in files:
        if f.filename.lower().endswith('.pdf'):
            safe_name = f.filename.replace(' ', '_')
            path = os.path.join(upload_dir, safe_name)
            f.save(path)
            saved.append(safe_name)
            print(f'Uploaded: {f.filename}')
    if saved:
        # Persist this upload batch so `only_new=1` can reliably show only these results.
        manifest_path = 'data/output/last_upload_manifest.json'
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        with open(manifest_path, 'w', encoding='utf-8') as fh:
            json.dump(
                {'pdf_files': saved, 'created_at': datetime.now().isoformat()},
                fh,
                ensure_ascii=False,
            )

        # Immediately mark the pipeline as running so the frontend polling
        # doesn't stop on the first request before the background thread updates.
        _pipeline_status = {
            "running": True,
            "step": "Queued",
            "message": "Upload received. Starting parser...",
            "progress": 5,
            "pdfs_found": len(saved),
            "records": 0,
        }

        def process():
            global _pipeline_status
            _pipeline_status = {"running": True, "step": "Parsing PDFs", "message": "Processing uploaded files...", "progress": 40, "pdfs_found": len(saved), "records": 0}
            subprocess.run(['python', '-m', 'src.parser', '--input', upload_dir], capture_output=True, cwd=os.getcwd(), timeout=120)
            _pipeline_status.update({"step": "Merging", "message": "Merging records...", "progress": 70})
            subprocess.run(['python', '-m', 'src.processor.merger'], capture_output=True, cwd=os.getcwd(), timeout=120)
            _pipeline_status = {"running": False, "step": "Complete", "message": "Processing complete!", "progress": 100, "pdfs_found": len(saved), "records": 0}
            _pipeline_status.update({"step": "Deduplicating", "message": "Removing duplicates...", "progress": 85})
            subprocess.run(['python', '-m', 'src.processor.deduplicator'], capture_output=True, cwd=os.getcwd())
            _pipeline_status = {"running": False, "step": "Complete", "message": f"Done! {len(saved)} file(s) processed.", "progress": 100, "pdfs_found": len(saved), "records": 0}
        threading.Thread(target=process, daemon=True).start()
    return jsonify({"saved": saved, "count": len(saved), "processing": True})
