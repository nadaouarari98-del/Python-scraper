import math
import os
import io
import json
from datetime import datetime
import pandas as pd
from flask import Blueprint, request, jsonify, send_file

shareholders_bp = Blueprint('shareholders', __name__)

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
        try:
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

            # Snapshot existing PDFs before this pipeline run.
            before_pdfs = list_input_pdf_basenames()

            _pipeline_status = {"running": True, "step": "Searching",
                               "message": f"Finding PDFs for {company or url}...",
                               "progress": 5, "pdfs_found": 0, "records": 0}
            from src.downloader import download_pdfs as _dl
            companies_list = [company] if company else []
            if url:
                # For direct URLs use requests scraping fallback
                import requests
                from bs4 import BeautifulSoup
                from urllib.parse import urljoin
                import os, time
                headers = {'User-Agent': 'Mozilla/5.0'}
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
            elif companies_list:
                result = _dl(companies=companies_list)
                pdfs = []
                for company_name, files in result.items():
                    pdfs.extend(files.keys())
            else:
                pdfs = []

            # Write manifest for `only_new` filtering (latest pipeline batch).
            # - If `company` is provided, treat "latest batch" as *all* PDFs for that company slug.
            #   This covers the case where PDFs already exist and downloader skips re-download.
            # - If only `url` is provided, fall back to "new PDFs" via filesystem diff.
            after_pdfs = list_input_pdf_basenames()
            new_pdfs = sorted(after_pdfs - before_pdfs)

            def slugify(name: str) -> str:
                # Must match auto_downloader's filename slug strategy.
                import re
                slug = name.lower().strip()
                slug = re.sub(r"[^a-z0-9]+", "-", slug)
                return slug.strip("-")

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
                                   "message": "No PDFs found at that URL. Try pasting the direct investor page URL.",
                                   "progress": 0, "pdfs_found": 0, "records": 0}
                return
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
        except Exception as e:
            _pipeline_status = {"running": False, "step": "Error",
                               "message": str(e), "progress": 0,
                               "pdfs_found": 0, "records": 0}

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
