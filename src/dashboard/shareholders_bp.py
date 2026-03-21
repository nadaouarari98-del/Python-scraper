def _load_df():
    # Load the main shareholders DataFrame from the SQLite database
    import sqlite3
    path = os.path.join('data', 'pipeline.db')
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(path)
        df = pd.read_sql_query("SELECT * FROM shareholders", conn)
        conn.close()
        return df.astype(str)
    except Exception as e:
        print(f"Error reading DB: {e}")
        return pd.DataFrame()

def _filter_df(df, search, company, min_wealth):
    # Filter DataFrame by search string, company, and minimum wealth
    if company:
        df = df[df['company_name'].astype(str).str.lower() == company.lower()]
    if search:
        mask = df.apply(lambda row: search.lower() in str(row).lower(), axis=1)
        df = df[mask]
    if min_wealth:
        try:
            df = df[df['total_wealth'].astype(float) >= float(min_wealth)]
        except Exception:
            pass
    return df
from flask import Blueprint, request, jsonify, send_file
import math
import os
import io
import json
import threading
import subprocess
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import pandas as pd

shareholders_bp = Blueprint('shareholders', __name__)

@shareholders_bp.route('/api/get-records')
def api_get_records():
    try:
        df = _load_df()
        # Pagination params
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        total = len(df)
        pages = max(1, (total + per_page - 1) // per_page)
        start = (page - 1) * per_page
        end = start + per_page
        page_df = df.iloc[start:end]
        cols = ['full_name','company_name','folio_no','current_holding','total_dividend','market_value','total_wealth','contact_number']
        available = [c for c in cols if c in page_df.columns]
        records = page_df[available].fillna('').astype(str).to_dict('records')
        companies = sorted(df['company_name'].astype(str).unique().tolist()) if 'company_name' in df.columns else []
        return jsonify({"records": records, "total": total, "page": page, "pages": pages, "companies": companies})
    except Exception as e:
        return jsonify({"error": str(e)})

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
    if _pipeline_status['running']:
        return jsonify({"error": "Pipeline already running. Please wait."})
    data = req.get_json() or {}
    url = data.get('url', '')
    company = data.get('company', '')

    def execute():
        import os, subprocess, json, time, requests
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
                                   "message": "Direct search blocked by NSE/BSE. Please use the Manual PDF Upload for this company.",
                                   "progress": 0, "pdfs_found": 0, "records": 0}
                return
            _pipeline_status.update({"step": "Parsing PDFs",
                                    "message": "Extracting shareholder records...",
                                    "progress": 40})
            subprocess.run(['python', '-m', 'src.parser', '--input', 'data/input/'],
                          capture_output=True, cwd=os.getcwd(), timeout=600)
            _pipeline_status.update({"step": "Merging",
                                    "message": "Merging all records into master dataset...",
                                    "progress": 60})
            subprocess.run(['python', '-m', 'src.processor.merger'],
                          capture_output=True, cwd=os.getcwd(), timeout=300)
            _pipeline_status.update({"step": "Deduplicating",
                                    "message": "Removing duplicate records...",
                                    "progress": 75})
            subprocess.run(['python', '-m', 'src.processor.deduplicator'],
                          capture_output=True, cwd=os.getcwd(), timeout=300)
            _pipeline_status.update({"step": "Market Prices",
                                    "message": "Fetching NSE/BSE market prices...",
                                    "progress": 90})
            subprocess.run(['python', '-m', 'src.enrichment.market_price',
                           '--input', 'data/output/master_merged.xlsx'],
                          capture_output=True, cwd=os.getcwd(), timeout=300)
            _pipeline_status.update({"step": "Syncing DB",
                                    "message": "Syncing processed data to SQLite database...",
                                    "progress": 95})
            subprocess.run(['python', '-m', 'src.processor.sync_to_db'],
                          capture_output=True, cwd=os.getcwd(), timeout=120)
            try:
                import sqlite3
                conn = sqlite3.connect('data/pipeline.db')
                df = pd.read_sql_query("SELECT * FROM shareholders", conn)
                conn.close()
                record_count = len(df)
            except:
                record_count = 0
            _pipeline_status = {"running": False, "step": "Complete",
                               "message": f"Done! {record_count} shareholder records processed.",
                               "progress": 100, "pdfs_found": len(pdfs), "records": record_count}
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print("PIPELINE ERROR:")
            print(tb)
            _pipeline_status = {"running": False, "step": "Error",
                               "message": f"{str(e)}\n\n{tb}", "progress": 0,
                               "pdfs_found": 0, "records": 0}

    threading.Thread(target=execute, daemon=True).start()
    return jsonify({"status": "started"})

@shareholders_bp.route('/api/pipeline/status')
def api_pipeline_status():
    return jsonify(_pipeline_status)

@shareholders_bp.route('/api/upload', methods=['POST'])
def api_upload_pdf():
    from flask import request as req
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
            subprocess.run(['python', '-m', 'src.parser', '--input', upload_dir], capture_output=True, cwd=os.getcwd(), timeout=600)
            _pipeline_status.update({"step": "Merging", "message": "Merging records...", "progress": 70})
            subprocess.run(['python', '-m', 'src.processor.merger'], capture_output=True, cwd=os.getcwd(), timeout=300)
            _pipeline_status.update({"step": "Deduplicating", "message": "Removing duplicates...", "progress": 85})
            subprocess.run(['python', '-m', 'src.processor.deduplicator'], capture_output=True, cwd=os.getcwd())
            _pipeline_status.update({"step": "Syncing DB", "message": "Syncing to Database...", "progress": 95})
            subprocess.run(['python', '-m', 'src.processor.sync_to_db'], capture_output=True, cwd=os.getcwd())
            _pipeline_status = {"running": False, "step": "Complete", "message": f"Done! {len(saved)} file(s) processed.", "progress": 100, "pdfs_found": len(saved), "records": 0}
        threading.Thread(target=process, daemon=True).start()
    return jsonify({"saved": saved, "count": len(saved), "processing": True})
