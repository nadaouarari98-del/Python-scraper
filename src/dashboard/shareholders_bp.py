import math
import os
import io
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
    try:
        df = _load_df()
    except Exception as e:
        return jsonify({"error": str(e), "records": [], "total": 0, "page": 1, "pages": 1, "companies": []})
    df = _filter_df(df, search, company, min_wealth)
    # Sorting
    valid_cols = ['full_name','company_name','folio_no','current_holding','total_dividend','market_value','total_wealth','contact_number']
    if sort not in valid_cols:
        sort = 'full_name'
    ascending = (order == 'asc')
    if sort in df.columns:
        try:
            numeric_cols = ['current_holding','total_dividend','market_value','total_wealth']
            if sort in numeric_cols:
                df[sort] = pd.to_numeric(df[sort], errors='coerce').fillna(0)
            elif sort == 'full_name':
                # Normalize for alpha sort
                df['_sort_name'] = df['full_name'].astype(str).str.strip().str.lower()
                df = df.sort_values(by='_sort_name', ascending=ascending, kind='mergesort')
                df = df.drop(columns=['_sort_name'])
            else:
                df = df.sort_values(by=sort, ascending=ascending, kind='mergesort')
        except Exception:
            pass
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
    os.makedirs('data/input/', exist_ok=True)
    for f in files:
        if f.filename.lower().endswith('.pdf'):
            safe_name = f.filename.replace(' ', '_')
            path = os.path.join('data/input/', safe_name)
            f.save(path)
            saved.append(f.filename)
            print(f'Uploaded: {f.filename}')
    if saved:
        def process():
            subprocess.run(['python', '-m', 'src.parser', '--input', 'data/input/'],
                          capture_output=True, cwd=os.getcwd())
            subprocess.run(['python', '-m', 'src.processor.merger'],
                          capture_output=True, cwd=os.getcwd())
            subprocess.run(['python', '-m', 'src.processor.deduplicator'],
                          capture_output=True, cwd=os.getcwd())
        threading.Thread(target=process, daemon=True).start()
    return jsonify({"saved": saved, "count": len(saved), "processing": True})
