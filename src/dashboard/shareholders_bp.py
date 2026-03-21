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

@shareholders_bp.route('/api/shareholders')
def api_shareholders():
    try:
        df = _load_df()
        
        search = request.args.get('search', '')
        company = request.args.get('company', '')
        min_wealth = request.args.get('min_wealth', '')
        
        df = _filter_df(df, search, company, min_wealth)
        
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

# Removed legacy manual pipeline functions that shadowed app.py endpoints.
# The dashboard frontend now uses app.py for /api/upload and /api/jobs/submit.

@shareholders_bp.route('/api/pipeline/status')
def api_pipeline_status():
    return jsonify(_pipeline_status)
