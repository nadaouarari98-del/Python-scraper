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
