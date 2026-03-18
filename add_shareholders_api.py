with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the exact text just before the first @app.route
# We'll insert our new endpoint right before the first route
insert_marker = "@app.route('/')"
if insert_marker not in content:
    # try alternative
    insert_marker = "@app.route(\"/\")"

if insert_marker not in content:
    print('Cannot find insertion point - showing first route:')
    idx = content.find('@app.route')
    print(repr(content[idx:idx+50]))
else:
    new_endpoint = '''@app.route('/api/shareholders')
def api_shareholders():
    import math, os
    search = request.args.get('search', '').lower()
    company = request.args.get('company', '')
    min_wealth = request.args.get('min_wealth', 0, type=float)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    try:
        prices_file = 'data/output/master_merged_with_prices.xlsx'
        merged_file = 'data/output/master_merged.xlsx'
        filepath = prices_file if os.path.exists(prices_file) else merged_file
        df = pd.read_excel(filepath, sheet_name=0, na_filter=False)
    except Exception as e:
        return jsonify({"error": str(e), "records": [], "total": 0, "page": 1, "pages": 1, "companies": []})
    if search:
        mask = (df.get('full_name', pd.Series(dtype=str)).astype(str).str.lower().str.contains(search, na=False) |
                df.get('folio_no', pd.Series(dtype=str)).astype(str).str.lower().str.contains(search, na=False) |
                df.get('company_name', pd.Series(dtype=str)).astype(str).str.lower().str.contains(search, na=False))
        df = df[mask]
    if company:
        df = df[df.get('company_name', pd.Series(dtype=str)).astype(str) == company]
    if min_wealth > 0 and 'total_wealth' in df.columns:
        df = df[pd.to_numeric(df['total_wealth'], errors='coerce').fillna(0) >= min_wealth]
    total = len(df)
    pages = max(1, math.ceil(total / per_page))
    start = (page - 1) * per_page
    page_df = df.iloc[start:start+per_page]
    cols = ['full_name','company_name','folio_no','current_holding','total_dividend','market_value','total_wealth','contact_number']
    available = [c for c in cols if c in page_df.columns]
    records = page_df[available].fillna('').astype(str).to_dict('records')
    companies = sorted(df['company_name'].astype(str).unique().tolist()) if 'company_name' in df.columns else []
    return jsonify({"records": records, "total": total, "page": page, "pages": pages, "companies": companies})


@app.route('/api/shareholders/download')
def api_shareholders_download():
    import os, io
    search = request.args.get('search', '').lower()
    company = request.args.get('company', '')
    min_wealth = request.args.get('min_wealth', 0, type=float)
    try:
        prices_file = 'data/output/master_merged_with_prices.xlsx'
        merged_file = 'data/output/master_merged.xlsx'
        filepath = prices_file if os.path.exists(prices_file) else merged_file
        df = pd.read_excel(filepath, sheet_name=0, na_filter=False)
        if search:
            mask = (df.get('full_name', pd.Series(dtype=str)).astype(str).str.lower().str.contains(search, na=False) |
                    df.get('folio_no', pd.Series(dtype=str)).astype(str).str.lower().str.contains(search, na=False) |
                    df.get('company_name', pd.Series(dtype=str)).astype(str).str.lower().str.contains(search, na=False))
            df = df[mask]
        if company:
            df = df[df.get('company_name', pd.Series(dtype=str)).astype(str) == company]
        if min_wealth > 0 and 'total_wealth' in df.columns:
            df = df[pd.to_numeric(df['total_wealth'], errors='coerce').fillna(0) >= min_wealth]
        output = io.BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        from flask import send_file
        return send_file(output, download_name='shareholders_filtered.xlsx', as_attachment=True,
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return jsonify({"error": str(e)})


'''
    content = content.replace(insert_marker, new_endpoint + insert_marker)
    with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print('Done - endpoints added')
    count = content.count("@app.route('/api/shareholders')")
    print('shareholders route count:', count)