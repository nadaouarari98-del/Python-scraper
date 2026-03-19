with open('src/dashboard/shareholders_bp.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the api_shareholders function and add actual sorting
old = "        records = page_df[available].fillna('').astype(str).to_dict('records')"

new = """        # Apply sort before pagination
        sort_col = sort if sort in df.columns else None
        if not sort_col:
            # Try common column name variations
            alt_names = {
                'full_name': ['full_name', 'name', 'Full Name', 'NAME'],
                'company_name': ['company_name', 'company', 'Company'],
                'current_holding': ['current_holding', 'shares', 'Current Holding'],
                'total_dividend': ['total_dividend', 'dividend', 'Total Dividend'],
                'total_wealth': ['total_wealth', 'Total Wealth'],
                'market_value': ['market_value', 'Market Value'],
            }
            for alt in alt_names.get(sort, []):
                if alt in df.columns:
                    sort_col = alt
                    break
        if sort_col and sort_col in df.columns:
            try:
                df['_sort_num'] = pd.to_numeric(df[sort_col], errors='coerce')
                if df['_sort_num'].notna().sum() > len(df) * 0.5:
                    df = df.sort_values('_sort_num', ascending=(order=='asc'))
                else:
                    df = df.sort_values(sort_col, ascending=(order=='asc'), key=lambda x: x.str.lower() if x.dtype == object else x)
                df = df.drop('_sort_num', axis=1, errors='ignore')
            except Exception as e:
                print(f'Sort error: {e}')
        
        total = len(df)
        pages = max(1, math.ceil(total / per_page))
        start = (page - 1) * per_page
        page_df = df.iloc[start:start+per_page]
        records = page_df[available].fillna('').astype(str).to_dict('records')"""

if old in content:
    # First remove the old total/pages/start/page_df lines that come after
    old_with_pagination = """        records = page_df[available].fillna('').astype(str).to_dict('records')"""
    content = content.replace(old, new)
    print('Sort logic added to API')
else:
    print('Pattern not found')
    idx = content.find('page_df[available]')
    if idx != -1:
        print(repr(content[idx-200:idx+100]))

with open('src/dashboard/shareholders_bp.py', 'w', encoding='utf-8') as f:
    f.write(content)