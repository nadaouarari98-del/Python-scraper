with open('src/enrichment/layer3_paid.py', 'r', encoding='utf-8') as f:
    content = f.read()

old = """mask = (df.get('contact_number', pd.Series([None]*len(df))).isnull() | (df['contact_number'] == '')) & \\"""

new = """if 'contact_number' not in df.columns:
        df['contact_number'] = ''
    if 'email_id' not in df.columns:
        df['email_id'] = ''
    if 'email' not in df.columns:
        df['email'] = ''
    mask = (df['contact_number'].isna() | (df['contact_number'] == '')) & \\"""

if old in content:
    content = content.replace(old, new)
    print('Fixed')
else:
    print('Pattern not found - showing mask line:')
    idx = content.find('mask =')
    print(repr(content[idx:idx+200]))

with open('src/enrichment/layer3_paid.py', 'w', encoding='utf-8') as f:
    f.write(content)