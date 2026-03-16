with open('src/enrichment/layer2_public.py', 'r', encoding='utf-8') as f:
    content = f.read()

old = "mask = df['contact_number'].isna() & df['email'].isna()"

new = """if 'contact_number' not in df.columns:
        df['contact_number'] = ''
    if 'email_id' not in df.columns:
        df['email_id'] = ''
    if 'email' not in df.columns:
        df['email'] = ''
    mask = (df['contact_number'].isna() | (df['contact_number'] == '')) & \
           (df['email_id'].isna() | (df['email_id'] == ''))"""

if old in content:
    content = content.replace(old, new)
    print('Fixed')
else:
    print('Pattern not found')
    idx = content.find('contact_number')
    print(repr(content[idx-20:idx+80]))

with open('src/enrichment/layer2_public.py', 'w', encoding='utf-8') as f:
    f.write(content)