with open('src/enrichment/layer3_paid.py', 'r', encoding='utf-8') as f:
    content = f.read()

idx = content.find('sqlite3.connect')
while idx != -1:
    print('Found at char', idx, ':', repr(content[idx:idx+80]))
    idx = content.find('sqlite3.connect', idx+1)

idx = content.find('.db')
while idx != -1:
    print('DB path mention:', repr(content[max(0,idx-40):idx+10]))
    idx = content.find('.db', idx+1)