with open('src/enrichment/orchestrator.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the is_already_processed query to use columns that actually exist
old = "c.execute('SELECT 1 FROM contact_results WHERE shareholder_id=? OR folio_no=? OR sr_no=?', (shareholder_id, shareholder_id, shareholder_id))"
new = "c.execute('SELECT 1 FROM contact_results WHERE name=?', (str(shareholder_id),))"

if old in content:
    content = content.replace(old, new)
    print('Fixed is_already_processed query')
else:
    print('Pattern not found - showing is_already_processed function:')
    idx = content.find('is_already_processed')
    print(repr(content[idx:idx+300]))

with open('src/enrichment/orchestrator.py', 'w', encoding='utf-8') as f:
    f.write(content)