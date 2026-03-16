with open('src/enrichment/layer3_paid.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix 1: SQLite column name - change 'company' to 'company_name' in INSERT
content = content.replace(
    'INSERT INTO contact_results (name, company,',
    'INSERT INTO contact_results (name, company_name,'
)

# Fix 2: Also fix the CREATE TABLE if it uses 'company' instead of 'company_name'
content = content.replace(
    'company TEXT,',
    'company_name TEXT,'
)

# Fix 3: mock flag not being passed - find where search_paid checks for mock
# The mock check must happen BEFORE the API key check
old = "if not apollo_key and not zoominfo_key:"
new = "if mock:\n        return MockClient(api_key='').search(record)\n    if not apollo_key and not zoominfo_key:"

if old in content:
    content = content.replace(old, new)
    print('Mock fix applied')
else:
    print('Mock pattern not found - showing search_paid function:')
    idx = content.find('def search_paid')
    print(repr(content[idx:idx+400]))

with open('src/enrichment/layer3_paid.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Done')