# fetch_companies.py
# Fetches all listed company names from NSE and BSE and saves to companies.json

import json
from nsepython import nse_eq
from bsedata.bse import BSE

companies = set()

# Fetch NSE companies
try:
    nse_data = nse_eq(symbol=None)
    for item in nse_data:
        name = item.get('symbol')
        if name:
            companies.add(name)
except Exception as e:
    print(f"NSE fetch failed: {e}")

# Fetch BSE companies
try:
    b = BSE(update_codes = True)
    bse_data = b.getScripCodes()
    for code, name in bse_data.items():
        if name:
            companies.add(name)
except Exception as e:
    print(f"BSE fetch failed: {e}")

# Save to companies.json
with open('companies.json', 'w', encoding='utf-8') as f:
    json.dump(sorted(list(companies)), f, ensure_ascii=False, indent=2)

print(f"Saved {len(companies)} companies to companies.json")
