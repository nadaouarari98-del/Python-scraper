with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

old = '''<h1>Data Collection</h1>
              <p style="color: #6b7280; margin: 20px 0;">Incoming data from multiple sources ready for processing.</p>
              <div class="sources-grid'''

new = '''<h1>Data Collection</h1>
              <div style="padding:0" id="shareholderViewer">
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px">
    <div style="background:#f8fafc;border-radius:8px;padding:16px;text-align:center"><div style="font-size:13px;color:#6b7280">Total Records</div><div style="font-size:24px;font-weight:600" id="p2total">-</div></div>
    <div style="background:#f8fafc;border-radius:8px;padding:16px;text-align:center"><div style="font-size:13px;color:#6b7280">Companies</div><div style="font-size:24px;font-weight:600" id="p2cos">-</div></div>
    <div style="background:#f8fafc;border-radius:8px;padding:16px;text-align:center"><div style="font-size:13px;color:#6b7280">Pages</div><div style="font-size:24px;font-weight:600" id="p2pages">-</div></div>
    <div style="background:#f8fafc;border-radius:8px;padding:16px;text-align:center"><div style="font-size:13px;color:#6b7280">Per Page</div><div style="font-size:24px;font-weight:600">50</div></div>
  </div>
  <div style="display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap;align-items:center">
    <input id="srchBox" placeholder="Search name, folio, company..." style="flex:1;min-width:200px;padding:8px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px">
    <select id="coFilter" style="padding:8px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px"><option value="">All Companies</option></select>
    <input id="wealthFilter" type="number" placeholder="Min wealth" style="width:140px;padding:8px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px">
    <button onclick="loadShareholders(1)" style="padding:8px 16px;background:#3b82f6;color:white;border:none;border-radius:6px;cursor:pointer">Filter</button>
    <button onclick="resetFilters()" style="padding:8px 16px;background:#6b7280;color:white;border:none;border-radius:6px;cursor:pointer">Reset</button>
    <a id="dlBtn" href="/api/shareholders/download" style="padding:8px 16px;background:#10b981;color:white;border-radius:6px;cursor:pointer;text-decoration:none;display:inline-block">Download Excel</a>
  </div>
  <div style="overflow-x:auto;border:1px solid #e2e8f0;border-radius:8px">
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0">
        <th style="padding:10px 12px;text-align:left">Name</th>
        <th style="padding:10px 12px;text-align:left">Company</th>
        <th style="padding:10px 12px;text-align:left">Folio No</th>
        <th style="padding:10px 12px;text-align:right">Shares</th>
        <th style="padding:10px 12px;text-align:right">Dividend</th>
        <th style="padding:10px 12px;text-align:right">Market Value</th>
        <th style="padding:10px 12px;text-align:right">Total Wealth</th>
        <th style="padding:10px 12px;text-align:center">Contact</th>
      </tr></thead>
      <tbody id="shBody"><tr><td colspan="8" style="text-align:center;padding:40px;color:#9ca3af">Loading...</td></tr></tbody>
    </table>
  </div>
  <div style="display:flex;justify-content:space-between;align-items:center;margin-top:12px;font-size:13px;color:#6b7280" id="pgInfo"></div>
</div>
              <div class="sources-grid'''

if old in content:
    content = content.replace(old, new)
    print('Fixed')
else:
    print('Pattern not found')
    idx = content.find('Data Collection</h1>')
    print('Context:', repr(content[idx:idx+300]))

with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
    f.write(content)