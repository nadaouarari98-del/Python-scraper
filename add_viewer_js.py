with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

viewer_html = """
<div style="padding:0">
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px" id="p2stats">
    <div style="background:#f8fafc;border-radius:8px;padding:16px;text-align:center"><div style="font-size:13px;color:#6b7280">Total Records</div><div style="font-size:24px;font-weight:600" id="p2total">-</div></div>
    <div style="background:#f8fafc;border-radius:8px;padding:16px;text-align:center"><div style="font-size:13px;color:#6b7280">Companies</div><div style="font-size:24px;font-weight:600" id="p2cos">-</div></div>
    <div style="background:#f8fafc;border-radius:8px;padding:16px;text-align:center"><div style="font-size:13px;color:#6b7280">With Contact</div><div style="font-size:24px;font-weight:600" id="p2contacts">-</div></div>
    <div style="background:#f8fafc;border-radius:8px;padding:16px;text-align:center"><div style="font-size:13px;color:#6b7280">High Value</div><div style="font-size:24px;font-weight:600" id="p2hv">-</div></div>
  </div>
  <div style="display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap;align-items:center">
    <input id="srchBox" placeholder="Search name, folio, company..." style="flex:1;min-width:200px;padding:8px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px">
    <select id="coFilter" style="padding:8px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px"><option value="">All Companies</option></select>
    <input id="wealthFilter" type="number" placeholder="Min wealth" style="width:140px;padding:8px 12px;border:1px solid #e2e8f0;border-radius:6px;font-size:14px">
    <button onclick="loadShareholders(1)" style="padding:8px 16px;background:#3b82f6;color:white;border:none;border-radius:6px;cursor:pointer">Filter</button>
    <button onclick="resetFilters()" style="padding:8px 16px;background:#6b7280;color:white;border:none;border-radius:6px;cursor:pointer">Reset</button>
    <a id="dlBtn" href="/api/shareholders/download" style="padding:8px 16px;background:#10b981;color:white;border:none;border-radius:6px;cursor:pointer;text-decoration:none">Download Excel</a>
  </div>
  <div style="overflow-x:auto;border:1px solid #e2e8f0;border-radius:8px">
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="background:#f8fafc;border-bottom:2px solid #e2e8f0">
        <th style="padding:10px 12px;text-align:left;font-weight:600">Name</th>
        <th style="padding:10px 12px;text-align:left;font-weight:600">Company</th>
        <th style="padding:10px 12px;text-align:left;font-weight:600">Folio No</th>
        <th style="padding:10px 12px;text-align:right;font-weight:600">Shares</th>
        <th style="padding:10px 12px;text-align:right;font-weight:600">Dividend</th>
        <th style="padding:10px 12px;text-align:right;font-weight:600">Market Value</th>
        <th style="padding:10px 12px;text-align:right;font-weight:600">Total Wealth</th>
        <th style="padding:10px 12px;text-align:center;font-weight:600">Contact</th>
      </tr></thead>
      <tbody id="shBody"><tr><td colspan="8" style="text-align:center;padding:40px;color:#9ca3af">Loading...</td></tr></tbody>
    </table>
  </div>
  <div style="display:flex;justify-content:space-between;align-items:center;margin-top:12px;font-size:13px;color:#6b7280" id="pgInfo"></div>
</div>
"""

js_code = """
var shPage = 1;
function loadShareholders(page) {
  shPage = page || shPage;
  var search = (document.getElementById('srchBox') || {}).value || '';
  var company = (document.getElementById('coFilter') || {}).value || '';
  var minWealth = (document.getElementById('wealthFilter') || {}).value || 0;
  var url = '/api/shareholders?search=' + encodeURIComponent(search) + '&company=' + encodeURIComponent(company) + '&min_wealth=' + minWealth + '&page=' + shPage + '&per_page=50';
  fetch(url).then(function(r){return r.json();}).then(function(data){
    var tbody = document.getElementById('shBody');
    if (!tbody) return;
    if (!data.records || data.records.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:40px;color:#9ca3af">No records found</td></tr>';
    } else {
      tbody.innerHTML = data.records.map(function(r){
        var contact = r.contact_number ? '<span style="color:#10b981;font-weight:500">Yes</span>' : '<span style="color:#d1d5db">No</span>';
        return '<tr style="border-bottom:1px solid #f1f5f9"><td style="padding:8px 12px">' + (r.full_name||'') + '</td><td style="padding:8px 12px;color:#6b7280">' + (r.company_name||'') + '</td><td style="padding:8px 12px;font-family:monospace;font-size:12px">' + (r.folio_no||'') + '</td><td style="padding:8px 12px;text-align:right">' + (r.current_holding||'') + '</td><td style="padding:8px 12px;text-align:right">' + (r.total_dividend||'') + '</td><td style="padding:8px 12px;text-align:right">' + (r.market_value||'') + '</td><td style="padding:8px 12px;text-align:right;font-weight:500">' + (r.total_wealth||'') + '</td><td style="padding:8px 12px;text-align:center">' + contact + '</td></tr>';
      }).join('');
    }
    var info = document.getElementById('pgInfo');
    if (info) {
      var start = (shPage-1)*50+1;
      var end = Math.min(shPage*50, data.total);
      var prevBtn = shPage > 1 ? '<button onclick="loadShareholders(' + (shPage-1) + ')" style="padding:4px 12px;border:1px solid #e2e8f0;border-radius:4px;cursor:pointer;background:white">Previous</button>' : '';
      var nextBtn = shPage < data.pages ? '<button onclick="loadShareholders(' + (shPage+1) + ')" style="padding:4px 12px;border:1px solid #e2e8f0;border-radius:4px;cursor:pointer;background:white">Next</button>' : '';
      info.innerHTML = '<span>Showing ' + start + ' to ' + end + ' of ' + data.total + ' records</span><div style="display:flex;gap:8px;align-items:center">' + prevBtn + '<span>Page ' + shPage + ' of ' + data.pages + '</span>' + nextBtn + '</div>';
    }
    var p2total = document.getElementById('p2total');
    if (p2total) p2total.textContent = data.total;
    var sel = document.getElementById('coFilter');
    if (sel && sel.options.length <= 1 && data.companies) {
      data.companies.forEach(function(c){ var o = document.createElement('option'); o.value=c; o.text=c; sel.appendChild(o); });
    }
    var cos = document.getElementById('p2cos');
    if (cos && data.companies) cos.textContent = data.companies.length;
    var dlBtn = document.getElementById('dlBtn');
    if (dlBtn) dlBtn.href = '/api/shareholders/download?search=' + encodeURIComponent(search) + '&company=' + encodeURIComponent(company) + '&min_wealth=' + minWealth;
  });
}
function resetFilters() {
  var s = document.getElementById('srchBox'); if(s) s.value='';
  var c = document.getElementById('coFilter'); if(c) c.value='';
  var w = document.getElementById('wealthFilter'); if(w) w.value='';
  loadShareholders(1);
}
"""

# Find page 2 content area and replace it
# Look for the Data Collection page content
import re

# Find where page 2 content is
p2_markers = [
    'id="page2"',
    "id='page2'",
    'Data Collection</h',
    'Incoming data from multiple sources'
]

found = False
for marker in p2_markers:
    if marker in content:
        print('Found page 2 marker:', repr(marker))
        found = True
        break

if not found:
    print('Page 2 marker not found - showing page content structure:')
    for i, marker in enumerate(['page1', 'page2', 'page3', 'mainContent']):
        idx = content.find(marker)
        if idx != -1:
            print(marker, 'at char', idx, ':', repr(content[idx:idx+100]))

# Add JS to file
if 'function loadShareholders' not in content:
    # Find a good place to insert - before the closing script tag
    close_script = '</script>'
    last_script = content.rfind(close_script)
    if last_script != -1:
        content = content[:last_script] + js_code + content[last_script:]
        print('JS added')
    else:
        print('No closing script tag found')
else:
    print('JS already present')

with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Done')