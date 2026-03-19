with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

old = "          document.getElementById('pageTitle').textContent = titles[pageNum - 1];\n        }"

new = "          document.getElementById('pageTitle').textContent = titles[pageNum - 1];\n          if (pageNum === 2) { setTimeout(function(){ if(typeof loadShareholders==='function') loadShareholders(1); }, 100); }\n          if (pageNum === 1) { setTimeout(function(){ if(typeof updateDashboard==='function') updateDashboard(); }, 100); }\n        }"

if old in content:
    content = content.replace(old, new)
    print('Fixed')
else:
    print('Pattern not found')
    idx = content.find('pageTitle')
    print(repr(content[idx:idx+100]))

with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
    f.write(content)