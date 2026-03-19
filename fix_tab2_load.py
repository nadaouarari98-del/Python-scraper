with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

if 'function loadShareholders' in content:
    print('loadShareholders: EXISTS')
else:
    print('loadShareholders: MISSING')

if 'function sortShareholders' in content:
    print('sortShareholders: EXISTS')
else:
    print('sortShareholders: MISSING')

idx = content.find('loadShareholders(1)')
if idx != -1:
    print('loadShareholders(1) called at char:', idx)
    print('Context:', repr(content[max(0,idx-100):idx+50]))
else:
    print('loadShareholders(1) never called on page load')

if 'parseInt(page) === 2' in content:
    idx2 = content.find('parseInt(page) === 2')
    print('Tab 2 handler:', repr(content[idx2:idx2+150]))
elif 'page == 2' in content or "page === '2'" in content:
    print('Tab 2 handler found with different syntax')
else:
    print('No tab 2 handler found')