with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Check if sort variables are initialized
if 'window._shSort' in content:
    print('Sort variables exist')
    idx = content.find('window._shSort')
    print('Context:', repr(content[idx-50:idx+100]))
else:
    print('Sort variables missing - adding initialization')

# Check the thead
if 'onclick="sortShareholders' in content:
    print('Sort onclick handlers exist in thead')
else:
    print('Sort onclick handlers MISSING from thead')

# Check the loadShareholders URL building
idx = content.find("'/api/shareholders?")
if idx != -1:
    print('URL building:', repr(content[idx:idx+200]))
else:
    print('URL pattern not found')
    idx = content.find('/api/shareholders')
    if idx != -1:
        print('API URL at:', repr(content[idx:idx+200]))