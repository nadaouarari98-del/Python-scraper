with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the leaked JS - it's outside script tags
idx = content.find('function sortShareholders')
print('sortShareholders found at char:', idx)
print('Context around it:')
print(repr(content[idx-200:idx+50]))
print('---')
# Check if it is inside a script tag
before = content[:idx]
last_script_open = before.rfind('<script>')
last_script_close = before.rfind('</script>')
print('Last <script> before it:', last_script_open)
print('Last </script> before it:', last_script_close)
if last_script_close > last_script_open:
    print('STATUS: sortShareholders is OUTSIDE script tag - LEAKED')
else:
    print('STATUS: sortShareholders is inside script tag - OK')