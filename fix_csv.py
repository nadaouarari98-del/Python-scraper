f = open('src/dashboard/app.py', 'r', encoding='utf-8')
content = f.read()
f.close()

bad = "let csv = 'Name,Company,Folio,Shares,Dividend,Contact Status\n';"
good = "let csv = 'Name,Company,Folio,Shares,Dividend,Contact Status\\n';"

if bad in content:
    content = content.replace(bad, good)
    print('Fixed')
else:
    print('Pattern not found - searching for partial match')
    idx = content.find('let csv =')
    print(repr(content[idx:idx+80]))

f = open('src/dashboard/app.py', 'w', encoding='utf-8')
f.write(content)
f.close()