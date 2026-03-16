f = open('src/dashboard/app.py', 'r', encoding='utf-8')
content = f.read()
f.close()

old = "let csv = 'Name,Company,Folio,Shares,Dividend,Contact Status\\\\n';"
new = "let csv = 'Name,Company,Folio,Shares,Dividend,Contact Status';"

if old in content:
    content = content.replace(old, new)
    print('Fixed double-escaped version')
else:
    old2 = "let csv = 'Name,Company,Folio,Shares,Dividend,Contact Status\\n';"
    new2 = "let csv = 'Name,Company,Folio,Shares,Dividend,Contact Status';"
    if old2 in content:
        content = content.replace(old2, new2)
        print('Fixed single-escaped version')
    else:
        print('Neither pattern matched')
        f.close()
        quit()

f = open('src/dashboard/app.py', 'w', encoding='utf-8')
f.write(content)
f.close()