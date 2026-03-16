f = open('src/dashboard/app.py', 'r', encoding='utf-8')
content = f.read()
f.close()

idx = content.find("let csv = 'Name,Company")
print('Found at index:', idx)
print('Context:', repr(content[idx:idx+100]))