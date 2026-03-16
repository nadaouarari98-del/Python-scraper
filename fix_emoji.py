with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('\u20b9', 'Rs.')

with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Done')