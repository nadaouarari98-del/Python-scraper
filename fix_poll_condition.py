with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

old = "if(!d.running && d.progress===0 || d.step==='Complete' || d.step==='Idle'){"
new = "if(!d.running || d.step==='Complete' || d.step==='Idle' || d.progress===100){"

if old in content:
    content = content.replace(old, new)
    print('Fixed polling condition')
else:
    print('Pattern not found')
    idx = content.find('uploadPollInterval')
    if idx != -1:
        print(repr(content[idx:idx+300]))

with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
    f.write(content)