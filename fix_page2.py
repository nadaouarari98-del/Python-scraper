with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the exact text to replace
old_text = 'Incoming data from multiple sources ready for processing.'
idx = content.find(old_text)
print('Found at char:', idx)
if idx != -1:
    print('Context:', repr(content[idx-200:idx+100]))