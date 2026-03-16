import os

# Delete the specific file
path = 'data/pipeline.db'
if os.path.exists(path):
    os.remove(path)
    print('Deleted:', path)
else:
    print('Not found:', path)

# Fix layer3_paid.py to use data/output/pipeline.db instead
with open('src/enrichment/layer3_paid.py', 'r', encoding='utf-8') as f:
    content = f.read()

old = "../../data/pipeline.db"
new = "../../data/output/pipeline.db"

if old in content:
    content = content.replace(old, new)
    print('Fixed DB path in layer3_paid.py')
else:
    print('Path pattern not found')

with open('src/enrichment/layer3_paid.py', 'w', encoding='utf-8') as f:
    f.write(content)

# Also delete any pipeline.db in data/output just in case
path2 = 'data/output/pipeline.db'
if os.path.exists(path2):
    os.remove(path2)
    print('Deleted:', path2)