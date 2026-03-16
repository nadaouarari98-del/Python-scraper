import os, glob

# Delete ALL db files in the project
for f in glob.glob('**/*.db', recursive=True):
    os.remove(f)
    print('Deleted:', f)

# Now fix the CREATE TABLE in layer3_paid.py
with open('src/enrichment/layer3_paid.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find and show the CREATE TABLE statement
idx = content.find('CREATE TABLE')
if idx == -1:
    print('No CREATE TABLE found in layer3_paid.py')
else:
    print('CREATE TABLE found:')
    print(repr(content[idx:idx+400]))