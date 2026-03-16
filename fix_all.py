import os, glob

for f in glob.glob('**/*.db', recursive=True):
    os.remove(f)
    print('Deleted:', f)

with open('src/enrichment/layer3_paid.py', 'r', encoding='utf-8') as f:
    content = f.read()

idx = content.find('CREATE TABLE IF NOT EXISTS contact_results')
end = content.find("''')", idx)
print('Schema:', content[idx:end+4])