with open('src/enrichment/orchestrator.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix layer1 import
content = content.replace(
    'from src.enrichment.layer1_inhouse import search_inhouse',
    'from src.enrichment.layer1_inhouse.layer1_inhouse import Layer1InhouseSearch as _L1'
)

# Fix layer2 import - check what it's called
with open('src/enrichment/layer2_public.py', 'r', encoding='utf-8') as f:
    l2 = f.read()
print('layer2 has search_public:', 'def search_public' in l2)
print('layer2 has search_public_batch:', 'def search_public_batch' in l2)

# Fix layer3 import
with open('src/enrichment/layer3_paid.py', 'r', encoding='utf-8') as f:
    l3 = f.read()
print('layer3 has search_paid:', 'def search_paid' in l3)

with open('src/enrichment/orchestrator.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Done')