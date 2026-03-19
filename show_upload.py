with open('src/dashboard/shareholders_bp.py', 'r', encoding='utf-8') as f:
    content = f.read()

idx = content.find('def process():')
print(repr(content[idx:idx+500]))