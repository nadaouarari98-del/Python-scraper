with open('src/dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_import = 'from flask import'
new_import = 'from src.dashboard.shareholders_bp import shareholders_bp\nfrom flask import'

if 'shareholders_bp' not in content:
    content = content.replace(old_import, new_import, 1)
    print('Import added')
else:
    print('Import already present')

old_create = 'app = Flask(__name__)'
new_create = 'app = Flask(__name__)\n    app.register_blueprint(shareholders_bp)'

if 'register_blueprint' not in content:
    content = content.replace(old_create, new_create, 1)
    print('Blueprint registered')
else:
    print('Blueprint already registered')

with open('src/dashboard/app.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Done')