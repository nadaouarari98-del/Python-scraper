with open('src/dashboard/shareholders_bp.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the import to use the existing downloader package
old = 'from src.downloader import download_company_pdfs'
new = 'from src.downloader import download_pdfs as download_company_pdfs'

if old in content:
    content = content.replace(old, new)
    print('Fixed import')
else:
    print('Pattern not found - searching for downloader import')
    idx = content.find('downloader')
    if idx != -1:
        print('Found at:', repr(content[idx-20:idx+80]))
    else:
        print('No downloader import found - checking execute function')
        idx = content.find('download_company_pdfs')
        if idx != -1:
            print('Found usage at:', repr(content[idx-50:idx+100]))

with open('src/dashboard/shareholders_bp.py', 'w', encoding='utf-8') as f:
    f.write(content)