with open('src/dashboard/shareholders_bp.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the download call in the execute function
old = """            from src.downloader import download_pdfs as download_company_pdfs
            pdfs = download_company_pdfs(company_name=company, url=url)"""

new = """            from src.downloader import download_pdfs as _dl
            companies_list = [company] if company else []
            if url:
                # For direct URLs use requests scraping fallback
                import requests
                from bs4 import BeautifulSoup
                from urllib.parse import urljoin
                import os, time
                headers = {'User-Agent': 'Mozilla/5.0'}
                pdfs = []
                try:
                    r = requests.get(url, headers=headers, timeout=15)
                    soup = BeautifulSoup(r.text, 'html.parser')
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        if '.pdf' in href.lower():
                            full_url = urljoin(url, href)
                            fname = full_url.split('/')[-1].split('?')[0]
                            if not fname.endswith('.pdf'):
                                fname += '.pdf'
                            fpath = os.path.join('data/input/', fname)
                            if not os.path.exists(fpath):
                                pr = requests.get(full_url, headers=headers, timeout=30, stream=True)
                                if pr.status_code == 200:
                                    with open(fpath, 'wb') as ff:
                                        for chunk in pr.iter_content(8192):
                                            ff.write(chunk)
                                    pdfs.append(fpath)
                                    time.sleep(1)
                except Exception as e:
                    print(f'URL scrape error: {e}')
            elif companies_list:
                result = _dl(companies=companies_list)
                pdfs = []
                for company_name, files in result.items():
                    pdfs.extend(files.keys())
            else:
                pdfs = []"""

if old in content:
    content = content.replace(old, new)
    print('Fixed pipeline download call')
else:
    print('Pattern not found - showing execute function context:')
    idx = content.find('download_company_pdfs')
    if idx != -1:
        print(repr(content[idx-100:idx+200]))
    else:
        idx = content.find('download_pdfs')
        if idx != -1:
            print(repr(content[idx-100:idx+200]))
        else:
            print('No download call found')

with open('src/dashboard/shareholders_bp.py', 'w', encoding='utf-8') as f:
    f.write(content)