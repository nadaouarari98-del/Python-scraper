import time, subprocess, os, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from src.company_registry import get_companies, update_company_status

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'}
IEPF_KEYWORDS = ['iepf', 'unclaim', 'unpaid', 'dividend', 'shareholder']

_bulk_status = {
    "running": False, "total": 0, "processed": 0,
    "current_company": "", "successful": 0, "failed": 0,
    "skipped": 0, "percent": 0, "message": "Not started"
}

def get_bulk_status() -> dict:
    return _bulk_status

def scrape_pdfs_from_url(url: str) -> list:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '.pdf' in href.lower():
                full_url = urljoin(url, href)
                text = (a.get_text() + ' ' + href).lower()
                score = sum(1 for kw in IEPF_KEYWORDS if kw in text)
                links.append((score, full_url, a.get_text().strip()))
        links.sort(reverse=True)
        return [(u, t) for s, u, t in links]
    except Exception as e:
        print(f'Scrape error {url}: {e}')
        return []

def download_pdf(pdf_url: str, output_dir: str = 'data/input/') -> str:
    os.makedirs(output_dir, exist_ok=True)
    try:
        filename = pdf_url.split('/')[-1].split('?')[0]
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
        filename = filename.replace('%20', '_').replace(' ', '_')[:100]
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath):
            return filepath
        r = requests.get(pdf_url, headers=HEADERS, timeout=30, stream=True)
        r.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        print(f'Downloaded: {filename}')
        time.sleep(1)
        return filepath
    except Exception as e:
        print(f'Download failed: {e}')
        return None

def process_one_company(company_id: int, name: str, nse_symbol: str, bse_code: str = '') -> dict:
    result = {"pdfs_found": 0, "downloaded": 0, "url": None, "status": "no_pdfs"}
    
    urls = []
    if nse_symbol and nse_symbol != 'nan':
        # BSE investor page using NSE symbol
        urls.append(f'https://www.bseindia.com/stock-share-price/{name.lower().replace(" ", "-")}/{nse_symbol}/500209/')
    if bse_code and bse_code != 'nan':
        urls.append(f'https://www.bseindia.com/stock-share-price/x/x/{bse_code}/')
        urls.append(f'https://www.bseindia.com/corporates/Comp_Info.aspx?scripcode={bse_code}')
    
    for url in urls:
        pdf_links = scrape_pdfs_from_url(url)
        if pdf_links:
            result['url'] = url
            result['pdfs_found'] = len(pdf_links)
            downloaded = []
            for pdf_url, text in pdf_links[:5]:
                path = download_pdf(pdf_url)
                if path:
                    downloaded.append(path)
            result['downloaded'] = len(downloaded)
            result['status'] = 'complete' if downloaded else 'no_pdfs'
            break
        time.sleep(1)
    
    return result

def run_bulk_scrape(limit: int = 10, sector: str = None):
    global _bulk_status
    companies = get_companies(status='pending', limit=limit, sector=sector)
    total = len(companies)
    
    if total == 0:
        _bulk_status = {"running": False, "total": 0, "processed": 0,
                       "current_company": "", "successful": 0, "failed": 0,
                       "skipped": 0, "percent": 0,
                       "message": "No pending companies. Load CSV first or all companies are done."}
        return
    
    _bulk_status = {"running": True, "total": total, "processed": 0,
                   "current_company": "", "successful": 0, "failed": 0,
                   "skipped": 0, "percent": 0,
                   "message": f"Starting {total} companies"}
    
    for _, company in companies.iterrows():
        cid = int(company['id'])
        name = str(company['company_name'])
        symbol = str(company.get('nse_symbol', ''))
        bse = str(company.get('bse_code', ''))
        
        _bulk_status['current_company'] = name
        _bulk_status['message'] = f'Processing: {name}'
        print(f'[{_bulk_status["processed"]+1}/{total}] {name}')
        
        try:
            result = process_one_company(cid, name, symbol, bse)
            update_company_status(cid, result['status'], result['pdfs_found'], result['downloaded'], result['url'])
            if result['status'] == 'complete' and result['downloaded'] > 0:
                _bulk_status['successful'] += 1
                subprocess.run(['python', '-m', 'src.parser', '--input', 'data/input/'],
                              capture_output=True, cwd=os.getcwd())
            else:
                _bulk_status['skipped'] += 1
        except Exception as e:
            print(f'Error {name}: {e}')
            update_company_status(cid, 'error')
            _bulk_status['failed'] += 1
        
        _bulk_status['processed'] += 1
        _bulk_status['percent'] = round(_bulk_status['processed'] / total * 100)
        time.sleep(2)
    
    _bulk_status['message'] = 'Finalizing...'
    subprocess.run(['python', '-m', 'src.processor.merger'], capture_output=True, cwd=os.getcwd())
    subprocess.run(['python', '-m', 'src.processor.deduplicator'], capture_output=True, cwd=os.getcwd())
    subprocess.run(['python', '-m', 'src.enrichment.market_price',
                   '--input', 'data/output/master_merged.xlsx'],
                   capture_output=True, cwd=os.getcwd())
    
    _bulk_status['running'] = False
    _bulk_status['message'] = f'Done. {_bulk_status['successful']} successful, {_bulk_status['skipped']} no PDFs, {_bulk_status['failed']} failed.'
    print(_bulk_status['message'])

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--sector', default=None)
    args = parser.parse_args()
    run_bulk_scrape(limit=args.limit, sector=args.sector)
