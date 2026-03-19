import requests
from bs4 import BeautifulSoup
import os
import time
from urllib.parse import urljoin

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
}

IEPF_KEYWORDS = ['iepf', 'unclaim', 'unpaid', 'dividend', 'shareholder', 'annual']

KNOWN_PAGES = {
    'tech mahindra': 'https://www.techmahindra.com/investors/shareholder-information/',
    'infosys': 'https://www.infosys.com/investors/shareholder-services.html',
    'wipro': 'https://www.wipro.com/investors/',
    'tcs': 'https://www.tcs.com/investor-relations',
    'tata consultancy': 'https://www.tcs.com/investor-relations',
    'reliance': 'https://www.ril.com/investor-relations',
    'reliance industries': 'https://www.ril.com/investor-relations',
    'hdfc bank': 'https://www.hdfcbank.com/personal/useful-links/general/investor-relations',
    'icici bank': 'https://www.icicibank.com/aboutus/investor.page',
    'state bank': 'https://sbi.co.in/web/investor-relations',
    'sbi': 'https://sbi.co.in/web/investor-relations',
    'hindustan unilever': 'https://www.hul.co.in/investor-relations/',
    'hul': 'https://www.hul.co.in/investor-relations/',
    'itc': 'https://www.itcportal.com/investor/',
    'asian paints': 'https://www.asianpaints.com/investor-relations.html',
    'bajaj finance': 'https://www.bajajfinserv.in/investments/bajaj-finance-investor-relations',
    'maruti': 'https://www.marutisuzuki.com/corporate/investors',
    'maruti suzuki': 'https://www.marutisuzuki.com/corporate/investors',
    'larsen': 'https://www.larsentoubro.com/investor-relations/',
    'sun pharma': 'https://sunpharma.com/investors/',
    'sun pharmaceutical': 'https://sunpharma.com/investors/',
    'ntpc': 'https://www.ntpc.co.in/en/investors',
    'ongc': 'https://www.ongcindia.com/ongc/investor-relations',
    'coal india': 'https://www.coalindia.in/investor-relations',
    'power grid': 'https://www.powergridindia.com/investor-relations',
    'bharti airtel': 'https://www.airtel.in/about-bharti/investor-relations',
    'airtel': 'https://www.airtel.in/about-bharti/investor-relations',
    'hcl': 'https://www.hcltech.com/investor-relations',
    'hcl technologies': 'https://www.hcltech.com/investor-relations',
    'axis bank': 'https://www.axisbank.com/shareholder-information',
    'kotak': 'https://www.kotak.com/en/investor-relations.html',
    'bajaj auto': 'https://www.bajajauto.com/investor-relations',
    'titan': 'https://www.titancompany.in/investors',
    'nestle': 'https://www.nestleindia.com/investors',
    'britannia': 'https://www.britannia.co.in/investors.aspx',
    'cipla': 'https://www.cipla.com/investors',
    'dr reddy': 'https://www.drreddys.com/investors',
    'divis': 'https://www.divislaboratories.com/investors',
    'eicher': 'https://www.eichermotors.com/investors',
    'hero motocorp': 'https://www.heromotocorp.com/en-in/investors.html',
    'tata motors': 'https://www.tatamotors.com/investors/',
    'tata steel': 'https://www.tatasteel.com/investors/',
    'hindalco': 'https://www.hindalco.com/investors',
    'jsw steel': 'https://www.jswsteel.in/investors',
    'ultratech': 'https://www.ultratechcement.com/investors',
    'grasim': 'https://www.grasim.com/investors',
    'adani': 'https://www.adanienterprises.com/investors',
}

def find_company_url(company_name: str) -> str:
    name = company_name.lower().strip()
    for key, url in KNOWN_PAGES.items():
        if key in name or name in key:
            return url
    return None


def scrape_pdf_links(url: str) -> list:
    # First try with requests (fast)
    links = _scrape_with_requests(url)
    if links:
        return links
    # Fallback to Playwright for JS-rendered pages
    print(f'No PDFs found with requests, trying Playwright for {url}')
    return _scrape_with_playwright(url)

def _scrape_with_requests(url: str) -> list:
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
        print(f'Requests scrape error {url}: {e}')
        return []

def _scrape_with_playwright(url: str) -> list:
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_extra_http_headers(HEADERS)
            page.goto(url, timeout=30000, wait_until='networkidle')
            content = page.content()
            browser.close()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')
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
        print(f'Playwright scrape error {url}: {e}')
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
            print(f'Already exists: {filename}')
            return filepath
        r = requests.get(pdf_url, headers=HEADERS, timeout=30, stream=True)
        r.raise_for_status()
        content_type = r.headers.get('content-type', '')
        if 'pdf' not in content_type.lower() and 'octet' not in content_type.lower():
            return None
        with open(filepath, 'wb') as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        print(f'Downloaded: {filename}')
        time.sleep(1)
        return filepath
    except Exception as e:
        print(f'Download failed: {e}')
        return None

def download_company_pdfs(company_name: str = None, url: str = None, output_dir: str = 'data/input/') -> list:
    if url:
        investor_url = url
    elif company_name:
        investor_url = find_company_url(company_name)
        if not investor_url:
            print(f'No known URL for {company_name}')
            return []
    else:
        return []
    print(f'Searching: {investor_url}')
    pdf_links = scrape_pdf_links(investor_url)
    print(f'Found {len(pdf_links)} PDF links')
    downloaded = []
    for pdf_url, text in pdf_links[:10]:
        path = download_pdf(pdf_url, output_dir)
        if path:
            downloaded.append(path)
    return downloaded

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', help='Direct investor page URL')
    parser.add_argument('--company', help='Company name')
    parser.add_argument('--output', default='data/input/')
    args = parser.parse_args()
    results = download_company_pdfs(company_name=args.company, url=args.url, output_dir=args.output)
    print(f'Downloaded {len(results)} PDFs')
