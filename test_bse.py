import requests

def test_bse():
    session = requests.Session()
    bse_headers = {
        "Referer": "https://www.bseindia.com/",
        "Origin": "https://www.bseindia.com",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    endpoints = [
        f"https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w?Quotetype=Q&scripcode=&segment=0&strType=C&companyname=TCS",
        f"https://www.bseindia.com/api/search/autocomplete?q=TCS"
    ]
    for ep in endpoints:
        print(f"Testing {ep}")
        try:
            r = session.get(ep, headers=bse_headers, timeout=10)
            print(r.status_code)
            if r.status_code == 200:
                print(r.text[:200])
        except Exception as e:
            print("Failed:", e)

test_bse()
