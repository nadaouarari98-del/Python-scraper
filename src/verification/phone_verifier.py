from tenacity import retry, wait_exponential, retry_if_exception_type, stop_after_attempt
import requests
import logging
logger = logging.getLogger("phone_verifier")
logging.basicConfig(level=logging.INFO)

@retry(retry=retry_if_exception_type(requests.exceptions.ConnectionError),
       wait=wait_exponential(multiplier=1, min=2, max=30),
       stop=stop_after_attempt(3), reraise=False)
def verify_numverify(number: str, api_key: str) -> dict:
    try:
        r = requests.get('http://apilayer.net/api/validate',
                        params={'access_key': api_key, 'number': number, 'country_code': 'IN'},
                        timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data.get('valid'):
            return {"valid": False, "carrier": "", "line_type": "", "location": "", "status": "invalid"}
        if data.get('line_type') != 'mobile':
            return {"valid": True, "carrier": data.get('carrier',''), "line_type": data.get('line_type',''), "location": data.get('location',''), "status": "not_mobile"}
        return {"valid": True, "carrier": data.get('carrier',''), "line_type": data.get('line_type',''), "location": data.get('location',''), "status": "verified"}
    except Exception as e:
        logger.warning(f'NumVerify error: {e}')
        return {"valid": False, "carrier": "", "line_type": "", "location": "", "status": "api_error"}

from rapidfuzz import fuzz

def check_truecaller_name(number: str, expected_name: str, api_key: str) -> dict:
    try:
        r = requests.get('https://api4.truecaller.com/v1/search',
                        params={'q': number, 'countryCode': 'IN'},
                        headers={'Authorization': f'Bearer {api_key}'},
                        timeout=10)
        if r.status_code == 404:
            return {"truecaller_name": "", "name_match_score": 0.0, "spam_score": 0, "status": "no_data"}
        r.raise_for_status()
        data = r.json()
        tc_name = data.get('data', [{}])[0].get('name', '') if data.get('data') else ''
        score = fuzz.ratio(tc_name.lower(), expected_name.lower()) / 100.0 if tc_name else 0.0
        spam = data.get('data', [{}])[0].get('spamScore', 0) if data.get('data') else 0
        status = 'name_match' if score >= 0.7 else ('name_mismatch' if tc_name else 'no_data')
        return {"truecaller_name": tc_name, "name_match_score": round(score, 3), "spam_score": spam, "status": status}
    except Exception as e:
        logger.warning(f'Truecaller error: {e}')
        return {"truecaller_name": "", "name_match_score": 0.0, "spam_score": 0, "status": "api_error"}

from datetime import datetime

def build_result(original, normalized, status, carrier="", line_type="", name_match_score=0.0):
    return {
        "original_number": original,
        "normalized_number": normalized or "",
        "verification_status": status,
        "carrier": carrier,
        "line_type": line_type,
        "name_match_score": name_match_score,
        "verified_at": datetime.now().isoformat()
    }

import yaml, os

def _load_config():
    config_path = os.path.join(os.path.dirname(__file__), '../../config/settings.yaml')
    with open(config_path) as f:
        return yaml.safe_load(f).get('verification', {})

def normalize_indian_number(raw: str) -> dict:
    """
    Returns: {
        "normalized": "+91XXXXXXXXXX" or None,
        "status": "valid_format" | "invalid_format" | "not_mobile",
        "reason": str
    }
    """
    # Clean the input
    cleaned = str(raw).strip()
    cleaned = cleaned.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    cleaned = cleaned.replace('+', '').replace('0091', '91').replace('091', '91')
    if cleaned.startswith('0'):
        cleaned = cleaned[1:]
    if not cleaned.startswith('91') and len(cleaned) == 10:
        cleaned = '91' + cleaned

    try:
        parsed = phonenumbers.parse('+' + cleaned, 'IN')
        if not phonenumbers.is_valid_number(parsed):
            return {"normalized": None, "status": "invalid_format", "reason": "fails phonenumbers validation"}
        digits = cleaned[-10:]
        if digits[0] not in '6789':
            return {"normalized": None, "status": "not_mobile", "reason": "does not start with 6/7/8/9"}
        return {"normalized": '+91' + digits, "status": "valid_format", "reason": ""}
    except Exception as e:
        return {"normalized": None, "status": "invalid_format", "reason": str(e)}

def verify_number(number: str, name: str = "") -> dict:
    norm = normalize_indian_number(str(number))
    if norm["status"] != "valid_format":
        return build_result(number, None, norm["status"])
    config = _load_config()
    numverify_key = config.get("numverify_api_key", "")
    truecaller_key = config.get("truecaller_api_key", "")
    if not numverify_key and not truecaller_key:
        return build_result(number, norm["normalized"], "api_not_configured")
    carrier, line_type, name_match_score = "", "", 0.0
    if numverify_key:
        nv = verify_numverify(norm["normalized"], numverify_key)
        if nv["status"] in ("invalid", "not_mobile"):
            return build_result(number, norm["normalized"], nv["status"], nv["carrier"], nv["line_type"])
        carrier = nv["carrier"]
        line_type = nv["line_type"]
    if truecaller_key and name:
        tc = check_truecaller_name(norm["normalized"], name, truecaller_key)
        name_match_score = tc["name_match_score"]
        if tc["status"] == "name_mismatch":
            return build_result(number, norm["normalized"], "name_mismatch", carrier, line_type, name_match_score)
    return build_result(number, norm["normalized"], "verified", carrier, line_type, name_match_score)

import pandas as pd
import sqlite3
from tqdm import tqdm
import json, time

def verify_batch(df: pd.DataFrame) -> pd.DataFrame:
    db_path = os.path.join(os.path.dirname(__file__), '../../data/output/pipeline.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS verification_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT, original_number TEXT, normalized_number TEXT,
        verification_status TEXT, carrier TEXT, line_type TEXT,
        name_match_score REAL, verified_at TEXT
    )''')
    conn.commit()

    mask = df['contact_number'].astype(str).str.strip().ne('')
    to_verify = df[mask].copy()
    logger.info(f'verify_batch: {len(to_verify)} records to verify out of {len(df)}')

    for col in ['verification_status','carrier','line_type','name_match_score','verified_at']:
        if col not in df.columns:
            df[col] = '' if col != 'name_match_score' else 0.0

    found = 0
    for idx, row in tqdm(to_verify.iterrows(), total=len(to_verify), desc='Verifying numbers'):
        result = verify_number(str(row.get('contact_number','')), str(row.get('full_name','')))
        df.at[idx, 'verification_status'] = result['verification_status']
        df.at[idx, 'carrier'] = result['carrier']
        df.at[idx, 'line_type'] = result['line_type']
        df.at[idx, 'name_match_score'] = float(result['name_match_score'])
        df.at[idx, 'verified_at'] = result['verified_at']
        if result['verification_status'] == 'verified':
            found += 1
        c.execute('INSERT INTO verification_results (name, original_number, normalized_number, verification_status, carrier, line_type, name_match_score, verified_at) VALUES (?,?,?,?,?,?,?,?)',
                  (str(row.get('full_name','')), result['original_number'], result['normalized_number'],
                   result['verification_status'], result['carrier'], result['line_type'],
                   result['name_match_score'], result['verified_at']))
        conn.commit()
        time.sleep(0.1)

    conn.close()
    progress = {"total_to_verify": len(to_verify), "verified": found,
                "invalid": int((df['verification_status']=='invalid_format').sum()),
                "failed": int((df['verification_status']=='verification_failed').sum())}
    with open('data/output/progress.json', 'w') as f:
        json.dump(progress, f, indent=2)

    out_path = 'data/output/master_verified.xlsx'
    df.to_excel(out_path, index=False)
    logger.info(f'Saved verified data to {out_path}')
    return df

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()
    df = pd.read_excel(args.input, sheet_name=0, na_filter=False)
    if args.limit:
        df = df.head(args.limit)
    if 'contact_number' not in df.columns:
        df['contact_number'] = ''
    verify_batch(df)
import phonenumbers


