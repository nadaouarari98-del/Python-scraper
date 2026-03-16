import os
import sys
import argparse
import pandas as pd
import yaml
import sqlite3
from tqdm import tqdm
from typing import Optional
from src.enrichment.apis.apollo import ApolloClient
from src.enrichment.apis.zoominfo import ZoomInfoClient
from src.enrichment.apis.mock_client import MockClient
from src.enrichment.apis.base_api import APIResult

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../../config/settings.yaml')
DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/output/pipeline.db')
PROGRESS_PATH = os.path.join(os.path.dirname(__file__), '../../data/output/progress.json')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '../../data/output/layer3_enriched.xlsx')

COSTS = {"apollo": 0.025, "zoominfo": 0.04}

def load_config():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def write_progress(stats: dict):
    import json
    with open(PROGRESS_PATH, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)

def log_sqlite(result: APIResult, record: dict, source_layer=3):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS contact_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT, company_name TEXT, contact_number TEXT, email TEXT, source TEXT, confidence REAL, credits_used INTEGER, source_layer INTEGER, raw_response TEXT
    )''')
    c.execute('''INSERT INTO contact_results (name, company_name, contact_number, email, source, confidence, credits_used, source_layer, raw_response)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
        record.get('full_name'), record.get('company_name'), result.contact_number, result.email, result.source, result.confidence, result.credits_used, source_layer, str(result.raw_response)))
    conn.commit()
    conn.close()

def search_paid(record, mock=False) -> Optional[APIResult]:
    config = load_config()
    apollo_key = config.get('layer3_apis', {}).get('apollo', {}).get('api_key', '')
    zoominfo_key = config.get('layer3_apis', {}).get('zoominfo', {}).get('api_key', '')
    if mock:
        client = MockClient()
        result = client.search(record)
        log_sqlite(result, record)
        return result
    if mock:
        return MockClient(api_key='').search(record)
    if not apollo_key and not zoominfo_key:
        print("Layer 3 skipped: no API keys configured")
        return None
    apollo = ApolloClient(apollo_key)
    zoominfo = ZoomInfoClient(zoominfo_key)
    result = apollo.search(record) if apollo.is_configured() else None
    if not result and zoominfo.is_configured():
        result = zoominfo.search(record)
    if result:
        log_sqlite(result, record)
    return result

def search_paid_batch(df, mock=False, limit=None):
    if 'contact_number' not in df.columns:
        df['contact_number'] = ''
    if 'email_id' not in df.columns:
        df['email_id'] = ''
    if 'email' not in df.columns:
        df['email'] = ''
    mask = (df['contact_number'].isna() | (df['contact_number'] == '')) & \
           (df.get('email_id', pd.Series([None]*len(df))).isnull() | (df['email_id'] == ''))
    to_search = df[mask]
    if limit:
        to_search = to_search.head(limit)
    found = 0
    calls = {"apollo": 0, "zoominfo": 0, "mock": 0}
    results = []
    for _, row in tqdm(to_search.iterrows(), total=len(to_search), desc="Layer 3 Paid Search"):
        rec = row.to_dict()
        result = search_paid(rec, mock=mock)
        if result and (result.contact_number or result.email):
            found += 1
        if result:
            calls[result.source] = calls.get(result.source, 0) + 1
        results.append(result)
    hit_rate = found / len(to_search) if len(to_search) else 0
    cost = sum(calls.get(k, 0) * COSTS.get(k, 0) for k in calls)
    print(f"Total searched: {len(to_search)} | Found: {found} | Hit rate: {hit_rate:.1%}")
    print(f"API calls: {calls} | Estimated cost: ${cost:.2f}{' (mock)' if mock else ''}")
    stats = {"total": len(to_search), "found": found, "hit_rate": hit_rate, "calls": calls, "cost_estimate": cost}
    write_progress(stats)
    # Save output
    out_df = df.copy()
    for idx, row in to_search.iterrows():
        result = results.pop(0)
        if result:
            out_df.at[idx, 'contact_number'] = result.contact_number
            out_df.at[idx, 'email_id'] = result.email
    out_df.to_excel(OUTPUT_PATH, index=False)
    print(f"Output saved to {OUTPUT_PATH}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--mock', action='store_true')
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()
    df = pd.read_excel(args.input)
    search_paid_batch(df, mock=args.mock, limit=args.limit)

if __name__ == "__main__":
    main()
