import os
import sys
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, wait_exponential, retry_if_exception_type
import requests
import sqlite3
import argparse

import logging

# Robust imports for all layers
try:
    pass  # Layer 1 handled dynamically below
except ImportError:
    raise ImportError("Layer 1 module missing: src/enrichment/layer1_inhouse.py")
try:
    from src.enrichment.layer2_public import search_public
except ImportError:
    raise ImportError("Layer 2 module missing: src/enrichment/layer2_public.py")
try:
    from src.enrichment.layer3_paid import search_paid
except ImportError:
    raise ImportError("Layer 3 module missing: src/enrichment/layer3_paid.py")

logger = logging.getLogger("orchestrator")
logging.basicConfig(level=logging.INFO)

def _search_layer1(record: dict):
    try:
        from src.enrichment.layer1_inhouse.layer1_inhouse import Layer1InhouseSearch
        import pandas as pd
        searcher = Layer1InhouseSearch()
        df_single = pd.DataFrame([record])
        if hasattr(searcher, 'search_batch'):
            result_df = searcher.search_batch(df_single)
        elif hasattr(searcher, 'search_inhouse_batch'):
            result_df = searcher.search_inhouse_batch(df_single)
        else:
            return None
        if result_df is not None and len(result_df) > 0:
            row = result_df.iloc[0]
            cn = row.get('contact_number', '')
            em = row.get('email_id', '') or row.get('email', '')
            if cn or em:
                return {'contact_number': cn, 'email': em, 'source': 'inhouse', 'confidence': 0.9}
        return None
    except Exception as e:
        logger.warning(f'Layer 1 search failed: {e}')
        return None

CHECKPOINT_PATH = os.path.join(os.path.dirname(__file__), '../../data/output/enrichment_checkpoint.json')
PROGRESS_PATH = os.path.join(os.path.dirname(__file__), '../../data/output/progress.json')
REPORT_PATH = os.path.join(os.path.dirname(__file__), '../../data/output/enrichment_report.json')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '../../data/output/master_enriched.xlsx')

start_time = time.time()

def is_already_processed(shareholder_id):
    if not shareholder_id:
        return False
    conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), '../../data/pipeline.db'))
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS contact_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT, company TEXT, contact_number TEXT, email TEXT, source TEXT, confidence REAL, credits_used INTEGER, source_layer INTEGER, raw_response TEXT, shareholder_id TEXT, folio_no TEXT, sr_no TEXT, contact_layer INTEGER
    )''')
    c.execute('SELECT 1 FROM contact_results WHERE name=?', (str(shareholder_id),))
    found = c.fetchone() is not None
    conn.close()
    return found

def save_and_return(record, result, layer):
    record = record.copy()
    if result:
        record['contact_number'] = result.contact_number
        record['email_id'] = result.email
        record['contact_layer'] = layer
    else:
        record['contact_layer'] = 0
    return record

def save_checkpoint(results):
    ids = [r.get('shareholder_id') or r.get('folio_no') or str(r.get('sr_no', '')) for r in results]
    with open(CHECKPOINT_PATH, 'w', encoding='utf-8') as f:
        json.dump(ids, f)

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_PATH):
        return set()
    with open(CHECKPOINT_PATH, 'r', encoding='utf-8') as f:
        return set(json.load(f))

def update_progress(processed, total, results):
    layer_counts = {1: 0, 2: 0, 3: 0, 0: 0}
    for r in results:
        layer_counts[r.get('contact_layer', 0)] += 1
    elapsed = time.time() - start_time
    speed = processed / (elapsed / 3600) if elapsed > 0 else 0
    remaining = (total - processed) / speed if speed > 0 else 0
    eta = datetime.now() + timedelta(hours=remaining)
    progress = {
        "total_records": total,
        "processed": processed,
        "layer1_found": layer_counts[1],
        "layer2_found": layer_counts[2],
        "layer3_found": layer_counts[3],
        "no_contact": layer_counts[0],
        "in_progress": processed < total,
        "estimated_completion": eta.strftime("%Y-%m-%d %H:%M:%S"),
        "current_speed_per_hour": round(speed)
    }
    with open(PROGRESS_PATH, 'w') as f:
        json.dump(progress, f, indent=2)

@retry(
    retry=retry_if_exception_type(requests.exceptions.ConnectionError),
    wait=wait_exponential(multiplier=1, min=30, max=300),
    reraise=False
)
def enrich_record_with_retry(record, mock=False):
    return enrich_record(record, mock)

def enrich_record(record: dict, mock: bool = False) -> dict:
    """Try Layer 1 → Layer 2 → Layer 3 in order. Stop at first hit."""
    shareholder_id = record.get('shareholder_id') or record.get('folio_no') or str(record.get('sr_no', ''))
    if is_already_processed(shareholder_id):
        return record
    result = _search_layer1(record)
    if result:
        return save_and_return(record, result, layer=1)
    result = search_public(record)
    if result:
        return save_and_return(record, result, layer=2)
    result = search_paid(record, mock=mock)
    if result:
        return save_and_return(record, result, layer=3)
    return save_and_return(record, None, layer=0)

def run_enrichment(df, mock=False, workers=5, resume=True):
    records = df.to_dict('records')
    results = []
    already_done = set()
    if resume and os.path.exists(CHECKPOINT_PATH):
        already_done = load_checkpoint()
        records = [r for r in records if (r.get('shareholder_id') or r.get('folio_no') or str(r.get('sr_no', ''))) not in already_done]
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(enrich_record_with_retry, rec, mock): rec for rec in records}
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            results.append(result)
            if i % 10 == 0:
                update_progress(i, len(records), results)
            if i % 100 == 0:
                save_checkpoint(results)
    return pd.DataFrame(results)

def run_enrichment_from_file(input_path, mock=False, limit=None, workers=5, resume=False):
    if resume:
        if not os.path.exists(CHECKPOINT_PATH):
            print("No checkpoint found — use --input to start a new run.")
            return
        df = pd.read_excel(OUTPUT_PATH)
    else:
        df = pd.read_excel(input_path)
        if limit:
            df = df.head(limit)
    result_df = run_enrichment(df, mock=mock, workers=workers, resume=resume)
    result_df.to_excel(OUTPUT_PATH, index=False)
    write_final_report(result_df)
    print(f"Enrichment complete. Output saved to {OUTPUT_PATH}")

def write_final_report(df):
    total = len(df)
    l1 = (df['contact_layer'] == 1).sum()
    l2 = (df['contact_layer'] == 2).sum()
    l3 = (df['contact_layer'] == 3).sum()
    none = (df['contact_layer'] == 0).sum()
    found = l1 + l2 + l3
    duration = (time.time() - start_time) / 3600
    report = {
        "run_date": datetime.now().strftime("%Y-%m-%d"),
        "total_records": total,
        "layer1_found": int(l1),
        "layer1_hit_rate": f"{l1/total:.1%}" if total else "0.0%",
        "layer2_found": int(l2),
        "layer2_hit_rate": f"{l2/total:.1%}" if total else "0.0%",
        "layer3_found": int(l3),
        "layer3_hit_rate": f"{l3/total:.1%}" if total else "0.0%",
        "total_found": int(found),
        "overall_hit_rate": f"{found/total:.1%}" if total else "0.0%",
        "no_contact": int(none),
        "duration_hours": round(duration, 1),
        "output_file": OUTPUT_PATH
    }
    with open(REPORT_PATH, 'w') as f:
        json.dump(report, f, indent=2)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str)
    parser.add_argument('--mock', action='store_true')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--workers', type=int, default=5)
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    if args.resume:
        run_enrichment_from_file(args.input, mock=args.mock, limit=args.limit, workers=args.workers, resume=True)
    elif args.input:
        run_enrichment_from_file(args.input, mock=args.mock, limit=args.limit, workers=args.workers, resume=False)
    else:
        print("Either --input or --resume must be specified.")

if __name__ == "__main__":
    main()
