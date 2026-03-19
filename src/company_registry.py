import pandas as pd
import sqlite3
import os
import math
from datetime import datetime

DB_PATH = 'data/output/pipeline.db'

def create_registry_table():
    os.makedirs('data/output', exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS companies_registry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT,
        nse_symbol TEXT,
        bse_code TEXT,
        isin TEXT,
        sector TEXT,
        series TEXT,
        scrape_status TEXT DEFAULT "pending",
        last_scraped TEXT,
        pdfs_found INTEGER DEFAULT 0,
        records_extracted INTEGER DEFAULT 0,
        investor_page_url TEXT,
        added_at TEXT
    )''')
    conn.commit()
    conn.close()

def load_from_csv(filepath: str) -> int:
    create_registry_table()
    try:
        df = pd.read_csv(filepath, encoding='utf-8')
    except:
        df = pd.read_csv(filepath, encoding='latin-1')
    
    print(f'Columns found: {list(df.columns)}')
    print(f'Total rows: {len(df)}')
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    count = 0
    
    for _, row in df.iterrows():
        name = str(row.get('Company', '')).strip()
        symbol = str(row.get('Symbol', '')).strip()
        isin = str(row.get('ISIN Code', '')).strip()
        sector = str(row.get('Sector', '')).strip()
        series = str(row.get('Series', '')).strip()
        
        if not name or name == 'nan':
            continue
        
        existing = c.execute(
            'SELECT id FROM companies_registry WHERE company_name=? OR nse_symbol=?',
            (name, symbol)
        ).fetchone()
        
        if not existing:
            c.execute('''INSERT INTO companies_registry 
                (company_name, nse_symbol, isin, sector, series, scrape_status, added_at)
                VALUES (?,?,?,?,?,"pending",?)''',
                (name, symbol, isin, sector, series, datetime.now().isoformat()))
            count += 1
    
    conn.commit()
    conn.close()
    print(f'Saved {count} new companies to database')
    return count

def get_companies(status=None, limit=None, search=None, sector=None, sort='company_name', order='asc') -> pd.DataFrame:
    create_registry_table()
    conn = sqlite3.connect(DB_PATH)
    allowed_sort = ['company_name', 'nse_symbol', 'sector', 'scrape_status', 'pdfs_found', 'records_extracted']
    if sort not in allowed_sort:
        sort = 'company_name'
    order_sql = 'DESC' if order.lower() == 'desc' else 'ASC'
    query = 'SELECT * FROM companies_registry WHERE 1=1'
    params = []
    if status:
        query += ' AND scrape_status=?'
        params.append(status)
    if search:
        query += ' AND (company_name LIKE ? OR nse_symbol LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%'])
    if sector:
        query += ' AND sector LIKE ?'
        params.append(f'%{sector}%')
    query += f' ORDER BY {sort} {order_sql}'
    if limit:
        query += f' LIMIT {limit}'
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def get_companies_count(status=None, search=None, sector=None) -> int:
    create_registry_table()
    conn = sqlite3.connect(DB_PATH)
    query = 'SELECT COUNT(*) as n FROM companies_registry WHERE 1=1'
    params = []
    if status:
        query += ' AND scrape_status=?'
        params.append(status)
    if search:
        query += ' AND (company_name LIKE ? OR nse_symbol LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%'])
    if sector:
        query += ' AND sector LIKE ?'
        params.append(f'%{sector}%')
    result = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return int(result.iloc[0]['n'])

def update_company_status(company_id: int, status: str, pdfs_found: int = 0, records: int = 0, url: str = None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''UPDATE companies_registry 
                 SET scrape_status=?, last_scraped=?, pdfs_found=?, records_extracted=?, investor_page_url=?
                 WHERE id=?''',
              (status, datetime.now().isoformat(), pdfs_found, records, url, company_id))
    conn.commit()
    conn.close()

def get_registry_stats() -> dict:
    create_registry_table()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    total = c.execute('SELECT COUNT(*) FROM companies_registry').fetchone()[0]
    pending = c.execute('SELECT COUNT(*) FROM companies_registry WHERE scrape_status="pending"').fetchone()[0]
    complete = c.execute('SELECT COUNT(*) FROM companies_registry WHERE scrape_status="complete"').fetchone()[0]
    failed = c.execute('SELECT COUNT(*) FROM companies_registry WHERE scrape_status="error"').fetchone()[0]
    no_pdfs = c.execute('SELECT COUNT(*) FROM companies_registry WHERE scrape_status="no_pdfs"').fetchone()[0]
    conn.close()
    return {"total": total, "pending": pending, "complete": complete, "failed": failed, "no_pdfs": no_pdfs}

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--load', help='CSV file path')
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--list', action='store_true')
    args = parser.parse_args()
    if args.load:
        load_from_csv(args.load)
    if args.stats:
        print(get_registry_stats())
    if args.list:
        df = get_companies(limit=20)
        print(df[['id','company_name','nse_symbol','sector','scrape_status']].to_string())
