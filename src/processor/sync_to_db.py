import os
import sqlite3
import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def sync_excel_to_db():
    root = Path(__file__).resolve().parents[2]
    excel_path = root / 'data' / 'output' / 'master_merged.xlsx'
    db_path = root / 'data' / 'pipeline.db'

    if not excel_path.exists():
        logger.error(f"Excel file not found at {excel_path}")
        return

    logger.info(f"Loading data from {excel_path}...")
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        logger.error(f"Error reading excel: {e}")
        return

    if df.empty:
        logger.info("DataFrame is empty. Nothing to sync.")
        return

    # Map columns to what the dashboard expects
    if 'name' in df.columns and 'full_name' not in df.columns:
        df['full_name'] = df['name']
    
    if 'mobile' in df.columns and 'contact_number' not in df.columns:
        df['contact_number'] = df['mobile']
    elif 'contact' in df.columns and 'contact_number' not in df.columns:
        df['contact_number'] = df['contact']
        
    if 'pan_number' in df.columns and 'pan' not in df.columns:
        df['pan'] = df['pan_number']
        
    if 'total_dividend' in df.columns and 'dividend' not in df.columns:
        df['dividend'] = df['total_dividend']
        
    if 'current_holding' in df.columns and 'shares' not in df.columns:
        df['shares'] = df['current_holding']
        
    if 'parsed_at' in df.columns and 'processed_at' not in df.columns:
        df['processed_at'] = df['parsed_at']

    # We use SQLite to_sql to write to a temp table, then swap it atomically.
    try:
        from sqlalchemy import create_engine, text
        db_path_str = str(db_path.absolute()).replace("\\", "/")
        engine = create_engine(f"sqlite:///{db_path_str}")
        
        # Write to temporary table
        df.to_sql("shareholders_tmp", engine, if_exists="replace", index=False)
        
        # Atomically swap tables
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS shareholders;"))
            conn.execute(text("ALTER TABLE shareholders_tmp RENAME TO shareholders;"))
            conn.commit()
        
        # Run VACUUM outside the transaction to clean up dropped table space
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn_vac:
            conn_vac.execute(text("VACUUM;"))
            
        logger.info(f"Successfully synced {len(df)} records into SQLite {db_path} and vacuumed DB.")
    except Exception as e:
        logger.error(f"Failed to sync with sqlalchemy: {e}")
        # fallback to sqlite3
        try:
            conn = sqlite3.connect(db_path)
            df.to_sql("shareholders_tmp", conn, if_exists="replace", index=False)
            
            # Autocommit mode required for VACUUM outside transaction
            conn.isolation_level = None
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION;")
            cursor.execute("DROP TABLE IF EXISTS shareholders;")
            cursor.execute("ALTER TABLE shareholders_tmp RENAME TO shareholders;")
            cursor.execute("COMMIT;")
            
            cursor.execute("VACUUM;")
            conn.close()
            logger.info(f"Successfully synced {len(df)} records using raw sqlite3 {db_path} and vacuumed DB.")
        except Exception as e2:
            logger.error(f"Failed to sync with sqlite3: {e2}")

if __name__ == "__main__":
    sync_excel_to_db()
