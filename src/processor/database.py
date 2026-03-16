"""
src/processor/database.py
-------------------------
SQLite database management for shareholder data with composite primary keys,
indexes, and INSERT OR UPDATE logic.
"""

import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class ShareholderDatabase:
    """Manages SQLite database for shareholder records."""
    
    def __init__(self, db_path: Path):
        """Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(str(self.db_path), timeout=10)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    def create_tables(self):
        """Create shareholders table with proper schema."""
        cursor = self.conn.cursor()
        
        # Main shareholders table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS shareholders (
                folio_no TEXT NOT NULL,
                company_name TEXT NOT NULL,
                sr_no TEXT,
                demat_account TEXT,
                first_name TEXT,
                middle_name TEXT,
                last_name TEXT,
                full_name TEXT,
                father_husband_name TEXT,
                address_line1 TEXT,
                address_line2 TEXT,
                city TEXT,
                state TEXT,
                pincode TEXT,
                country TEXT,
                current_holding INTEGER,
                share_type TEXT,
                total_dividend REAL,
                source_pdf TEXT,
                pdf_financial_year TEXT,
                date_processed TEXT,
                mobile_number TEXT,
                email_id TEXT,
                contact_source TEXT,
                verification_status TEXT,
                crm_push_status TEXT,
                crm_push_date TEXT,
                email_sent TEXT,
                email_sent_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                extra_data TEXT,
                PRIMARY KEY (folio_no, company_name)
            );
        """)
        
        # Create indexes for fast queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pincode 
            ON shareholders(pincode);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_state 
            ON shareholders(state);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_company 
            ON shareholders(company_name);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_full_name 
            ON shareholders(full_name);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_demat 
            ON shareholders(demat_account);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_fy 
            ON shareholders(pdf_financial_year);
        """)
        
        self.conn.commit()
        logger.info("Database tables created successfully")
    
    def insert_or_update_shareholders(self, df: pd.DataFrame) -> Dict[str, int]:
        """Insert or update shareholder records.
        
        Args:
            df: DataFrame with shareholder data
            
        Returns:
            Dict with counts: {'inserted': int, 'updated': int, 'failed': int}
        """
        if df.empty:
            logger.warning("No data to insert")
            return {'inserted': 0, 'updated': 0, 'failed': 0}
        
        stats = {'inserted': 0, 'updated': 0, 'failed': 0}
        cursor = self.conn.cursor()
        
        # Ensure required columns exist
        required = ['folio_no', 'company_name']
        if not all(col in df.columns for col in required):
            logger.error(f"Missing required columns. Need: {required}")
            return stats
        
        # Standardize column names
        df = df.copy()
        df.columns = [col.lower() for col in df.columns]
        
        for idx, row in df.iterrows():
            try:
                folio = str(row.get('folio_no', '')).strip()
                company = str(row.get('company_name', '')).strip()
                
                if not folio or not company:
                    stats['failed'] += 1
                    continue
                
                # Prepare values for insert/update
                values = {
                    'folio_no': folio,
                    'company_name': company,
                    'sr_no': self._safe_str(row.get('sr_no')),
                    'demat_account': self._safe_str(row.get('demat_account')),
                    'first_name': self._safe_str(row.get('first_name')),
                    'middle_name': self._safe_str(row.get('middle_name')),
                    'last_name': self._safe_str(row.get('last_name')),
                    'full_name': self._safe_str(row.get('full_name')),
                    'father_husband_name': self._safe_str(row.get('father_husband_name')),
                    'address_line1': self._safe_str(row.get('address_line1')),
                    'address_line2': self._safe_str(row.get('address_line2')),
                    'city': self._safe_str(row.get('city')),
                    'state': self._safe_str(row.get('state')),
                    'pincode': self._safe_str(row.get('pincode')),
                    'country': self._safe_str(row.get('country')),
                    'current_holding': self._safe_int(row.get('current_holding')),
                    'share_type': self._safe_str(row.get('share_type')),
                    'total_dividend': self._safe_float(row.get('total_dividend')),
                    'source_pdf': self._safe_str(row.get('source_pdf')),
                    'pdf_financial_year': self._safe_str(row.get('pdf_financial_year')),
                    'date_processed': self._safe_str(row.get('date_processed')),
                    'mobile_number': self._safe_str(row.get('mobile_number')),
                    'email_id': self._safe_str(row.get('email_id')),
                    'contact_source': self._safe_str(row.get('contact_source')),
                    'verification_status': self._safe_str(row.get('verification_status')),
                    'crm_push_status': self._safe_str(row.get('crm_push_status')),
                    'crm_push_date': self._safe_str(row.get('crm_push_date')),
                    'email_sent': self._safe_str(row.get('email_sent')),
                    'email_sent_date': self._safe_str(row.get('email_sent_date')),
                }
                
                # Check if record exists
                cursor.execute(
                    "SELECT 1 FROM shareholders WHERE folio_no = ? AND company_name = ?",
                    (folio, company)
                )
                
                if cursor.fetchone():
                    # Update existing record
                    set_clause = ", ".join([f"{k} = ?" for k in values.keys()])
                    vals = list(values.values())
                    cursor.execute(
                        f"UPDATE shareholders SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE folio_no = ? AND company_name = ?",
                        vals + [folio, company]
                    )
                    stats['updated'] += 1
                else:
                    # Insert new record
                    cols = ", ".join(values.keys())
                    placeholders = ", ".join(["?"] * len(values))
                    cursor.execute(
                        f"INSERT INTO shareholders ({cols}) VALUES ({placeholders})",
                        list(values.values())
                    )
                    stats['inserted'] += 1
                    
            except Exception as e:
                logger.error(f"Failed to process row {idx}: {e}")
                stats['failed'] += 1
        
        self.conn.commit()
        logger.info(f"Insert/Update complete: {stats['inserted']} inserted, {stats['updated']} updated, {stats['failed']} failed")
        return stats
    
    def get_by_financial_year(self, fy: str, company: Optional[str] = None) -> pd.DataFrame:
        """Get shareholders for a specific financial year.
        
        Args:
            fy: Financial year (e.g., '2017-18')
            company: Optional company filter
            
        Returns:
            DataFrame with filtered records
        """
        query = "SELECT * FROM shareholders WHERE pdf_financial_year = ?"
        params = [fy]
        
        if company:
            query += " AND company_name = ?"
            params.append(company)
        
        return pd.read_sql_query(query, self.conn, params=params)
    
    def get_all(self) -> pd.DataFrame:
        """Get all shareholder records."""
        return pd.read_sql_query("SELECT * FROM shareholders ORDER BY company_name, folio_no", self.conn)
    
    def get_by_company(self, company: str) -> pd.DataFrame:
        """Get all records for a specific company."""
        return pd.read_sql_query(
            "SELECT * FROM shareholders WHERE company_name = ? ORDER BY folio_no",
            self.conn,
            params=[company]
        )
    
    def get_by_pincode(self, pincode: str) -> pd.DataFrame:
        """Get all records for a specific pincode."""
        return pd.read_sql_query(
            "SELECT * FROM shareholders WHERE pincode = ? ORDER BY company_name, folio_no",
            self.conn,
            params=[pincode]
        )
    
    def get_by_state(self, state: str) -> pd.DataFrame:
        """Get all records for a specific state."""
        return pd.read_sql_query(
            "SELECT * FROM shareholders WHERE state = ? ORDER BY company_name, folio_no",
            self.conn,
            params=[state]
        )
    
    def get_high_value_records(self, min_holding: int = 500, min_dividend: float = 10000) -> pd.DataFrame:
        """Get high-value shareholder records."""
        return pd.read_sql_query(
            """
            SELECT * FROM shareholders 
            WHERE current_holding >= ? AND total_dividend >= ?
            ORDER BY total_dividend DESC
            """,
            self.conn,
            params=[min_holding, min_dividend]
        )
    
    def get_enriched_records(self) -> pd.DataFrame:
        """Get records with enriched contact info."""
        return pd.read_sql_query(
            """
            SELECT * FROM shareholders 
            WHERE mobile_number IS NOT NULL AND mobile_number != ''
            ORDER BY company_name, folio_no
            """,
            self.conn
        )
    
    def _safe_str(self, val: Any) -> str:
        """Convert value to string, handle NaN/None."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        return str(val).strip()
    
    def _safe_int(self, val: Any) -> int:
        """Convert value to int, handle NaN/None."""
        if val is None or (isinstance(val, float) and pd.isna(val)) or val == "":
            return 0
        try:
            return int(float(str(val)))
        except (ValueError, TypeError):
            return 0
    
    def _safe_float(self, val: Any) -> float:
        """Convert value to float, handle NaN/None."""
        if val is None or (isinstance(val, float) and pd.isna(val)) or val == "":
            return 0.0
        try:
            return float(str(val))
        except (ValueError, TypeError):
            return 0.0


def get_database(root_path: Optional[Path] = None) -> ShareholderDatabase:
    """Get or create database instance.
    
    Args:
        root_path: Root project path (defaults to repo root)
        
    Returns:
        ShareholderDatabase instance
    """
    if root_path is None:
        root_path = Path(__file__).resolve().parents[2]
    
    db_path = root_path / 'data' / 'shareholders.db'
    db = ShareholderDatabase(db_path)
    db.create_tables()
    return db
