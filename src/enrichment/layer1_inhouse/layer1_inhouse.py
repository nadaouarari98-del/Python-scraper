"""
Layer 1: In-House Demat Database Contact Search Module

Search the client's own in-house Demat account holder database for matching contact information
before going to any external source. This is the FIRST layer of contact enrichment.

Features:
- Exact match on demat account number
- Fuzzy match on name + address (85% threshold)
- Persistent SQLite database for inhouse records
- Load/refresh inhouse database from Excel files in data/inhouse_db/
- Track search results with match type and confidence score
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional, Dict, List, Tuple
from datetime import datetime
from pathlib import Path
from rapidfuzz import fuzz
import sqlite3
from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean, DateTime, Text
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import select

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()


class InhouseContact(Base):
    """SQLAlchemy ORM model for inhouse demat contacts."""
    __tablename__ = 'inhouse_contacts'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    address = Column(String)
    demat_no = Column(String, unique=True, index=True)
    contact = Column(String)
    email = Column(String)
    loaded_at = Column(DateTime, default=datetime.utcnow)


class ContactSearchResult(Base):
    """SQLAlchemy ORM model for contact search results."""
    __tablename__ = 'contact_results'
    
    id = Column(Integer, primary_key=True)
    shareholder_id = Column(String, index=True)
    folio_no = Column(String)
    name = Column(String)
    match_found = Column(Boolean, default=False)
    match_type = Column(String)  # demat_exact, name_address_fuzzy, none
    match_score = Column(Float)
    contact_number = Column(String)
    email = Column(String)
    source = Column(String, default='inhouse_db')
    inhouse_name = Column(String)
    inhouse_address = Column(String)
    inhouse_demat = Column(String)
    searched_at = Column(DateTime, default=datetime.utcnow)


class Layer1InhouseSearch:
    """
    Layer 1 contact search: In-house Demat database.
    
    Search for shareholder contact information in the client's own inhouse database
    using exact demat matching and fuzzy name+address matching.
    """
    
    def __init__(self, db_path: str = "data/pipeline.db", fuzzy_threshold: int = 85, verbose: bool = True):
        """
        Initialize Layer1 searcher.
        
        Args:
            db_path: Path to SQLite database
            fuzzy_threshold: Minimum fuzzy match score (0-100)
            verbose: Enable detailed logging
        """
        self.db_path = db_path
        self.fuzzy_threshold = fuzzy_threshold
        self.verbose = verbose
        self.inhouse_data = None
        self.statistics = {
            'total_searched': 0,
            'matches_found': 0,
            'exact_demat_matches': 0,
            'fuzzy_name_address_matches': 0,
            'no_matches': 0,
            'hit_rate': 0.0
        }
    
    def load_inhouse_database(self, inhouse_db_folder: str = "data/inhouse_db/") -> int:
        """
        Load all Excel files from inhouse_db folder and create/update SQLite database.
        
        Args:
            inhouse_db_folder: Folder containing inhouse Excel files
        
        Returns:
            Number of records loaded
        """
        inhouse_path = Path(inhouse_db_folder)
        if not inhouse_path.exists():
            logger.warning(f"Inhouse database folder not found: {inhouse_path}")
            return 0
        
        # Find all Excel files
        excel_files = list(inhouse_path.glob("*.xlsx")) + list(inhouse_path.glob("*.xls"))
        
        if not excel_files:
            logger.warning(f"No Excel files found in {inhouse_path}")
            return 0
        
        if self.verbose:
            logger.info(f"Found {len(excel_files)} Excel files in inhouse database folder")
        
        engine = create_engine(f'sqlite:///{self.db_path}')
        Base.metadata.create_all(engine)
        
        total_loaded = 0
        
        with Session(engine) as session:
            # Clear existing inhouse contacts
            session.query(InhouseContact).delete()
            session.commit()
            
            for excel_file in excel_files:
                try:
                    if self.verbose:
                        logger.info(f"Loading: {excel_file.name}")
                    
                    df = pd.read_excel(excel_file)
                    
                    # Map columns (case-insensitive)
                    col_map = {}
                    for col in df.columns:
                        col_lower = col.lower().strip()
                        if 'name' in col_lower:
                            col_map['name'] = col
                        elif 'address' in col_lower:
                            col_map['address'] = col
                        elif 'demat' in col_lower:
                            col_map['demat_no'] = col
                        elif 'contact' in col_lower and 'email' not in col_lower:
                            col_map['contact'] = col
                        elif 'email' in col_lower:
                            col_map['email'] = col
                    
                    # Validate required columns
                    required = ['name', 'demat_no', 'contact', 'email']
                    missing = [col for col in required if col not in col_map]
                    if missing:
                        logger.warning(f"Missing columns in {excel_file.name}: {missing}")
                        continue
                    
                    # Load records
                    for _, row in df.iterrows():
                        demat_no = str(row.get(col_map['demat_no'], '')).strip()
                        
                        # Skip empty demat accounts
                        if not demat_no or demat_no.lower() == 'nan':
                            continue
                        
                        # Check if already exists
                        existing = session.query(InhouseContact).filter(
                            InhouseContact.demat_no == demat_no
                        ).first()
                        
                        if not existing:
                            contact_record = InhouseContact(
                                name=str(row.get(col_map['name'], '')).strip(),
                                address=str(row.get(col_map.get('address', ''), '')).strip(),
                                demat_no=demat_no,
                                contact=str(row.get(col_map['contact'], '')).strip(),
                                email=str(row.get(col_map['email'], '')).strip()
                            )
                            session.add(contact_record)
                            total_loaded += 1
                    
                    session.commit()
                    if self.verbose:
                        logger.info(f"Loaded {total_loaded} records from {excel_file.name}")
                
                except Exception as e:
                    logger.error(f"Error loading {excel_file.name}: {e}")
                    continue
        
        if self.verbose:
            logger.info(f"Total inhouse records loaded: {total_loaded}")
        
        return total_loaded
    
    def search_inhouse(self, record: Dict) -> Optional[Dict]:
        """
        Search for a single shareholder record in inhouse database.
        
        Args:
            record: Dictionary with shareholder data (name, address, demat_account)
        
        Returns:
            Dictionary with match results or None if no match found
        """
        engine = create_engine(f'sqlite:///{self.db_path}')
        
        with Session(engine) as session:
            # Step A: Exact match on demat account (skip if NaN or empty)
            demat_account = str(record.get('demat_account', '')).strip()
            if demat_account and demat_account.lower() != 'nan' and demat_account != '':
                exact_match = session.query(InhouseContact).filter(
                    InhouseContact.demat_no == demat_account
                ).first()
                
                if exact_match:
                    return {
                        'match_found': True,
                        'match_type': 'demat_exact',
                        'match_score': 100.0,
                        'contact_number': exact_match.contact,
                        'email': exact_match.email,
                        'inhouse_name': exact_match.name,
                        'inhouse_address': exact_match.address,
                        'inhouse_demat': exact_match.demat_no
                    }
            
            # Step B: Fuzzy match on name + address
            name = str(record.get('name', '')).strip().lower()
            address = str(record.get('address', '')).strip().lower()
            
            # Treat 'nan' string as empty (from NaN float conversion)
            if address == 'nan' or not address:
                address = ''
            
            if name:
                # Get all inhouse records
                all_contacts = session.query(InhouseContact).all()
                
                best_match = None
                best_score = 0
                
                for contact in all_contacts:
                    contact_name = contact.name.lower() if contact.name else ''
                    contact_addr = contact.address.lower() if contact.address else ''
                    
                    # Skip if names don't match at all
                    if not contact_name:
                        continue
                    
                    # Calculate name similarity
                    name_sim = fuzz.token_sort_ratio(name, contact_name)
                    
                    # Calculate address similarity (if both have addresses)
                    if address and contact_addr:
                        addr_sim = fuzz.token_sort_ratio(address, contact_addr)
                        combined_sim = (name_sim + addr_sim) / 2
                    else:
                        # If no address provided or no contact address, use name alone
                        combined_sim = name_sim
                    
                    # Track best match if score meets threshold
                    if combined_sim >= self.fuzzy_threshold and combined_sim > best_score:
                        best_score = combined_sim
                        best_match = contact
                
                if best_match:
                    return {
                        'match_found': True,
                        'match_type': 'name_address_fuzzy',
                        'match_score': best_score,
                        'contact_number': best_match.contact,
                        'email': best_match.email,
                        'inhouse_name': best_match.name,
                        'inhouse_address': best_match.address,
                        'inhouse_demat': best_match.demat_no
                    }
        
        # Step C: No match found
        return None
    
    def search_inhouse_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Search for all shareholder records in inhouse database.
        
        Args:
            df: DataFrame with shareholder records
        
        Returns:
            Enriched DataFrame with contact information and match details
        """
        self.statistics['total_searched'] = len(df)
        
        if self.verbose:
            logger.info(f"Searching inhouse database for {len(df)} records...")
        
        df_enriched = df.copy()
        df_enriched['contact_number'] = None
        df_enriched['email'] = None
        df_enriched['contact_source'] = None
        df_enriched['match_type'] = None
        df_enriched['match_score'] = None
        df_enriched['inhouse_name'] = None
        df_enriched['inhouse_address'] = None
        df_enriched['inhouse_demat'] = None
        
        # Search each record
        for idx, row in df.iterrows():
            record_dict = row.to_dict()
            result = self.search_inhouse(record_dict)
            
            if result and result['match_found']:
                df_enriched.at[idx, 'contact_number'] = result['contact_number']
                df_enriched.at[idx, 'email'] = result['email']
                df_enriched.at[idx, 'contact_source'] = 'inhouse_db'
                df_enriched.at[idx, 'match_type'] = result['match_type']
                df_enriched.at[idx, 'match_score'] = result['match_score']
                df_enriched.at[idx, 'inhouse_name'] = result['inhouse_name']
                df_enriched.at[idx, 'inhouse_address'] = result['inhouse_address']
                df_enriched.at[idx, 'inhouse_demat'] = result['inhouse_demat']
                
                # Update statistics
                if result['match_type'] == 'demat_exact':
                    self.statistics['exact_demat_matches'] += 1
                elif result['match_type'] == 'name_address_fuzzy':
                    self.statistics['fuzzy_name_address_matches'] += 1
                
                self.statistics['matches_found'] += 1
            else:
                self.statistics['no_matches'] += 1
        
        # Calculate hit rate
        self.statistics['hit_rate'] = 100 * self.statistics['matches_found'] / max(1, len(df))
        
        if self.verbose:
            logger.info(f"\nSearch results:")
            logger.info(f"  - Total searched: {self.statistics['total_searched']}")
            logger.info(f"  - Matches found: {self.statistics['matches_found']}")
            logger.info(f"  - Exact demat: {self.statistics['exact_demat_matches']}")
            logger.info(f"  - Fuzzy name+address: {self.statistics['fuzzy_name_address_matches']}")
            logger.info(f"  - Hit rate: {self.statistics['hit_rate']:.1f}%")
        
        return df_enriched
    
    def save_enriched_records(self, df: pd.DataFrame, output_path: str = "data/output/master_enriched_layer1.xlsx") -> str:
        """
        Save enriched records to Excel.
        
        Args:
            df: Enriched DataFrame
            output_path: Output file path
        
        Returns:
            Path to saved file
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_excel(output_file, index=False)
        
        if self.verbose:
            logger.info(f"Saved enriched records to {output_file}")
        
        return str(output_file)
    
    def save_search_results(self, df: pd.DataFrame, db_path: str = None) -> None:
        """
        Save search results to SQLite database.
        
        Args:
            df: Enriched DataFrame with search results
            db_path: Database path
        """
        db_path = db_path or self.db_path
        engine = create_engine(f'sqlite:///{db_path}')
        Base.metadata.create_all(engine)
        
        with Session(engine) as session:
            # Clear existing results
            session.query(ContactSearchResult).delete()
            session.commit()
            
            # Insert new results
            for _, row in df.iterrows():
                result = ContactSearchResult(
                    shareholder_id=str(row.get('folio_no', '')),
                    folio_no=str(row.get('folio_no', '')),
                    name=str(row.get('name', '')),
                    match_found=pd.notna(row.get('contact_source')),
                    match_type=str(row.get('match_type', '')) if pd.notna(row.get('match_type')) else None,
                    match_score=float(row.get('match_score', 0)) if pd.notna(row.get('match_score')) else 0,
                    contact_number=str(row.get('contact_number', '')) if pd.notna(row.get('contact_number')) else None,
                    email=str(row.get('email', '')) if pd.notna(row.get('email')) else None,
                    inhouse_name=str(row.get('inhouse_name', '')) if pd.notna(row.get('inhouse_name')) else None,
                    inhouse_address=str(row.get('inhouse_address', '')) if pd.notna(row.get('inhouse_address')) else None,
                    inhouse_demat=str(row.get('inhouse_demat', '')) if pd.notna(row.get('inhouse_demat')) else None
                )
                session.add(result)
            
            session.commit()
            
            if self.verbose:
                logger.info(f"Saved {len(df)} search results to database")
    
    def update_progress(self, progress_file: str = "data/progress_status.json") -> None:
        """
        Update progress JSON with Layer 1 statistics.
        
        Args:
            progress_file: Path to progress JSON file
        """
        import json
        
        progress_path = Path(progress_file)
        progress_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # Load existing progress
            if progress_path.exists():
                with open(progress_path, 'r') as f:
                    progress = json.load(f)
            else:
                progress = {}
            
            # Update with Layer 1 statistics
            progress['layer1_inhouse'] = {
                'total_searched': int(self.statistics['total_searched']),
                'layer1_found': int(self.statistics['matches_found']),
                'exact_demat_matches': int(self.statistics['exact_demat_matches']),
                'fuzzy_matches': int(self.statistics['fuzzy_name_address_matches']),
                'layer1_hit_rate': f"{self.statistics['hit_rate']:.1f}%",
                'timestamp': datetime.now().isoformat()
            }
            
            with open(progress_path, 'w') as f:
                json.dump(progress, f, indent=2)
            
            if self.verbose:
                logger.info(f"Updated progress file: {progress_file}")
        except Exception as e:
            logger.error(f"Error updating progress file: {e}")
    
    def get_statistics(self) -> Dict:
        """Return Layer 1 search statistics."""
        return self.statistics.copy()


def search_inhouse_batch(df: pd.DataFrame, db_path: str = "data/pipeline.db", 
                         fuzzy_threshold: int = 85, verbose: bool = True) -> pd.DataFrame:
    """
    Public API: Search all records in inhouse database.
    
    Args:
        df: Input DataFrame with shareholder records
        db_path: SQLite database path
        fuzzy_threshold: Fuzzy match threshold (0-100)
        verbose: Enable detailed logging
    
    Returns:
        Enriched DataFrame with contact information
    """
    searcher = Layer1InhouseSearch(db_path=db_path, fuzzy_threshold=fuzzy_threshold, verbose=verbose)
    return searcher.search_inhouse_batch(df)
