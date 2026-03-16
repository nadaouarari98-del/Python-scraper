import logging
import sqlite3
import json
from pathlib import Path
from typing import Optional
import pandas as pd
from tqdm import tqdm
import yaml

from src.enrichment.sources.base import ContactResult, PublicSource
from src.enrichment.sources.mca21 import MCA21Source
from src.enrichment.sources.bse import BSESource
from src.enrichment.sources.nse import NSESource
from src.enrichment.sources.iepf import IEPFSource
from src.enrichment.sources.data_gov import DataGovSource
from src.enrichment.sources.truecaller import TruecallerSource

logger = logging.getLogger(__name__)

# Configuration
CONFIG_FILE = Path('config/settings.yaml')
OUTPUT_DIR = Path('data/output')
DB_FILE = Path('data/inhouse_db/contact_results.db')

# Global sources cache (initialized once per session)
_SOURCES_CACHE = None


def _load_config() -> dict:
    """Load settings from YAML config file."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.safe_load(f) or {}
            return config.get('layer2_sources', {})
    return {}


def _get_sources() -> list[PublicSource]:
    """Get list of enabled sources in order from config (cached globally)."""
    global _SOURCES_CACHE
    
    # Return cached sources if already initialized
    if _SOURCES_CACHE is not None:
        return _SOURCES_CACHE
    
    config = _load_config()
    sources = []
    
    # Order matters: try sources in this sequence
    source_classes = [
        ('mca21', MCA21Source),
        ('bse', BSESource),
        ('nse', NSESource),
        ('iepf', IEPFSource),
        ('data_gov', DataGovSource),
        ('truecaller', TruecallerSource),
    ]
    
    for source_id, source_class in source_classes:
        source_config = config.get(source_id, {})
        if source_config.get('enabled', True):
            source = source_class()
            # Update rate limit from config if specified
            if 'rate_limit_seconds' in source_config:
                source.rate_limit_seconds = source_config['rate_limit_seconds']
            sources.append(source)
            logger.info(f"Loaded source: {source_id}")
    
    # Cache globally so it persists across records
    _SOURCES_CACHE = sources
    return _SOURCES_CACHE


def _init_db():
    """Initialize SQLite database for contact results."""
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(DB_FILE))
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contact_results (
            id INTEGER PRIMARY KEY,
            shareholder_id TEXT,
            source_tried TEXT,
            source_found TEXT,
            contact_number TEXT,
            email TEXT,
            confidence_score REAL,
            source_layer INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()


def _save_to_db(shareholder_id: str, source_tried: str, result: Optional[ContactResult]):
    """Save enrichment result to database."""
    _init_db()
    
    conn = sqlite3.connect(str(DB_FILE))
    cursor = conn.cursor()
    
    source_found = result.source if result else None
    contact_number = result.contact_number if result else None
    email = result.email if result else None
    confidence_score = result.confidence if result else 0.0
    
    cursor.execute("""
        INSERT INTO contact_results 
        (shareholder_id, source_tried, source_found, contact_number, email, confidence_score, source_layer)
        VALUES (?, ?, ?, ?, ?, ?, 2)
    """, (shareholder_id, source_tried, source_found, contact_number, email, confidence_score))
    
    conn.commit()
    conn.close()


def search_public(record: dict) -> Optional[dict]:
    """
    Try to find contact information using public data sources.
    
    Returns enriched record if found, None otherwise.
    """
    record_id = record.get('id', record.get('shareholder_id', '?'))
    
    # Skip if already has contact info from Layer 1
    if record.get('contact_number') or record.get('email'):
        logger.debug(f"Record {record_id}: already has Layer 1 contact info, skipping Layer 2")
        return None
    
    sources = _get_sources()
    sources_tried = []
    
    for source in sources:
        try:
            if not source.is_available():
                logger.debug(f"Record {record_id}: source {source.source_id} not available")
                continue
            
            logger.debug(f"Record {record_id}: trying {source.source_id}")
            result = source.search(record)
            sources_tried.append(source.source_id)
            
            # Save attempt to database
            _save_to_db(record_id, source.source_id, result)
            
            # Stop at first successful source that returns contact_number or email
            if result and (result.contact_number or result.email):
                logger.info(f"Record {record_id}: found contact via {result.source} - "
                           f"phone={result.contact_number}, email={result.email}")
                
                # Enrich record with new contact info
                enriched = record.copy()
                enriched['contact_number'] = result.contact_number
                enriched['email'] = result.email
                enriched['source_layer'] = 2
                enriched['enriched_source'] = result.source
                enriched['confidence_score'] = result.confidence
                
                return enriched
        
        except Exception as e:
            logger.error(f"Record {record_id}: error with source {source.source_id}: {e}")
            sources_tried.append(f"{source.source_id}:error")
            continue
    
    logger.debug(f"Record {record_id}: no public source found contact (tried: {', '.join(sources_tried)})")
    return None


def search_public_batch(df: pd.DataFrame, progress_bar: bool = True) -> pd.DataFrame:
    """
    Batch process DataFrame to enrich with public source data.
    
    Only processes records where contact_number IS NULL and email IS NULL.
    Returns DataFrame with new contact info added.
    """
    # Filter to records without Layer 1 contact info
    if 'contact_number' not in df.columns:
        df['contact_number'] = ''
    if 'email_id' not in df.columns:
        df['email_id'] = ''
    if 'email' not in df.columns:
        df['email'] = ''
    mask = (df['contact_number'].isna() | (df['contact_number'] == '')) &            (df['email_id'].isna() | (df['email_id'] == ''))
    to_process = df[mask].copy()
    
    if len(to_process) == 0:
        logger.info("search_public_batch: No records to process (all have Layer 1 contacts)")
        return df
    
    logger.info(f"search_public_batch: Processing {len(to_process)} records out of {len(df)}")
    
    # Track per-source hits
    source_hits = {}
    
    # Iterator for progress
    iterator = tqdm(to_process.iterrows(), total=len(to_process), 
                    desc="Layer 2 enrichment", disable=not progress_bar)
    
    for idx, row in iterator:
        record = row.to_dict()
        enriched = search_public(record)
        
        if enriched:
            source = enriched.get('enriched_source')
            source_hits[source] = source_hits.get(source, 0) + 1
            
            # Update original dataframe row
            df.loc[idx, 'contact_number'] = enriched.get('contact_number')
            df.loc[idx, 'email'] = enriched.get('email')
            df.loc[idx, 'source_layer'] = 2
            df.loc[idx, 'enriched_source'] = source
    
    # Print per-source statistics
    logger.info("\n=== Layer 2 Enrichment Results ===")
    total_found = sum(source_hits.values())
    logger.info(f"Total contacts found: {total_found} / {len(to_process)}")
    logger.info(f"Hit rate: {100 * total_found / len(to_process):.1f}%")
    logger.info("\nPer-source breakdown:")
    for source, count in sorted(source_hits.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {source}: {count} hits")
    
    # Update progress.json
    progress_file = OUTPUT_DIR / 'progress.json'
    try:
        if progress_file.exists():
            with open(progress_file, 'r') as f:
                progress = json.load(f)
        else:
            progress = {}
        
        progress['layer2_searched'] = len(to_process)
        progress['layer2_found'] = total_found
        progress['layer2_hit_rate'] = total_found / len(to_process) if len(to_process) > 0 else 0
        
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
        
        logger.info(f"Updated progress file: {progress_file}")
    except Exception as e:
        logger.error(f"Failed to update progress.json: {e}")
    
    return df


if __name__ == "__main__":
    import argparse
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description="Enrich shareholder data with public sources (Layer 2)")
    parser.add_argument("--input", required=True, help="Input Excel file path")
    parser.add_argument("--output", default="data/output/layer2_enriched.xlsx", 
                       help="Output Excel file path")
    parser.add_argument("--limit", type=int, default=None, 
                       help="Process only N records (for testing)")
    
    args = parser.parse_args()
    
    logger.info(f"Loading input file: {args.input}")
    df = pd.read_excel(args.input)
    
    if args.limit:
        df = df.head(args.limit)
        logger.info(f"Limiting to {args.limit} records")
    
    logger.info(f"Loaded {len(df)} records")
    
    # Run enrichment
    df_enriched = search_public_batch(df)
    
    # Save output
    output_dir = Path(args.output).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    df_enriched.to_excel(args.output, index=False)
    
    logger.info(f"Saved enriched data to: {args.output}")
