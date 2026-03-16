"""
CLI interface for Layer 1 in-house database contact search.

Usage:
    python -m src.enrichment.layer1_inhouse --input data/output/master_filtered.xlsx
    python -m src.enrichment.layer1_inhouse --input data/output/master_filtered.xlsx --threshold 80
    python -m src.enrichment.layer1_inhouse --load-only
"""

import argparse
import sys
import logging
from pathlib import Path
import pandas as pd

from .layer1_inhouse import Layer1InhouseSearch

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('layer1_inhouse.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Layer 1 contact search: Search in-house demat database for shareholder contacts"
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default='data/output/master_filtered.xlsx',
        help='Input dataset file (default: data/output/master_filtered.xlsx)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='data/output/master_enriched_layer1.xlsx',
        help='Output enriched file (default: data/output/master_enriched_layer1.xlsx)'
    )
    
    parser.add_argument(
        '--database',
        type=str,
        default='data/pipeline.db',
        help='SQLite database path (default: data/pipeline.db)'
    )
    
    parser.add_argument(
        '--inhouse-folder',
        type=str,
        default='data/inhouse_db/',
        help='Inhouse database folder (default: data/inhouse_db/)'
    )
    
    parser.add_argument(
        '--threshold',
        type=int,
        default=85,
        help='Fuzzy match threshold 0-100 (default: 85)'
    )
    
    parser.add_argument(
        '--load-only',
        action='store_true',
        help='Only load inhouse database without searching'
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize Layer 1 searcher
        layer1 = Layer1InhouseSearch(
            db_path=args.database,
            fuzzy_threshold=args.threshold,
            verbose=True
        )
        
        # Load inhouse database
        logger.info(f"Loading inhouse database from: {args.inhouse_folder}")
        inhouse_count = layer1.load_inhouse_database(args.inhouse_folder)
        logger.info(f"Loaded {inhouse_count} inhouse contact records")
        
        if args.load_only:
            logger.info("Load-only mode: exiting without searching")
            sys.exit(0)
        
        # Load input dataset
        input_path = Path(args.input)
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            sys.exit(1)
        
        logger.info(f"Loading input dataset: {input_path}")
        if args.input.endswith('.xlsx'):
            df = pd.read_excel(input_path)
        elif args.input.endswith('.csv'):
            df = pd.read_csv(input_path)
        else:
            logger.error("Unsupported file format. Use .xlsx or .csv")
            sys.exit(1)
        
        logger.info(f"Loaded {len(df)} shareholder records")
        
        # Search inhouse database
        logger.info("\nSearching inhouse database...")
        df_enriched = layer1.search_inhouse_batch(df)
        
        # Show statistics
        stats = layer1.get_statistics()
        logger.info(f"\nLayer 1 Search Results:")
        logger.info(f"  Total searched: {stats['total_searched']}")
        logger.info(f"  Contacts found: {stats['matches_found']}")
        logger.info(f"  Exact demat matches: {stats['exact_demat_matches']}")
        logger.info(f"  Fuzzy name+address matches: {stats['fuzzy_name_address_matches']}")
        logger.info(f"  Hit rate: {stats['hit_rate']:.1f}%")
        
        # Save enriched records
        logger.info(f"\nSaving enriched records to: {args.output}")
        layer1.save_enriched_records(df_enriched, args.output)
        
        # Save to database
        logger.info("Saving search results to database...")
        layer1.save_search_results(df_enriched, args.database)
        
        # Update progress
        logger.info("Updating progress file...")
        layer1.update_progress()
        
        logger.info("\nLayer 1 search completed successfully")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
