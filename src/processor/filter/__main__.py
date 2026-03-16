"""
CLI interface for the filter module.

Usage:
    python -m src.processor.filter --input data/output/master_deduplicated.xlsx --preset high_value
    python -m src.processor.filter --input data/output/master_deduplicated.xlsx --min-holding 500 --min-dividend 10000
    python -m src.processor.filter --input data/output/master_deduplicated.xlsx --min-holding 500 --logic and
"""

import argparse
import sys
import json
import logging
from pathlib import Path
import pandas as pd
import yaml

from .filter import Filter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('filter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/settings.yaml") -> dict:
    """Load configuration from settings.yaml."""
    config_path = Path(config_path)
    if not config_path.exists():
        logger.warning(f"Config file not found: {config_path}, using defaults")
        return {
            'filter_presets': {
                'high_value': {
                    'min_current_holding': 500,
                    'min_total_dividend': 10000,
                    'logic': 'or'
                },
                'ultra_high_value': {
                    'min_current_holding': 5000,
                    'min_total_dividend': 100000,
                    'logic': 'and'
                }
            }
        }
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config or {}


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Apply value-based filters to isolate high-value shareholder records"
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default='data/output/master_deduplicated.xlsx',
        help='Input dataset file (default: data/output/master_deduplicated.xlsx)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='data/output/',
        help='Output directory (default: data/output/)'
    )
    
    parser.add_argument(
        '--preset',
        type=str,
        help='Filter preset name (e.g., high_value, ultra_high_value)'
    )
    
    parser.add_argument(
        '--min-holding',
        type=int,
        default=0,
        help='Minimum current holding (shares)'
    )
    
    parser.add_argument(
        '--min-dividend',
        type=float,
        default=0,
        help='Minimum total dividend amount'
    )
    
    parser.add_argument(
        '--min-single-year-dividend',
        type=float,
        default=0,
        help='Minimum dividend in any single year'
    )
    
    parser.add_argument(
        '--investor-type',
        type=str,
        help='Filter by investor type (e.g., Promoter, HNI, Institutional)'
    )
    
    parser.add_argument(
        '--logic',
        type=str,
        choices=['or', 'and'],
        default='or',
        help='Logic for combining criteria: "or" (any) or "and" (all) (default: or)'
    )
    
    parser.add_argument(
        '--database',
        type=str,
        default='data/pipeline.db',
        help='SQLite database path (default: data/pipeline.db)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config()
        presets = config.get('filter_presets', {})
        
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
        
        logger.info(f"Loaded {len(df)} records from {input_path}")
        
        # Initialize filter
        filter_obj = Filter(presets=presets, verbose=True)
        
        # Apply filter
        if args.preset:
            logger.info(f"Applying preset: {args.preset}")
            df_filtered = filter_obj.apply_preset(df, args.preset)
        else:
            logger.info("Applying custom filter criteria")
            df_filtered = filter_obj.apply_filter(
                df,
                min_current_holding=args.min_holding,
                min_total_dividend=args.min_dividend,
                min_single_year_dividend=args.min_single_year_dividend,
                investor_type=args.investor_type,
                logic=args.logic
            )
            df_filtered['filter_preset_used'] = 'custom'
        
        # Show statistics
        stats = filter_obj.get_statistics()
        logger.info(f"\nFilter Statistics:")
        logger.info(f"  Total input: {stats['total_input']}")
        logger.info(f"  High-value records: {stats['high_value_count']}")
        logger.info(f"  Hit rate: {100 * stats['high_value_count'] / max(1, stats['total_input']):.1f}%")
        
        # Save filtered records
        logger.info("\nSaving filtered records...")
        output_files = filter_obj.save_filtered_records(df_filtered, args.output)
        logger.info(f"  Excel: {output_files['excel']}")
        logger.info(f"  CSV: {output_files['csv']}")
        
        # Update database
        logger.info("\nUpdating SQLite database...")
        filter_obj.update_database(df_filtered, args.database)
        
        # Update progress
        logger.info("Updating progress file...")
        filter_obj.update_progress()
        
        logger.info("\n✅ Filter completed successfully")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
