"""
src/processor/deduplicator/__main__.py
------------------------------------
CLI entry point for the deduplicator module.

Usage:
    python -m src.processor.deduplicator --input data/output/master_merged.xlsx --threshold 85
    python -m src.processor.deduplicator --help
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

from src.parser.excel_writer import safe_excel_write

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from .deduplicator import Deduplicator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent.parent.parent.parent / "dedup.log"),
    ],
)
logger = logging.getLogger(__name__)


def load_config() -> dict:
    """Load deduplication settings from config file."""
    config_path = Path(__file__).parent.parent.parent.parent / "config" / "settings.yaml"
    
    defaults = {
        "deduplication": {
            "fuzzy_threshold": 85,
            "match_on_name_address": True,
            "match_on_demat": True,
        }
    }
    
    if not config_path.exists():
        logger.warning(f"Config file not found: {config_path}, using defaults")
        return defaults
    
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}
        # Merge with defaults
        return {**defaults, **config}
    except Exception as e:
        logger.warning(f"Error loading config: {e}, using defaults")
        return defaults


def main():
    """Parse arguments and execute deduplication."""
    parser = argparse.ArgumentParser(
        description="Remove duplicate shareholder records using fuzzy matching and exact matching",
        prog="python -m src.processor.deduplicator",
    )

    parser.add_argument(
        "--input",
        type=str,
        default="data/output/master_merged.xlsx",
        help="Input file with shareholder records (default: data/output/master_merged.xlsx)",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="data/output/",
        help="Output folder for deduplicated files (default: data/output/)",
    )

    parser.add_argument(
        "--threshold",
        type=int,
        default=None,
        help="Fuzzy matching threshold 0-100 (default: from config or 85)",
    )

    parser.add_argument(
        "--find-only",
        action="store_true",
        help="Find duplicates without removing them",
    )

    args = parser.parse_args()

    # Load config
    config = load_config()
    threshold = args.threshold or config["deduplication"]["fuzzy_threshold"]

    input_path = Path(args.input)
    output_path = Path(args.output)

    # Validate input
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return 1

    logger.info(f"Loading dataset: {input_path}")
    try:
        df = pd.read_excel(input_path)
        logger.info(f"Loaded {len(df)} records")
    except Exception as e:
        logger.error(f"Failed to load input: {e}")
        return 1

    # Create deduplicator
    dedup = Deduplicator(threshold=threshold, verbose=True)

    if args.find_only:
        # Find duplicates without removing
        logger.info(f"Finding duplicates (threshold: {threshold}%)...")
        duplicates_df = dedup.find_duplicates(df)
        
        if duplicates_df.empty:
            logger.info("No duplicates found!")
        else:
            logger.info(f"Found {len(duplicates_df)} duplicate pairs")
            
            # Save duplicate pairs
            output_path.mkdir(parents=True, exist_ok=True)
            duplicates_file = output_path / "duplicate_pairs_review.xlsx"
            safe_excel_write(duplicates_df, str(duplicates_file), index=False)
            logger.info(f"Saved duplicate pairs to: {duplicates_file}")
    else:
        # Deduplicate
        logger.info(f"Deduplicating dataset (threshold: {threshold}%)...")
        clean_df, removed_df = dedup.deduplicate(df)

        # Save outputs
        output_path.mkdir(parents=True, exist_ok=True)

        # Clean dataset
        clean_file = output_path / "master_deduplicated.xlsx"
        safe_excel_write(clean_df, str(clean_file), index=False, engine="openpyxl")
        logger.info(f"Saved clean dataset to: {clean_file}")

        # Removed records
        if not removed_df.empty:
            removed_file = output_path / "duplicates_removed.xlsx"
            safe_excel_write(removed_df, str(removed_file), index=False, engine="openpyxl")
            logger.info(f"Saved removed records to: {removed_file}")

        # Update SQLite database (if available)
        try:
            from sqlalchemy import create_engine

            db_path = output_path / "pipeline.db"
            engine = create_engine(f"sqlite:///{str(db_path.absolute()).replace(chr(92), '/')}")
            clean_df.to_sql("shareholders_deduplicated", engine, if_exists="replace", index=False)
            logger.info(f"Updated SQLite database: {db_path}")
        except Exception as e:
            logger.warning(f"Could not update SQLite database: {e}")

        # Save statistics
        stats_file = output_path / "dedup_stats.json"
        import json
        with open(stats_file, "w") as f:
            json.dump(dedup.get_statistics(), f, indent=2)
        logger.info(f"Saved statistics to: {stats_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
