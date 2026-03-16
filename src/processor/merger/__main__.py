"""
src/processor/merger/__main__.py
--------------------------------
CLI entry point for the merger module.

Usage:
    python -m src.processor.merger --input data/output/parsed/ --output data/output/
    python -m src.processor.merger --help
"""

import argparse
import logging
import sys
from pathlib import Path

from .merger import Merger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent.parent.parent / "merge.log"),
    ],
)
logger = logging.getLogger(__name__)


def main():
    """Parse arguments and execute merge operation."""
    parser = argparse.ArgumentParser(
        description="Merge all parsed Excel files into a unified master dataset",
        prog="python -m src.processor.merger",
    )

    parser.add_argument(
        "--input",
        type=str,
        default="data/output/parsed/",
        help="Input folder containing parsed Excel files (default: data/output/parsed/)",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="data/output/",
        help="Output folder for merged files (default: data/output/)",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        logger.error(f"Input folder does not exist: {input_path}")
        return 1

    logger.info(f"Starting merge operation...")
    logger.info(f"Input folder: {input_path}")
    logger.info(f"Output folder: {output_path}")

    try:
        merger = Merger(input_path, output_path)
        merged_df = merger.merge_all()
        merger.save_outputs(merged_df)
        merger.update_progress()
        merger.log_summary()

        logger.info("Merge operation completed successfully!")
        return 0

    except Exception as e:
        logger.exception(f"Merge operation failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
