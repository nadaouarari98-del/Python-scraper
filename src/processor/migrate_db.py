"""
src/processor/migrate_db.py
----------------------------
One-shot migration script: creates all tables and optionally bulk-loads
an existing master Excel file into the new person-wise schema.

Usage (from project root):
    python -m src.processor.migrate_db                       # create tables only
    python -m src.processor.migrate_db --excel data/output/master_merged.xlsx
    python -m src.processor.migrate_db --db data/custom.db
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create/migrate the person-wise shareholder SQLite database."
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to the SQLite file (default: data/shareholders.db)",
    )
    parser.add_argument(
        "--excel",
        default=None,
        help="Path to a master Excel file to bulk-load into the database.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without writing anything.",
    )
    args = parser.parse_args()

    from src.processor.database_schema import (
        DEFAULT_DB_PATH,
        create_all_tables,
        upsert_dataframe_to_db,
    )

    db_path = Path(args.db) if args.db else DEFAULT_DB_PATH

    if args.dry_run:
        _logger.info("[DRY-RUN] Would create tables in: %s", db_path)
        if args.excel:
            _logger.info("[DRY-RUN] Would load Excel: %s", args.excel)
        return 0

    # Step 1: create tables
    _logger.info("Creating tables in: %s", db_path)
    create_all_tables(db_path)
    _logger.info("Tables created successfully.")

    # Step 2 (optional): bulk-load Excel
    if args.excel:
        excel_path = Path(args.excel)
        if not excel_path.exists():
            _logger.error("Excel file not found: %s", excel_path)
            return 1

        _logger.info("Loading Excel: %s", excel_path)
        try:
            import pandas as pd
            # Try multiple sheets; fall back to default (sheet 0)
            try:
                df = pd.read_excel(excel_path, sheet_name="ALL_COMPANIES")
            except Exception:
                df = pd.read_excel(excel_path, sheet_name=0)

            _logger.info("Read %d rows from Excel", len(df))
            inserted = upsert_dataframe_to_db(df, db_path)
            _logger.info("Upserted %d person-holding rows into DB", inserted)
        except Exception as exc:
            _logger.exception("Failed to load Excel: %s", exc)
            return 1

    _logger.info("Migration complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
