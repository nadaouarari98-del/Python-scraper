"""
src/parser/__main__.py
-----------------------
CLI entry point for the parser module.

Usage
-----
Parse a single PDF::

    python -m src.parser --input tests/sample_pdfs/TechMahindra_IEPF_2017-2018.pdf

Parse a whole folder::

    python -m src.parser --input data/input/

Custom output paths::

    python -m src.parser --input data/input/ --output data/output/parsed/ \\
        --master data/output/master_shareholder_data.xlsx

Disable OCR (faster, no tesseract needed)::

    python -m src.parser --input data/input/ --no-ocr

Show current progress status::

    python -m src.parser --status
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# On Windows, reconfigure stdout/stderr to UTF-8 if possible (Python 3.7+)
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from .progress import load_parser_status


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.parser",
        description=(
            "shareholder-pipeline parser — convert IEPF/shareholding PDFs "
            "into clean Excel DataFrames."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse a single PDF:
  python -m src.parser --input tests/sample_pdfs/TechMahindra_IEPF_2017-2018.pdf

  # Parse all PDFs in a folder:
  python -m src.parser --input data/input/

  # Disable OCR, custom output:
  python -m src.parser --input data/input/ --no-ocr --output data/output/parsed/

  # Show parsing progress:
  python -m src.parser --status
""",
    )
    p.add_argument(
        "--input", "-i",
        type=str,
        default="",
        metavar="PATH",
        help="Path to a single PDF file or a folder containing PDFs.",
    )
    p.add_argument(
        "--output", "-o",
        type=str,
        default="data/output/parsed/",
        metavar="DIR",
        help="Output directory for per-company Excel files (default: data/output/parsed/).",
    )
    p.add_argument(
        "--master", "-m",
        type=str,
        default="data/output/master_shareholder_data.xlsx",
        metavar="FILE",
        help="Path to the master Excel file (default: data/output/master_shareholder_data.xlsx).",
    )
    p.add_argument(
        "--no-ocr",
        action="store_true",
        default=False,
        help="Disable OCR fallback for scanned pages (faster, no tesseract needed).",
    )
    p.add_argument(
        "--status",
        action="store_true",
        default=False,
        help="Print current parser progress status JSON and exit.",
    )
    return p


def _setup_logging() -> None:
    """Configure root logger for CLI usage (console INFO + file DEBUG)."""
    import logging
    from src.downloader.logger import get_logger  # reuse the shared logger setup
    get_logger("parser")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]``).

    Returns:
        Exit code (0 = success, non-zero = error).
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    # --status
    if args.status:
        status = load_parser_status()
        print(json.dumps(status, indent=2))
        return 0

    if not args.input:
        parser.print_help()
        print("\nerror: --input is required.", file=sys.stderr)
        return 1

    _setup_logging()
    enable_ocr = not args.no_ocr
    input_path = Path(args.input)

    from .pdf_parser import parse_pdf, parse_all_pdfs  # noqa: PLC0415

    if input_path.is_file():
        # --- Single file mode ---
        print(f"Parsing single PDF: {input_path}")
        df = parse_pdf(
            str(input_path),
            output_dir=args.output,
            master_path=args.master,
            enable_ocr=enable_ocr,
        )
        if df.empty:
            print("[WARN] No records extracted. Check data/logs/parser_status.json for details.")
            return 1
        print(f"\n[OK] Extracted {len(df)} records.")
        print(f"   Individual Excel -> {args.output}")
        print(f"   Master Excel     -> {args.master}")
        return 0

    elif input_path.is_dir():
        # --- Folder mode ---
        print(f"Parsing all PDFs under: {input_path}")
        df = parse_all_pdfs(
            str(input_path),
            output_dir=args.output,
            master_path=args.master,
            enable_ocr=enable_ocr,
        )
        if df.empty:
            print("[WARN] No records extracted.")
            return 1
        print(f"\n[OK] Extracted {len(df)} total records from all PDFs.")
        print(f"   Individual Excel files -> {args.output}")
        print(f"   Master Excel           -> {args.master}")

        # Print summary table
        if "company_name" in df.columns and "year" in df.columns:
            summary = (
                df.groupby(["company_name", "year"])
                .size()
                .reset_index(name="records")
            )
            print(f"\n{'Company':<30} {'Year':<12} {'Records':>8}")
            print("-" * 53)
            for _, row in summary.iterrows():
                print(f"{row['company_name']:<30} {row['year']:<12} {row['records']:>8}")

        return 0

    else:
        print(f"error: '{args.input}' is not a valid file or directory.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
