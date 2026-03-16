"""
src/downloader/__main__.py
---------------------------
CLI entry point for the downloader module.

Usage
-----
Auto-download mode::

    python -m src.downloader --mode auto --companies "Tech Mahindra,Reliance,TCS"
    python -m src.downloader --mode auto --companies "TCS" --source bse

Manual upload mode::

    python -m src.downloader --mode manual --path /path/to/pdfs/
    python -m src.downloader --mode manual --path /path/to/file.pdf --no-interactive

Show progress status::

    python -m src.downloader --status
"""

from __future__ import annotations

import argparse
import json
import sys

from .logger import get_logger
from .progress import load_status

_logger = get_logger("downloader.cli")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.downloader",
        description=(
            "shareholder-pipeline downloader — "
            "auto-download or manually upload IEPF/shareholding PDFs."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-download for specific companies:
  python -m src.downloader --mode auto --companies "Tech Mahindra,TCS"

  # Auto-download from BSE only:
  python -m src.downloader --mode auto --companies "Reliance" --source bse

  # Manual upload from a folder:
  python -m src.downloader --mode manual --path /path/to/pdfs/

  # Manual upload without interactive prompts (batch mode):
  python -m src.downloader --mode manual --path /path/to/pdfs/ --no-interactive

  # Show current progress counters:
  python -m src.downloader --status
""",
    )

    parser.add_argument(
        "--mode",
        choices=["auto", "manual"],
        help="Operation mode: 'auto' (BSE/NSE discovery) or 'manual' (local file upload).",
    )
    parser.add_argument(
        "--companies",
        type=str,
        default="",
        metavar="NAMES",
        help=(
            "[auto mode] Comma-separated list of company names to search for. "
            'Example: "Tech Mahindra,Reliance,TCS"'
        ),
    )
    parser.add_argument(
        "--source",
        choices=["bse", "nse", "both"],
        default="both",
        help="[auto mode] Which exchange(s) to search (default: both).",
    )
    parser.add_argument(
        "--path",
        type=str,
        default="",
        metavar="PATH",
        help="[manual mode] Path to a PDF file or folder containing PDFs.",
    )
    parser.add_argument(
        "--no-interactive",
        action="store_true",
        default=False,
        help=(
            "[manual mode] Disable interactive prompts; mark undetectable "
            "company/year as 'unknown'."
        ),
    )
    parser.add_argument(
        "--config",
        type=str,
        default="",
        metavar="PATH",
        help="Override path to sources.yaml (default: config/sources.yaml).",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        default=False,
        help="Print the current progress status JSON and exit.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]``).

    Returns:
        Exit code (0 = success, non-zero = error).
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    # --status flag — show progress and exit
    if args.status:
        status = load_status()
        print(json.dumps(status, indent=2))
        return 0

    if not args.mode:
        parser.print_help()
        print("\nerror: --mode is required (use 'auto' or 'manual')", file=sys.stderr)
        return 1

    config_path: str | None = args.config or None

    # ------------------------------------------------------------------ #
    #  AUTO MODE                                                           #
    # ------------------------------------------------------------------ #
    if args.mode == "auto":
        if not args.companies:
            print(
                "error: --companies is required in auto mode.\n"
                'Example: --companies "Tech Mahindra,TCS"',
                file=sys.stderr,
            )
            return 1

        companies = [c.strip() for c in args.companies.split(",") if c.strip()]
        _logger.info(
            "Auto mode | companies=%s | source=%s", companies, args.source
        )

        from .auto_downloader import download_pdfs  # noqa: PLC0415

        results = download_pdfs(
            companies=companies, source=args.source, config_path=config_path
        )

        # Summary table
        print("\n" + "=" * 55)
        print(f"{'Company':<30} {'Found':>6} {'OK':>6} {'Failed':>6}")
        print("-" * 55)
        for company, counts in results.items():
            print(
                f"{company:<30} {counts['found']:>6} "
                f"{counts['downloaded']:>6} {counts['failed']:>6}"
            )
        print("=" * 55)
        return 0

    # ------------------------------------------------------------------ #
    #  MANUAL MODE                                                         #
    # ------------------------------------------------------------------ #
    if args.mode == "manual":
        if not args.path:
            print(
                "error: --path is required in manual mode.\n"
                "Example: --path /path/to/pdfs/",
                file=sys.stderr,
            )
            return 1

        interactive = not args.no_interactive
        _logger.info(
            "Manual mode | path=%s | interactive=%s", args.path, interactive
        )

        from .manual_uploader import upload_pdfs  # noqa: PLC0415

        saved = upload_pdfs(
            path=args.path,
            interactive=interactive,
            config_path=config_path,
        )

        print(f"\nSuccessfully uploaded {len(saved)} PDF(s):")
        for p in saved:
            print(f"  {p}")
        return 0

    # Should never reach here
    return 1


if __name__ == "__main__":
    sys.exit(main())
