"""
src/worker/tasks.py
-------------------
Background task functions enqueued via Python-RQ (or the threading fallback).

Each function must be:
  - importable as a module-level symbol (required by RQ)
  - self-contained (accepts primitive arg types only for RQ serialisation)

Progress updates are pushed via src.worker.sse_stream.push_event() so the
/api/stream/<job_id> SSE endpoint can relay them to the browser.

NOTE: job_id is always the FIRST argument. task_queue.submit_job() injects
it automatically — callers do NOT pass it manually.

Public functions
----------------
run_download_pipeline(job_id, companies, source)
run_parse_pipeline(job_id, pdf_paths)
run_full_pipeline(job_id, companies, source)
run_upload_pipeline(job_id, pdf_paths)
"""
from __future__ import annotations

import glob as _glob
import logging
import os
import traceback
from pathlib import Path
from typing import Any

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: emit a progress event and update the job's RQ meta
# ---------------------------------------------------------------------------

def _emit(job_id: str, step: str, pct: int, message: str = "", status: str = "running") -> None:
    """Push an SSE event and update the RQ job meta (if RQ is active)."""
    from src.worker.sse_stream import push_event
    from src.worker.task_queue import update_job_meta

    payload: dict[str, Any] = {
        "step": step,
        "pct": pct,
        "message": message,
        "status": status,
    }
    if job_id:
        push_event(job_id, payload)
        update_job_meta(job_id, {"progress": pct, "step": step, "message": message})
    _logger.info("[job=%s] %s (%d%%) %s", job_id, step, pct, message)


# ---------------------------------------------------------------------------
# Task 1: Download pipeline
# ---------------------------------------------------------------------------

def run_download_pipeline(
    job_id: str,
    companies: list[str],
    source: str = "both",
) -> dict[str, Any]:
    """Search BSE/NSE and download PDFs for *companies*.

    Args:
        job_id:    The job's ID (injected by submit_job), used to emit SSE events.
        companies: List of company name strings.
        source:    "bse", "nse", or "both".

    Returns:
        A dict ``{company_name: {found, downloaded, failed}}`` from the downloader.
    """
    try:
        _emit(job_id, "Starting download", 0, f"Preparing to search {len(companies)} company(ies)")

        from src.downloader.auto_downloader import download_pdfs

        total = len(companies)
        results: dict[str, Any] = {}

        for idx, company in enumerate(companies, start=1):
            pct = int((idx - 1) / total * 60)   # download phase = 0–60%
            _emit(
                job_id,
                f"Searching {company}",
                pct,
                f"Company {idx}/{total}: looking up {company} on {source.upper()}",
            )

            partial = download_pdfs([company], source=source)
            results.update(partial)

            downloaded = partial.get(company, {}).get("downloaded", 0)
            failed = partial.get(company, {}).get("failed", 0)
            _emit(
                job_id,
                f"Downloaded {company}",
                int(idx / total * 60),
                f"{downloaded} PDF(s) saved, {failed} failed",
            )

        _emit(job_id, "Download complete", 60, f"All {total} companies processed")
        return results

    except Exception as exc:  # noqa: BLE001
        _logger.exception("Download pipeline failed for job %s", job_id)
        _emit(job_id, "Download failed", 0, str(exc), status="error")
        raise


# ---------------------------------------------------------------------------
# Task 2: Parse pipeline
# ---------------------------------------------------------------------------

def run_parse_pipeline(
    job_id: str,
    pdf_paths: list[str],
) -> dict[str, Any]:
    """Parse a list of PDFs, merge into master Excel, and deduplicate.

    Args:
        job_id:    The job's ID.
        pdf_paths: Absolute paths to PDF files to process.

    Returns:
        Dict with ``{records_extracted, records_after_dedup, output_path}``.
    """
    import concurrent.futures as _cf
    import threading
    import pandas as pd
    from src.parser.pdf_parser import parse_pdf

    try:
        total = len(pdf_paths)
        _emit(job_id, "Starting parse", 60, f"Processing {total} PDF(s)")
        all_dfs = []

        for idx, pdf_path in enumerate(pdf_paths, start=1):
            pct_start = 60 + int((idx - 1) / total * 30)   # 60–90%
            pct_end   = 60 + int(idx / total * 30)
            pdf_name  = Path(pdf_path).name
            _emit(job_id, f"Parsing PDF {idx}/{total}", pct_start, pdf_name)

            # -----------------------------------------------------------------
            # Run parse_pdf in a worker thread; emit heartbeat ticks in the
            # main thread so the browser progress bar keeps moving.  This is
            # important for large IEPF PDFs (16 pp) that take 30–120 s even
            # without OCR.
            # -----------------------------------------------------------------
            df_holder: list[Any] = []
            exc_holder: list[Exception] = []
            done_evt = threading.Event()

            def _parse_worker(_path: str = pdf_path) -> None:
                try:
                    result = parse_pdf(_path, skip_excel_write=True, enable_ocr=False)
                    df_holder.append(result)
                except Exception as _exc:  # noqa: BLE001
                    exc_holder.append(_exc)
                finally:
                    done_evt.set()

            worker = threading.Thread(target=_parse_worker, daemon=True)
            worker.start()

            # Heartbeat: nudge the progress bar every 4 s while parse runs
            tick = 0
            timeout_s = 180
            elapsed = 0
            while not done_evt.wait(timeout=4):
                elapsed += 4
                if elapsed >= timeout_s:
                    _logger.warning("parse_pdf timed out for %s — skipping", pdf_name)
                    break
                tick_pct = min(pct_start + int((pct_end - pct_start) * elapsed / timeout_s), pct_end - 1)
                _emit(job_id, f"Parsing PDF {idx}/{total}", tick_pct, f"{pdf_name} ({elapsed}s…)")

            worker.join(timeout=2)  # give worker a moment to finish cleanly

            if exc_holder:
                _logger.warning("Skipping %s: %s", pdf_name, exc_holder[0])
            elif df_holder and df_holder[0] is not None and not df_holder[0].empty:
                all_dfs.append(df_holder[0])
            else:
                _logger.warning("No data extracted from %s", pdf_name)

        if not all_dfs:
            _emit(job_id, "No data extracted", 90, "Zero records found in PDFs", status="error")
            return {"records_extracted": 0, "records_after_dedup": 0}

        merged_df = pd.concat(all_dfs, ignore_index=True)
        records_extracted = len(merged_df)

        deduped_df = merged_df.drop_duplicates()
        records_after_dedup = len(deduped_df)

        _emit(job_id, "Saving output", 95, "Writing master Excel file")
        output_path = _save_output(deduped_df)

        _emit(
            job_id,
            "Parse complete",
            100,
            f"Extracted {records_extracted} → deduplicated to {records_after_dedup} records",
            status="done",
        )

        return {
            "records_extracted": records_extracted,
            "records_after_dedup": records_after_dedup,
            "output_path": str(output_path),
        }

    except Exception as exc:  # noqa: BLE001
        _logger.exception("Parse pipeline failed for job %s", job_id)
        _emit(job_id, "Parse failed", 0, str(exc), status="error")
        raise


def _save_output(df) -> Path:
    """Write *df* to the canonical master Excel path and return it."""
    import pandas as pd

    root = Path(__file__).resolve().parents[2]
    out_dir = root / "data" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "master_shareholders.xlsx"
    df.to_excel(out_path, index=False)
    return out_path


# ---------------------------------------------------------------------------
# Task 3: Full end-to-end pipeline
# ---------------------------------------------------------------------------

def run_full_pipeline(
    job_id: str,
    companies: list[str],
    source: str = "both",
) -> dict[str, Any]:
    """Chain: download → parse → dedup for each company.

    This is the function called by the "Search and Download" button on
    the Data Collection page.  It is safe to enqueue with RQ.
    """
    try:
        _emit(job_id, "Pipeline starting", 0, f"{len(companies)} company(ies) queued")

        # Step 1: download
        download_results = run_download_pipeline(job_id, companies, source)

        # Collect all PDFs that were downloaded
        root = Path(__file__).resolve().parents[2]
        input_dir = root / "data" / "input"
        pdf_paths = [str(p) for p in input_dir.rglob("*.pdf")]

        if not pdf_paths:
            _emit(job_id, "No PDFs found", 62, "Nothing downloaded to parse", status="error")
            return {**download_results, "records_extracted": 0, "records_after_dedup": 0}

        # Step 2: parse + dedup
        parse_results = run_parse_pipeline(job_id, pdf_paths)

        return {**download_results, **parse_results}

    except Exception as exc:  # noqa: BLE001
        _logger.exception("Full pipeline failed for job %s", job_id)
        _emit(job_id, "Pipeline failed", 0, str(exc), status="error")
        raise


# ---------------------------------------------------------------------------
# Task 4: Manual upload pipeline (for uploaded PDFs)
# ---------------------------------------------------------------------------

def run_upload_pipeline(
    job_id: str,
    pdf_paths: list[str],
) -> dict[str, Any]:
    """Process manually uploaded PDFs — parse & dedup only, no download step.

    Args:
        job_id:    The job's ID (injected by submit_job).
        pdf_paths: Absolute paths to uploaded PDF files.

    Returns:
        Same shape as run_parse_pipeline.
    """
    _emit(job_id, "Upload pipeline starting", 0, f"{len(pdf_paths)} PDF(s) to process")
    return run_parse_pipeline(job_id, pdf_paths)
