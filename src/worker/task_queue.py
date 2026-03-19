"""
src/worker/task_queue.py
------------------------
Job submission and status management using Python-RQ + Redis.

Falls back to a ThreadPoolExecutor-based in-memory queue if Redis is
unavailable (e.g. Redis not started yet), so the app still boots.

Usage:
    from src.worker.task_queue import submit_job, get_job_status

    job_id = submit_job(run_download_pipeline, companies=["TCS"], source="both")
    status = get_job_status(job_id)
    # {"job_id": "...", "status": "queued|started|finished|failed", "meta": {...}}
"""
from __future__ import annotations

import logging
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Try RQ + Redis; fall back to threading pool
# ---------------------------------------------------------------------------
_USE_RQ = False
_rq_queue = None
_redis_conn = None

try:
    import redis as _redis_lib
    from rq import Queue as _RQQueue
    from rq.job import Job as _RQJob

    _redis_conn = _redis_lib.Redis(host="localhost", port=6379, db=0, socket_connect_timeout=2)
    _redis_conn.ping()          # raises if Redis is not running
    _rq_queue = _RQQueue("shareholder_pipeline", connection=_redis_conn, default_timeout=3600)
    _USE_RQ = True
    _logger.info("RQ + Redis connected — using distributed task queue")
except Exception as exc:
    _logger.warning("Redis unavailable (%s) — falling back to ThreadPoolExecutor", exc)

# Threading fallback
_thread_pool = ThreadPoolExecutor(max_workers=4)
_in_memory_jobs: dict[str, dict[str, Any]] = {}   # job_id -> status dict


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def submit_job(task_fn: Callable, *args: Any, **kwargs: Any) -> str:
    """Enqueue *task_fn* for background execution.

    The job_id is ALWAYS injected as the first positional argument to *task_fn*.
    Tasks should declare their signature as: fn(job_id: str, *your_args).

    Args:
        task_fn: A module-level callable (must be importable by the RQ worker).
        *args:   Additional positional arguments forwarded to *task_fn* after job_id.
        **kwargs: Keyword arguments forwarded to *task_fn*.

    Returns:
        A unique job ID string.
    """
    if _USE_RQ:
        # RQ auto-generates the job id; we pass it explicitly so the task
        # function receives it as its first argument via RQ's job.id mechanism.
        # We use a pre-generated UUID so we can return it synchronously.
        rq_job_id = str(uuid.uuid4())
        job = _rq_queue.enqueue(
            task_fn,
            rq_job_id,   # inject job_id as first arg
            *args,
            **kwargs,
            job_id=rq_job_id,
        )
        _logger.info("RQ job enqueued: %s → %s", task_fn.__name__, job.id)
        return job.id

    # Threading fallback
    job_id = str(uuid.uuid4())
    _in_memory_jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "meta": {"progress": 0, "message": "Job queued"},
        "func": task_fn.__name__,
    }

    def _run():
        _in_memory_jobs[job_id]["status"] = "started"
        try:
            result = task_fn(job_id, *args, **kwargs)   # inject job_id as first arg
            _in_memory_jobs[job_id]["status"] = "finished"
            _in_memory_jobs[job_id]["result"] = result
        except Exception as exc:  # noqa: BLE001
            _logger.exception("Background job %s failed: %s", job_id, exc)
            _in_memory_jobs[job_id]["status"] = "failed"
            _in_memory_jobs[job_id]["error"] = str(exc)

    future: Future = _thread_pool.submit(_run)
    _in_memory_jobs[job_id]["_future"] = future
    _logger.info("Thread job submitted: %s → %s", task_fn.__name__, job_id)
    return job_id


def get_job_status(job_id: str) -> dict[str, Any]:
    """Return the current status of a job.

    Returns:
        Dict with keys: job_id, status, meta, [result], [error].
        status is one of: queued, started, finished, failed, unknown.
    """
    if _USE_RQ:
        try:
            job = _RQJob.fetch(job_id, connection=_redis_conn)
            return {
                "job_id": job_id,
                "status": job.get_status().value,   # e.g. "queued", "started", "finished"
                "meta": job.meta or {},
                "result": str(job.result) if job.result is not None else None,
                "error": job.exc_info if job.exc_info else None,
            }
        except Exception as exc:
            _logger.warning("Could not fetch RQ job %s: %s", job_id, exc)
            return {"job_id": job_id, "status": "unknown", "meta": {}}

    # Threading fallback
    if job_id not in _in_memory_jobs:
        return {"job_id": job_id, "status": "unknown", "meta": {}}

    info = _in_memory_jobs[job_id]
    return {
        "job_id": job_id,
        "status": info["status"],
        "meta": info.get("meta", {}),
        "result": info.get("result"),
        "error": info.get("error"),
    }


def update_job_meta(job_id: str, meta: dict[str, Any]) -> None:
    """Update the metadata (progress, message) for a running job.

    For RQ jobs, writes to job.meta and saves. For threading, updates
    the in-memory dict directly.
    """
    if _USE_RQ:
        try:
            job = _RQJob.fetch(job_id, connection=_redis_conn)
            job.meta.update(meta)
            job.save_meta()
        except Exception as exc:
            _logger.warning("Could not update meta for RQ job %s: %s", job_id, exc)
        return

    if job_id in _in_memory_jobs:
        _in_memory_jobs[job_id].setdefault("meta", {}).update(meta)


def cancel_job(job_id: str) -> bool:
    """Attempt to cancel a job. Returns True if cancelled, False otherwise."""
    if _USE_RQ:
        try:
            job = _RQJob.fetch(job_id, connection=_redis_conn)
            job.cancel()
            return True
        except Exception:
            return False

    if job_id in _in_memory_jobs:
        future = _in_memory_jobs[job_id].get("_future")
        if future and not future.done():
            cancelled = future.cancel()
            if cancelled:
                _in_memory_jobs[job_id]["status"] = "cancelled"
            return cancelled
    return False
