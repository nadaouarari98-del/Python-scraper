"""
src/worker/sse_stream.py
------------------------
Thread-safe Server-Sent Events (SSE) event store.

Architecture:
- A global dict maps job_id → deque of event dicts.
- Worker tasks call push_event() to broadcast progress updates.
- The Flask /api/stream/<job_id> route calls stream_events() which
  yields SSE-formatted strings, blocking until new events arrive.

SSE wire format per event:
    data: {"step": "Parsing PDF 3/10", "pct": 30, "status": "running"}

Usage:
    from src.worker.sse_stream import push_event, stream_events

    # In a worker task:
    push_event(job_id, {"step": "Downloading PDFs", "pct": 10, "status": "running"})

    # In a Flask route:
    return Response(stream_events(job_id), mimetype='text/event-stream')
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict, deque

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global state — thread-safe via Lock
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_event_store: dict[str, deque] = defaultdict(deque)
_conditions: dict[str, threading.Condition] = {}      # per-job condition vars
_closed_jobs: set[str] = set()                        # jobs that have finished streaming

_MAX_EVENTS_PER_JOB = 500      # cap to avoid memory leak
_SSE_HEARTBEAT_SECS = 15       # send a keepalive comment every N seconds


def _get_condition(job_id: str) -> threading.Condition:
    """Return (creating if needed) the Condition variable for *job_id*."""
    with _lock:
        if job_id not in _conditions:
            _conditions[job_id] = threading.Condition(threading.Lock())
        return _conditions[job_id]


# ---------------------------------------------------------------------------
# Push API (called by worker tasks)
# ---------------------------------------------------------------------------

def push_event(job_id: str, data: dict) -> None:
    """Push an event for *job_id* and wake up any waiting SSE consumers.

    Args:
        job_id: The RQ or threading job ID.
        data:   A dict with at minimum {"step": str, "pct": int, "status": str}.
                status should be "running", "done", or "error".
    """
    cond = _get_condition(job_id)
    with _lock:
        q = _event_store[job_id]
        if len(q) >= _MAX_EVENTS_PER_JOB:
            q.popleft()     # drop oldest to avoid unbounded growth
        q.append(data)

    with cond:
        cond.notify_all()

    _logger.debug("SSE push [%s]: %s", job_id, data)

    # Mark job as done if the event signals completion
    if data.get("status") in ("done", "error"):
        with _lock:
            _closed_jobs.add(job_id)


def close_stream(job_id: str) -> None:
    """Signal that no more events will be pushed for *job_id*."""
    with _lock:
        _closed_jobs.add(job_id)
    cond = _get_condition(job_id)
    with cond:
        cond.notify_all()


# ---------------------------------------------------------------------------
# Stream generator (consumed by Flask SSE route)
# ---------------------------------------------------------------------------

def stream_events(job_id: str, poll_timeout: float = 30.0):
    """Generator that yields SSE-formatted strings for *job_id*.

    Yields:
        Strings in SSE format: ``data: {...}\\n\\n``
        Periodically yields ``: heartbeat`` comment lines to keep the
        connection alive through proxies.

    The generator exits when:
    - The job is marked as done/error (status in ("done", "error")).
    - The client disconnects (GeneratorExit).
    """
    cond = _get_condition(job_id)
    last_sent_idx = 0

    try:
        while True:
            # Check for newly queued events
            with _lock:
                q = _event_store[job_id]
                events_to_send = list(q)[last_sent_idx:]
                is_closed = job_id in _closed_jobs

            for event in events_to_send:
                yield f"data: {json.dumps(event)}\n\n"
                last_sent_idx += 1
                # If this event is terminal, stop the stream
                if event.get("status") in ("done", "error"):
                    return

            if is_closed and not events_to_send:
                # Job finished and all events delivered
                return

            # Wait for new events or heartbeat timeout
            with cond:
                cond.wait(timeout=poll_timeout)

            # Send SSE heartbeat comment (keeps proxy connections alive)
            yield ": heartbeat\n\n"

    except GeneratorExit:
        _logger.debug("SSE client disconnected for job %s", job_id)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup_job(job_id: str) -> None:
    """Free memory for a completed job's event queue and condition."""
    with _lock:
        _event_store.pop(job_id, None)
        _closed_jobs.discard(job_id)
    with _lock:
        _conditions.pop(job_id, None)
