"""
src/worker
----------
Background task queue using Python-RQ (Redis) with an in-memory fallback
for environments where Redis is unavailable.
"""
from .task_queue import submit_job, get_job_status, cancel_job
from .sse_stream import push_event, stream_events

__all__ = ["submit_job", "get_job_status", "cancel_job", "push_event", "stream_events"]
