"""Buyee.jp-style HTTP stack: per-key proxy pool, WAF solver, curl_cffi fetch."""

from buyee_http.http_client import FetchError, fetch, fetch_response
from buyee_http.proxy_manager import ProxyAssignments, WorkerProxy, load_keys
from buyee_http.thread_context import (
    bind_thread_worker_proxy,
    get_thread_worker_proxy,
)

__all__ = [
    "FetchError",
    "ProxyAssignments",
    "WorkerProxy",
    "bind_thread_worker_proxy",
    "fetch",
    "fetch_response",
    "get_thread_worker_proxy",
    "load_keys",
]
