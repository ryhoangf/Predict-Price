"""Thread-local WorkerProxy binding for detail parallel fetch."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from buyee_http.proxy_manager import WorkerProxy

_tls = threading.local()


def bind_thread_worker_proxy(wp: "WorkerProxy | None") -> None:
    _tls.worker_proxy = wp


def get_thread_worker_proxy() -> "WorkerProxy | None":
    return getattr(_tls, "worker_proxy", None)
