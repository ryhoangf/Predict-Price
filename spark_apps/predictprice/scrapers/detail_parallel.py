"""Gọi fetch theo URL song song — mỗi index gắn WorkerProxy riêng (buyee.jp)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, TypeVar

import config as cfg

T = TypeVar("T")
R = TypeVar("R")


def map_urls_parallel(
    urls: List[T],
    fn: Callable[[T], R],
    max_workers: int,
) -> List[R]:
    if not urls:
        return []

    cfg.ensure_buyee_http_pool()
    pa = cfg.get_proxy_assignments()
    workers = max(1, min(max(1, max_workers), len(urls)))
    if pa:
        workers = min(workers, len(pa))

    def _run(args):
        idx, url = args
        if pa:
            wp = cfg.proxy_for_thread_index(idx)
            cfg.bind_thread_worker_proxy(wp)
        else:
            cfg.bind_thread_worker_proxy(None)
        return fn(url)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(_run, enumerate(urls)))
