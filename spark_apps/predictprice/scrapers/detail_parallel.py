"""Gọi fetch theo URL song song, giữ thứ tự khớp danh sách đầu vào."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, TypeVar

T = TypeVar("T")
R = TypeVar("R")


def map_urls_parallel(
    urls: List[T],
    fn: Callable[[T], R],
    max_workers: int,
) -> List[R]:
    if not urls:
        return []
    workers = max(1, min(max(1, max_workers), len(urls)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(fn, urls))
