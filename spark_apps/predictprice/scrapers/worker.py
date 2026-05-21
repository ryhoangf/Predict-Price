"""Detail workers — fetch item pages, batch persist to Mongo (buyee.jp style)."""

from __future__ import annotations

import logging
import queue
import threading
import time

import config
from buyee_http.http_client import fetch
from scrapers import pipeline_state
from scrapers.buyee_parser import parse_item_page
from buyee_http.proxy_manager import WorkerProxy

log = logging.getLogger("worker")

SENTINEL = ("__STOP__", "", -1, "")


def feed_queue(
    sources: list[str],
    q: "queue.Queue",
    stop_event: threading.Event,
    lister_done: threading.Event,
    worker_count: int,
) -> threading.Thread:
    seen: set[str] = set()
    for src in sources:
        seen.update(pipeline_state.load_finished(src))

    def loop() -> None:
        while not stop_event.is_set():
            for src in sources:
                for entry in pipeline_state.load_pending(src):
                    pid = entry[3]
                    if pid not in seen:
                        seen.add(pid)
                        q.put(entry)
            if lister_done.is_set():
                for _ in range(worker_count):
                    q.put(SENTINEL)
                return
            if stop_event.wait(2.0):
                return

    t = threading.Thread(target=loop, name="feeder", daemon=True)
    t.start()
    return t


class WorkerThread(threading.Thread):
    def __init__(
        self,
        idx: int,
        q: "queue.Queue",
        worker_proxy: WorkerProxy | None,
        stop_event: threading.Event,
        mongo_writers: dict[str, "MongoBatchWriter"] | None = None,
    ):
        super().__init__(name=f"worker-{idx}", daemon=True)
        self.q = q
        self.worker_proxy = worker_proxy
        self.stop_event = stop_event
        self.mongo_writers = mongo_writers or {}
        self.processed = 0
        self.failed = 0

    def run(self) -> None:
        log.info("[%s] started", self.name)
        while not self.stop_event.is_set():
            try:
                item = self.q.get(timeout=5)
            except queue.Empty:
                continue

            if item == SENTINEL:
                self.q.task_done()
                break

            platform, category_id, page, pid = item
            try:
                self._process(platform, category_id, page, pid)
                self.processed += 1
            except Exception as exc:  # noqa: BLE001
                self.failed += 1
                log.exception(
                    "[%s] %s/%s failed: %s", self.name, platform, pid, exc
                )
            finally:
                self.q.task_done()
                if config.WORKER_DELAY_SEC:
                    time.sleep(config.WORKER_DELAY_SEC)

        log.info(
            "[%s] done. processed=%s failed=%s",
            self.name,
            self.processed,
            self.failed,
        )

    def _source_name(self, platform: str) -> str:
        return config.PLATFORM_TO_SOURCE.get(platform, platform)

    def _process(self, platform: str, category_id: str, page: int, pid: str) -> None:
        source_name = self._source_name(platform)
        spec = config.PLATFORMS[platform]
        url = spec.item_url_template.format(product_id=pid)
        ref_key = {
            "mercari": "mercari",
            "rakuma": "rakuma",
            "jdirectitems": "yahoo",
        }.get(platform, "mercari")
        referer = config.REFERERS.get(ref_key, config.REFERERS["mercari"])
        hdr = config.buyee_page_headers(referer=referer)

        html = fetch(url, self.worker_proxy, extra_headers=hdr)

        def fetcher(follow_url: str) -> str:
            return fetch(
                follow_url,
                self.worker_proxy,
                extra_headers=config.buyee_page_headers(referer=url),
            )

        row = parse_item_page(
            html,
            pid,
            platform,
            fetcher=fetcher if spec.description_url_template else None,
            category_id=category_id,
        )

        if config.CSV_ENABLED:
            pipeline_state.append_csv(source_name, row)

        writer = self.mongo_writers.get(source_name)
        if writer is not None:
            writer.add(pid, row)
        else:
            pipeline_state.mark_finished(source_name, pid)
            pipeline_state.remove_pending(source_name, pid)

        log.info(
            "[%s] OK %s/%s page=%s name=%r",
            self.name,
            platform,
            pid,
            page,
            (row.get("name") or "")[:60],
        )


class MongoBatchWriter:
    """Thread-safe buffer; flush inserts raw rows (NLP on Spark)."""

    BATCH_SIZE = 25

    def __init__(self, source_name: str, mongo_uri: str | None = None):
        self.source_name = source_name
        self.mongo_uri = mongo_uri
        self._buffer: list[tuple[str, dict]] = []
        self._lock = threading.Lock()
        self.total_saved = 0
        self.last_stats: dict = {}

    def add(self, pid: str, row: dict) -> None:
        with self._lock:
            self._buffer.append((pid, row))
            if len(self._buffer) >= self.BATCH_SIZE:
                self._flush_unlocked()

    def flush(self) -> dict:
        with self._lock:
            return self._flush_unlocked()

    def _flush_unlocked(self) -> dict:
        if not self._buffer:
            return self.last_stats
        from scrapers import mongo_sink

        batch = self._buffer
        self._buffer = []
        pids = [pid for pid, _ in batch]
        rows = [row for _, row in batch]
        stats = mongo_sink.persist_rows(
            rows, self.source_name, self.mongo_uri, run_nlp=False
        )
        self.last_stats = stats
        saved = int(stats.get("saved") or 0)
        self.total_saved += saved

        stage = str(stats.get("stage") or "")
        if stage in ("inserted", "all_duplicates") or saved > 0:
            for pid in pids:
                pipeline_state.mark_finished(self.source_name, pid)
                pipeline_state.remove_pending(self.source_name, pid)
        elif stage in ("mongo_disabled", "empty_df"):
            for pid in pids:
                pipeline_state.mark_finished(self.source_name, pid)
                pipeline_state.remove_pending(self.source_name, pid)
        else:
            log.warning(
                "[%s] Mongo flush stage=%s saved=%s — pending kept for retry",
                self.source_name,
                stage,
                saved,
            )

        return stats
