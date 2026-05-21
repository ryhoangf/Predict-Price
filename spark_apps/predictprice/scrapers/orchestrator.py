"""Run buyee lister + detail workers for one Spark source (mercari/rakuma/yahooauction)."""

from __future__ import annotations

import logging
import queue
import threading

import config
import ingestion
from buyee_http.proxy_manager import ProxyAssignments, load_keys
from scrapers.lister import ListerThread, Target
from scrapers.worker import MongoBatchWriter, WorkerThread, feed_queue

log = logging.getLogger("orchestrator")


def _cap_workers(requested: int, assignments: ProxyAssignments | None) -> int:
    n = max(config.PIPELINE_MIN_WORKERS, requested)
    if assignments is not None:
        n = min(n, len(assignments))
    return n


def run_pipeline_source(
    source_name: str,
    *,
    max_pages: int | None = None,
    mongo_uri: str | None = None,
    no_lister: bool = False,
    worker_count: int | None = None,
    warm_waf: bool = True,
) -> str:
    """
    Scrape one source: list (max_pages) → detail workers → NLP → Mongo.
    Returns status string for Spark driver (SUCCESS/WARNING/ERROR).
    """
    if source_name not in config.SOURCE_TO_PLATFORM:
        return f"ERROR: {source_name} - Unknown source"

    platform = config.SOURCE_TO_PLATFORM[source_name]
    category_id = config.category_id_for_source(source_name)
    page_cap = max_pages if max_pages is not None else config.max_pages_for_source(source_name)
    uri = mongo_uri or config.WORKER_MONGO_URI

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s",
    )

    config.ensure_buyee_http_pool(warm_waf=warm_waf)

    assignments: ProxyAssignments | None = None
    if config.USE_PROXY:
        keys = load_keys()
        if not keys:
            return (
                f"ERROR: {source_name} - no proxy keys "
                f"(PROXY_KEYS_FILE={config.PROXY_KEYS_FILE})"
            )
        assignments = ProxyAssignments(keys)
        ok = assignments.warm_up_all()
        if ok == 0:
            return f"ERROR: {source_name} - proxy warm-up failed"
        if warm_waf and config.buyee_settings.BUYEE_WAF_WARMUP:
            n_waf = min(config.DETAIL_FETCH_MAX_WORKERS, len(assignments))
            assignments.warm_waf_all(n_waf)

    n_workers = _cap_workers(
        worker_count or config.PIPELINE_NUM_WORKERS,
        assignments,
    )
    config.PIPELINE_NUM_WORKERS = n_workers

    if config.MONGO_ENABLED:
        from scrapers import mongo_sink

        mongo_sink.ensure_indexes(uri)

    targets = [Target(platform=platform, category_id=category_id)]
    stop_event = threading.Event()
    lister_done = threading.Event()
    q: queue.Queue = queue.Queue(maxsize=10_000)

    mongo_writer = (
        MongoBatchWriter(source_name, uri) if config.MONGO_ENABLED else None
    )

    feeder = feed_queue(source_name, q, stop_event, lister_done)

    workers: list[WorkerThread] = []
    for i in range(n_workers):
        wp = assignments.for_worker(i) if assignments else None
        if wp:
            wp.owner = f"worker-{i}"
        workers.append(
            WorkerThread(
                source_name, i, q, wp, stop_event, mongo_writer=mongo_writer
            )
        )
    for w in workers:
        w.start()

    lister: ListerThread | None = None
    if not no_lister:
        lister_proxy = assignments.for_worker(0) if assignments else None
        if lister_proxy:
            lister_proxy.owner = "lister"
        lister = ListerThread(
            source_name, targets, lister_proxy, stop_event, max_pages=page_cap
        )
        lister.start()

    try:
        if lister:
            while lister.is_alive():
                lister.join(timeout=1)
        lister_done.set()
        feeder.join()
        for w in workers:
            w.join()
    finally:
        stop_event.set()

    if mongo_writer is not None:
        stats = mongo_writer.flush()
    else:
        stats = {"saved": 0, "stage": "mongo_disabled"}

    processed = sum(w.processed for w in workers)
    failed = sum(w.failed for w in workers)
    saved = int(stats.get("saved") or 0)
    stage = str(stats.get("stage") or "")

    if processed == 0 and not no_lister:
        hint = (
            f"keys={len(config.PROXY_XOAY_KEYS)} "
            f"max_pages={page_cap} "
            f"last_fetch={config.last_fetch_error() or 'n/a'}"
        )
        return f"WARNING: {source_name} - No items processed ({hint})"

    if stage == "mongo_connection_failed":
        return f"ERROR: {source_name} - Mongo connection failed"

    if config.MONGO_ENABLED and saved == 0 and processed > 0:
        if stage in ("all_duplicates",):
            return (
                f"OK: {source_name} - processed={processed}, "
                f"mongo_inserted=0 (all duplicates)"
            )
        if stage not in ("mongo_disabled", "empty_df"):
            return (
                f"WARN: {source_name} - processed={processed}, "
                f"failed={failed}, mongo_inserted=0 stage={stage}"
            )

    return (
        f"SUCCESS: {source_name} - processed={processed}, failed={failed}, "
        f"mongo_inserted={saved}, stage={stage} | "
        f"mongo={ingestion.redact_mongo_uri(uri)}"
    )
