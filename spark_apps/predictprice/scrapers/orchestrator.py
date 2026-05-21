"""Run buyee lister + detail workers (buyee.jp style) → raw Mongo."""

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


def _targets_for_sources(sources: list[str]) -> list[Target]:
    out: list[Target] = []
    for source_name in sources:
        if source_name not in config.SOURCE_TO_PLATFORM:
            raise ValueError(f"Unknown source: {source_name}")
        platform = config.SOURCE_TO_PLATFORM[source_name]
        category_id = config.category_id_for_source(source_name)
        out.append(Target(platform=platform, category_id=category_id))
    return out


def run_pipeline(
    sources: list[str],
    *,
    max_pages: int | None = None,
    mongo_uri: str | None = None,
    no_lister: bool = False,
    worker_count: int | None = None,
    warm_waf: bool = True,
) -> str:
    """
    Buyee.jp-style scrape: one proxy warm-up, N workers (capped by # keys),
    one lister for all targets → raw Mongo per source.
    """
    if not sources:
        return "ERROR: no sources specified"

    try:
        targets = _targets_for_sources(sources)
    except ValueError as exc:
        return f"ERROR: {exc}"

    uri = mongo_uri or config.WORKER_MONGO_URI
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s",
    )

    n_workers = worker_count or config.PIPELINE_NUM_WORKERS
    if n_workers < config.PIPELINE_MIN_WORKERS:
        n_workers = config.PIPELINE_MIN_WORKERS

    assignments: ProxyAssignments | None = None
    if config.USE_PROXY:
        keys = load_keys()
        if not keys:
            return (
                f"ERROR: no proxy keys "
                f"(PROXY_KEYS_FILE={config.PROXY_KEYS_FILE})"
            )
        assignments = ProxyAssignments(keys)
        if n_workers > len(assignments):
            log.warning(
                "worker_count=%s > %s key(s) — capping to %s",
                n_workers,
                len(assignments),
                len(assignments),
            )
            n_workers = len(assignments)

        log.info("Warming up %s proxy key(s)...", len(assignments))
        ok = assignments.warm_up_all()
        log.info("Proxy warm-up: %s/%s keys returned a proxy", ok, len(assignments))
        if ok == 0:
            return "ERROR: proxy warm-up failed (0 keys)"

        if warm_waf and config.buyee_settings.BUYEE_WAF_WARMUP:
            log.info("Harvesting WAF cookies for %s worker slot(s)...", n_workers)
            for i in range(n_workers):
                wp = assignments.for_worker(i)
                bundle = wp.refresh_waf(force=True)
                if bundle is None or bundle.is_empty():
                    log.warning("[worker-%s] WAF warm-up failed; will retry lazily", i)
                else:
                    log.info(
                        "[worker-%s] WAF cookie ready (proxy=%s)",
                        i,
                        bundle.proxy_endpoint,
                    )

    config.PIPELINE_NUM_WORKERS = n_workers
    log.info(
        "Pipeline start: workers=%s proxy=%s targets=%s",
        n_workers,
        assignments is not None,
        [str(t) for t in targets],
    )

    if config.MONGO_ENABLED:
        from scrapers import mongo_sink

        mongo_sink.ensure_indexes(uri)

    stop_event = threading.Event()
    lister_done = threading.Event()
    q: queue.Queue = queue.Queue(maxsize=10_000)

    mongo_writers: dict[str, MongoBatchWriter] = {}
    if config.MONGO_ENABLED:
        for src in sources:
            mongo_writers[src] = MongoBatchWriter(src, uri)

    feeder = feed_queue(sources, q, stop_event, lister_done, n_workers)

    workers: list[WorkerThread] = []
    for i in range(n_workers):
        wp = assignments.for_worker(i) if assignments else None
        if wp:
            wp.owner = f"worker-{i}"
        workers.append(
            WorkerThread(i, q, wp, stop_event, mongo_writers=mongo_writers)
        )
    for w in workers:
        w.start()

    lister: ListerThread | None = None
    if not no_lister and targets:
        lister_proxy = assignments.for_worker(0) if assignments else None
        if lister_proxy:
            lister_proxy.owner = "lister"
        lister = ListerThread(
            targets, lister_proxy, stop_event, max_pages=max_pages
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

    mongo_stats: dict[str, dict] = {}
    saved_by_source: dict[str, int] = {}
    for src, writer in mongo_writers.items():
        mongo_stats[src] = writer.flush()
        saved_by_source[src] = writer.total_saved

    processed = sum(w.processed for w in workers)
    failed = sum(w.failed for w in workers)
    saved_total = sum(saved_by_source.values())

    if processed == 0 and not no_lister:
        return (
            f"WARNING: No items processed "
            f"(workers={n_workers} keys={len(assignments) if assignments else 0} "
            f"targets={len(targets)})"
        )

    per_source = []
    for src in sources:
        per_source.append(f"{src}: mongo_saved={saved_by_source.get(src, 0)}")

    if config.MONGO_ENABLED and saved_total == 0 and processed > 0:
        stages = {
            src: str(mongo_stats.get(src, {}).get("stage") or "") for src in sources
        }
        return (
            f"WARN: processed={processed} failed={failed} mongo_inserted=0 "
            f"stages={stages} | mongo={ingestion.redact_mongo_uri(uri)}"
        )

    return (
        f"SUCCESS: processed={processed} failed={failed} "
        f"mongo_inserted={saved_total} | "
        f"mongo={ingestion.redact_mongo_uri(uri)} | "
        + "; ".join(per_source)
    )
