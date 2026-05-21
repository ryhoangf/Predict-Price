"""Paginated lister — queues product IDs to per-source pending.txt."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

import config
from buyee_http.http_client import FetchError, fetch_response
from scrapers import pipeline_state
from scrapers.buyee_parser import parse_search_page
from buyee_http.proxy_manager import WorkerProxy

log = logging.getLogger("lister")


@dataclass
class Target:
    platform: str
    category_id: str

    def __str__(self) -> str:
        return f"{self.platform}:{self.category_id}"


def _source_for_platform(platform: str) -> str:
    return config.PLATFORM_TO_SOURCE.get(platform, platform)


class ListerThread(threading.Thread):
    def __init__(
        self,
        targets: list[Target],
        worker_proxy: WorkerProxy | None,
        stop_event: threading.Event,
        max_pages: int | None = None,
    ):
        super().__init__(name="lister", daemon=True)
        self.targets = targets
        self.worker_proxy = worker_proxy
        self.stop_event = stop_event
        self.max_pages = max_pages
        self.discovered_total = 0

    def _state_key(self, t: Target) -> str:
        return f"{t.platform}:{t.category_id}"

    def _page_cap(self, t: Target) -> int | None:
        if self.max_pages is not None:
            return self.max_pages
        return config.max_pages_for_platform(t.platform)

    def _crawl_target(self, t: Target) -> None:
        source_name = _source_for_platform(t.platform)
        spec = config.PLATFORMS[t.platform]
        key = self._state_key(t)
        page_cap = self._page_cap(t)
        s = pipeline_state.load_lister_state(source_name)
        s.setdefault(key, {})
        page = int(s[key].get("next_page", 1))
        last_page: int | None = s[key].get("last_page")

        log.info("Lister[%s] start at page %s (max_pages=%s)", key, page, page_cap or "all")

        while not self.stop_event.is_set():
            if page_cap and page > page_cap:
                log.info("Lister[%s] reached max_pages=%s", key, page_cap)
                break
            if last_page and page > last_page:
                log.info("Lister[%s] reached final page %s", key, last_page)
                break

            url = spec.search_url_template.format(cat=t.category_id, page=page)
            ref_key = {
                "mercari": "mercari",
                "rakuma": "rakuma",
                "jdirectitems": "yahoo",
            }.get(t.platform, "mercari")
            hdr = config.buyee_page_headers(referer=config.REFERERS[ref_key])
            try:
                _status, html = fetch_response(
                    url, self.worker_proxy, extra_headers=hdr
                )
            except FetchError as exc:
                log.error(
                    "Lister[%s] failed page %s: %s — sleeping 30s", key, page, exc
                )
                if self.stop_event.wait(30):
                    return
                continue

            ids, lp = parse_search_page(html, t.platform)
            if lp and (last_page is None or lp > last_page):
                last_page = lp

            display_last = last_page or "?"
            if page_cap and last_page and last_page > page_cap:
                display_last = page_cap

            entries = [(t.platform, t.category_id, page, pid) for pid in ids]
            added = pipeline_state.append_pending(source_name, entries)
            self.discovered_total += added
            log.info(
                "Lister[%s] page %s/%s -> %s ids (new=%s, total=%s)",
                key,
                page,
                display_last,
                len(ids),
                added,
                self.discovered_total,
            )

            next_page = page if not ids else page + 1
            s[key] = {"next_page": next_page, "last_page": last_page}
            pipeline_state.save_lister_state(source_name, s)

            if not ids:
                log.info("Lister[%s] no items on page %s — assuming end.", key, page)
                break

            if page_cap and page >= page_cap:
                log.info("Lister[%s] finished max_pages=%s", key, page_cap)
                break

            page += 1
            if self.stop_event.wait(config.LISTING_DELAY_SEC):
                return

    def run(self) -> None:
        log.info(
            "Lister start: %d target(s): %s",
            len(self.targets),
            [str(t) for t in self.targets],
        )
        for t in self.targets:
            if self.stop_event.is_set():
                break
            try:
                self._crawl_target(t)
            except Exception:  # noqa: BLE001
                log.exception("Lister[%s] unhandled error", self._state_key(t))
        log.info("Lister exited. Total new IDs queued: %s", self.discovered_total)
