"""Two-phase scrape: listing DOM → pending file → parallel detail fetch."""

from __future__ import annotations

import time
from typing import Callable

import pandas as pd

import config as cfg
from scrapers.detail_parallel import map_urls_parallel
from scrapers import scrape_state


def run_two_phase_scrape(
    source: str,
    listing_fn: Callable[[], pd.DataFrame],
    detail_fn: Callable[[str], tuple | None],
    *,
    resume: bool = True,
) -> pd.DataFrame:
    """
    Phase 1: listing_fn() → save listing.csv + pending_links.txt
    Phase 2: drain pending via detail_fn(url) with per-thread WorkerProxy
    """
    cfg.ensure_buyee_http_pool()

    pending = scrape_state.load_pending_links(source) if resume else []
    if pending:
        print(f"   [{source}] Resuming {len(pending)} pending link(s) from state — skip listing")
        df = scrape_state.load_listing_df(source)
        if df is None or df.empty:
            df = pd.DataFrame({"link": pending})
    else:
        if not resume:
            scrape_state.wipe_listing_state(source)
        df = listing_fn()
        if df is None or df.empty:
            return df
        scrape_state.save_listing_df(source, df)
        links = df["link"].dropna().astype(str).tolist()
        added = scrape_state.append_pending_links(source, links)
        print(f"   [{source}] Listing saved; queued {added} link(s) for detail")
        pending = scrape_state.load_pending_links(source)

    if not pending:
        return df

    print(
        f"   → [{source}] Fetching details for {len(pending)} items "
        f"(workers={cfg.DETAIL_FETCH_MAX_WORKERS})..."
    )

    def _one(url: str):
        out = cfg.safe_fetch_with_retry(
            detail_fn,
            url,
            max_retries=2,
            invalidate_proxy_on_retry=True,
        )
        if out is not None:
            scrape_state.mark_link_finished(source, url)
        if cfg.WORKER_DELAY_SEC:
            time.sleep(cfg.WORKER_DELAY_SEC)
        return out

    t0 = time.perf_counter()
    pairs = map_urls_parallel(pending, _one, cfg.DETAIL_FETCH_MAX_WORKERS)
    print(f"   [{source}] [timing] detail_fetch={time.perf_counter() - t0:.1f}s")

    link_to_pair = dict(zip(pending, pairs))
    if "condition" not in df.columns:
        df["condition"] = None
    if "explanation" not in df.columns:
        df["explanation"] = None

    for i, row in df.iterrows():
        link = row.get("link")
        if link is None or (isinstance(link, float) and pd.isna(link)):
            continue
        link = str(link).strip()
        p = link_to_pair.get(link)
        if p is None:
            continue
        if isinstance(p, (tuple, list)) and len(p) >= 2:
            df.at[i, "condition"] = p[0]
            df.at[i, "explanation"] = p[1]

    return df
