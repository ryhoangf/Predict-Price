"""CLI entry — buyee lister/worker pipeline (raw Mongo ingest; NLP via Spark main.py).

Examples:
  python -m scrapers.run --sources mercari,rakuma,yahooauction
  python -m scrapers.run --sources mercari --max-pages 5
  python -m scrapers.run --sources mercari --no-lister --mongo-uri mongodb://...

After ingest, run Spark NLP: make submitmain (main.py).
"""

from __future__ import annotations

import argparse
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config
from scrapers import pipeline_state
from scrapers.orchestrator import run_pipeline_source


def main() -> int:
    ap = argparse.ArgumentParser(description="Predict Price buyee pipeline")
    ap.add_argument(
        "--sources",
        default="mercari,rakuma,yahooauction",
        help="comma-separated: mercari,rakuma,yahooauction",
    )
    ap.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="cap listing pages per source (0 = use config/env MAX_PAGES_*)",
    )
    ap.add_argument(
        "--mongo-uri",
        default="",
        help="override WORKER_MONGO_URI for this run",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=0,
        help=f"detail workers (default {config.PIPELINE_NUM_WORKERS})",
    )
    ap.add_argument(
        "--no-lister",
        action="store_true",
        help="only drain pending.txt (skip listing crawl)",
    )
    ap.add_argument(
        "--session",
        choices=("continue", "new", "reset"),
        default="continue",
        help="continue=keep state, new=wipe pending+lister_state, reset=all",
    )
    ap.add_argument(
        "--skip-waf-warmup",
        action="store_true",
        help="skip Playwright WAF warm-up at startup",
    )
    args = ap.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]

    if args.session == "new":
        for src in sources:
            pipeline_state.wipe_session_partial(src)
        print(f"Session: wiped pending + lister_state for {sources}")
    elif args.session == "reset":
        for src in sources:
            pipeline_state.wipe_session_full(src)
        print(f"Session: full reset for {sources}")

    max_pages = args.max_pages if args.max_pages > 0 else None
    mongo_uri = args.mongo_uri.strip() or None
    worker_count = args.workers if args.workers > 0 else None

    exit_code = 0
    for src in sources:
        print(f"\n=== Pipeline: {src} ===")
        key_idx = config.proxy_key_index_for_source(src)
        msg = run_pipeline_source(
            src,
            max_pages=max_pages,
            mongo_uri=mongo_uri,
            no_lister=args.no_lister,
            worker_count=worker_count,
            warm_waf=not args.skip_waf_warmup,
            proxy_key_index=key_idx,
        )
        print(msg)
        if msg.startswith("ERROR"):
            exit_code = 2
        elif msg.startswith("WARNING") and exit_code == 0:
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
