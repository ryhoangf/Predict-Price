"""File-backed listing/detail state (buyee.jp pending/finished pattern for URLs)."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import pandas as pd

_lock = threading.Lock()
_STATE_ROOT = Path(
    os.getenv("SCRAPE_STATE_DIR", str(Path(__file__).resolve().parent.parent / "data" / "scrape_state"))
)


def _source_dir(source: str) -> Path:
    d = _STATE_ROOT / source.lower()
    d.mkdir(parents=True, exist_ok=True)
    return d


def listing_csv_path(source: str) -> Path:
    return _source_dir(source) / "listing.csv"


def pending_path(source: str) -> Path:
    return _source_dir(source) / "pending_links.txt"


def finished_path(source: str) -> Path:
    return _source_dir(source) / "finished_links.txt"


def lister_state_path(source: str) -> Path:
    return _source_dir(source) / "lister_state.json"


def load_finished_links(source: str) -> set[str]:
    p = finished_path(source)
    if not p.exists():
        return set()
    with p.open("r", encoding="utf-8") as f:
        return {ln.strip() for ln in f if ln.strip()}


def load_pending_links(source: str) -> list[str]:
    p = pending_path(source)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


def save_listing_df(source: str, df: pd.DataFrame) -> None:
    with _lock:
        df.to_csv(listing_csv_path(source), index=False, encoding="utf-8")


def load_listing_df(source: str) -> pd.DataFrame | None:
    p = listing_csv_path(source)
    if not p.exists():
        return None
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def append_pending_links(source: str, links: list[str]) -> int:
    if not links:
        return 0
    finished = load_finished_links(source)
    existing = set(load_pending_links(source))
    new = [u for u in links if u and u not in finished and u not in existing]
    if not new:
        return 0
    with _lock:
        with pending_path(source).open("a", encoding="utf-8") as f:
            for u in new:
                f.write(u + "\n")
    return len(new)


def mark_link_finished(source: str, link: str) -> None:
    with _lock:
        with finished_path(source).open("a", encoding="utf-8") as f:
            f.write(link + "\n")
        pending = load_pending_links(source)
        kept = [u for u in pending if u != link]
        tmp = pending_path(source).with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for u in kept:
                f.write(u + "\n")
        os.replace(tmp, pending_path(source))


def load_lister_state(source: str) -> dict:
    p = lister_state_path(source)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_lister_state(source: str, state: dict) -> None:
    lister_state_path(source).write_text(json.dumps(state, indent=2), encoding="utf-8")


def wipe_listing_state(source: str) -> None:
    """New run: clear pending + lister cursor; keep finished + listing.csv for dedupe."""
    with _lock:
        for name in ("pending_links.txt", "lister_state.json"):
            p = _source_dir(source) / name
            if p.exists():
                p.unlink()
