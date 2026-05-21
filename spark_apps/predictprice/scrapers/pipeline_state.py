"""Thread-safe persistence for buyee lister/worker pipeline (per-source dirs)."""

from __future__ import annotations

import csv
import json
import os
import threading
from pathlib import Path
from typing import Iterable

import config
from scrapers.buyee_parser import CSV_FIELDS

_lock = threading.Lock()
PendingEntry = tuple[str, str, int, str]


def source_data_dir(source: str) -> Path:
    d = config.PIPELINE_DATA_DIR / source.lower()
    d.mkdir(parents=True, exist_ok=True)
    return d


def pending_path(source: str) -> Path:
    return source_data_dir(source) / "pending.txt"


def finished_path(source: str) -> Path:
    return source_data_dir(source) / "finished_scraping.txt"


def lister_state_path(source: str) -> Path:
    return source_data_dir(source) / "lister_state.json"


def output_csv_path(source: str) -> Path:
    return source_data_dir(source) / "items.csv"


def _ensure_files(source: str) -> None:
    source_data_dir(source)
    for p in (pending_path(source), finished_path(source)):
        if not p.exists():
            p.touch()
    if config.CSV_ENABLED and not output_csv_path(source).exists():
        with output_csv_path(source).open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()


def _parse_pending_line(line: str) -> PendingEntry | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split("|")
    if len(parts) != 4:
        return None
    platform, cat, page_s, pid = parts
    try:
        return (platform, cat, int(page_s), pid)
    except ValueError:
        return None


def load_finished(source: str) -> set[str]:
    _ensure_files(source)
    with finished_path(source).open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def load_pending(source: str) -> list[PendingEntry]:
    _ensure_files(source)
    out: list[PendingEntry] = []
    with pending_path(source).open("r", encoding="utf-8") as f:
        for line in f:
            e = _parse_pending_line(line)
            if e is not None:
                out.append(e)
    return out


def append_pending(source: str, entries: Iterable[PendingEntry]) -> int:
    entries = list(entries)
    if not entries:
        return 0
    _ensure_files(source)
    with _lock:
        finished = load_finished(source)
        existing = {pid for _, _, _, pid in load_pending(source)}
        new = [e for e in entries if e[3] not in finished and e[3] not in existing]
        if not new:
            return 0
        with pending_path(source).open("a", encoding="utf-8") as f:
            for plat, cat, pg, pid in new:
                f.write(f"{plat}|{cat}|{pg}|{pid}\n")
        return len(new)


def remove_pending(source: str, pid: str) -> None:
    _ensure_files(source)
    with _lock:
        rows = load_pending(source)
        kept = [e for e in rows if e[3] != pid]
        tmp = pending_path(source).with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for plat, cat, pg, p in kept:
                f.write(f"{plat}|{cat}|{pg}|{p}\n")
        os.replace(tmp, pending_path(source))


def mark_finished(source: str, pid: str) -> None:
    _ensure_files(source)
    with _lock:
        with finished_path(source).open("a", encoding="utf-8") as f:
            f.write(pid + "\n")


def mark_many_finished(source: str, pids: Iterable[str]) -> None:
    _ensure_files(source)
    with _lock:
        with finished_path(source).open("a", encoding="utf-8") as f:
            for pid in pids:
                if pid:
                    f.write(pid + "\n")


def append_csv(source: str, row: dict) -> None:
    if not config.CSV_ENABLED:
        return
    _ensure_files(source)
    with _lock:
        with output_csv_path(source).open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writerow({k: row.get(k, "") for k in CSV_FIELDS})


def load_lister_state(source: str) -> dict:
    p = lister_state_path(source)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return {}


def save_lister_state(source: str, state: dict) -> None:
    lister_state_path(source).write_text(
        json.dumps(state, indent=2), encoding="utf-8"
    )


def wipe_session_partial(source: str) -> None:
    with _lock:
        for p in (pending_path(source), lister_state_path(source)):
            if p.exists():
                p.unlink()


def wipe_session_full(source: str) -> None:
    with _lock:
        for p in (
            pending_path(source),
            lister_state_path(source),
            finished_path(source),
            output_csv_path(source),
        ):
            if p.exists():
                p.unlink()
