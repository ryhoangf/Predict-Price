"""Per-worker proxy provider for proxyxoay.shop (one API key per worker thread)."""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests

from buyee_http import settings as config
from buyee_http.waf_solver import WafBundle, solve_via_browser

log = logging.getLogger("proxy")


def load_keys(path: Path | None = None) -> list[str]:
    keys: list[str] = []
    p = path or config.PROXY_KEYS_FILE
    if p.exists():
        for raw in p.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("PUT-KEY-") or line.upper() == "PUT-YOUR-API-KEY-HERE":
                continue
            keys.append(line)
    if config.PROXY_XOAY_KEYS_RAW:
        for k in config.PROXY_XOAY_KEYS_RAW.split(","):
            k = k.strip()
            if k and k not in keys:
                keys.append(k)
    if not keys and config.PROXY_XOAY_KEY:
        keys = [config.PROXY_XOAY_KEY]
    return keys


@dataclass
class Proxy:
    host: str
    port: int
    username: str = ""
    password: str = ""
    expires_at: float = 0.0
    fetched_at: float = field(default_factory=time.time)

    @property
    def http_url(self) -> str:
        if self.username:
            return f"http://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"http://{self.host}:{self.port}"

    def as_requests_dict(self) -> dict:
        url = self.http_url
        return {"http": url, "https": url}

    def is_expired(self) -> bool:
        return time.time() >= self.expires_at


class WorkerProxy:
    """Owns a single API key and the currently-live proxy for that key."""

    def __init__(self, owner: str, api_key: str) -> None:
        self.owner = owner
        self.api_key = api_key
        self._proxy: Optional[Proxy] = None
        self._lock = threading.Lock()
        self._last_api_call = 0.0
        self._api_throttle_sec = 2.0
        self._waf: Optional[WafBundle] = None
        self._waf_lock = threading.Lock()

    def _call_api(self) -> Optional[Proxy]:
        wait = self._api_throttle_sec - (time.time() - self._last_api_call)
        if wait > 0:
            time.sleep(wait)
        url = config.PROXY_API_URL.format(key=self.api_key)
        try:
            r = requests.get(url, timeout=15)
            self._last_api_call = time.time()
            data = r.json()
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] proxy API error: %s", self.owner, exc)
            return None

        status = data.get("status")
        msg = data.get("message", "")
        if status == 102:
            log.error("[%s] proxy API key expired (status=102)", self.owner)
            return None
        if status != 100:
            log.info(
                "[%s] proxy API status=%s msg=%r (transient, will retry)",
                self.owner,
                status,
                msg,
            )
            return None

        parts = (data.get("proxyhttp") or "").split(":")
        if len(parts) < 2:
            log.warning("[%s] bad proxyhttp string: %r", self.owner, data.get("proxyhttp"))
            return None
        host = parts[0]
        port = int(parts[1])
        user = parts[2] if len(parts) > 2 else ""
        pwd = parts[3] if len(parts) > 3 else ""

        lifetime = config.PROXY_MIN_LIFETIME_SEC
        m = re.search(r"(\d+)\s*s", msg)
        if m:
            lifetime = max(config.PROXY_MIN_LIFETIME_SEC, int(m.group(1)))

        p = Proxy(
            host=host,
            port=port,
            username=user,
            password=pwd,
            expires_at=time.time() + lifetime,
        )
        log.info("[%s] proxy=%s:%s ttl=%ss", self.owner, host, port, lifetime)
        return p

    def get(self, force_new: bool = False) -> Optional[Proxy]:
        with self._lock:
            if self._proxy and not force_new and not self._proxy.is_expired():
                return self._proxy
            new = self._call_api()
            if new is not None:
                self._proxy = new
            return self._proxy

    def warm_up(self, attempts: int = 5) -> bool:
        for _ in range(attempts):
            if self.get() is not None:
                return True
            time.sleep(3)
        return False

    @property
    def waf(self) -> Optional[WafBundle]:
        return self._waf

    def waf_cookies(self) -> dict[str, str]:
        b = self._waf
        return dict(b.cookies) if b else {}

    def refresh_waf(self, force: bool = False) -> Optional[WafBundle]:
        proxy = self.get()
        if proxy is None:
            log.warning("[%s] refresh_waf: no proxy available", self.owner)
            return None
        endpoint = f"{proxy.host}:{proxy.port}"

        with self._waf_lock:
            if (
                not force
                and self._waf
                and self._waf.proxy_endpoint == endpoint
                and not self._waf.is_empty()
            ):
                return self._waf

            log.info(
                "[%s] refreshing WAF cookie via browser through %s ...",
                self.owner,
                endpoint,
            )
            bundle = solve_via_browser(
                proxy_server=f"http://{endpoint}",
                proxy_username=proxy.username,
                proxy_password=proxy.password,
            )
            if bundle is None or bundle.is_empty():
                log.warning(
                    "[%s] WAF browser solve failed; caller should rotate proxy",
                    self.owner,
                )
                return None
            self._waf = bundle
            return self._waf


class ProxyAssignments:
    """Holds N WorkerProxy instances, one per loaded key."""

    def __init__(self, keys: list[str]) -> None:
        self.workers: list[WorkerProxy] = [
            WorkerProxy(owner=f"key{i}", api_key=k) for i, k in enumerate(keys)
        ]

    def __len__(self) -> int:
        return len(self.workers)

    def for_worker(self, idx: int) -> WorkerProxy:
        return self.workers[idx % len(self.workers)]

    def warm_up_all(self) -> int:
        ok = 0
        for w in self.workers:
            if w.warm_up():
                ok += 1
        return ok

    def warm_waf_all(self, count: int | None = None) -> int:
        n = len(self.workers) if count is None else min(count, len(self.workers))
        ok = 0
        for i in range(n):
            bundle = self.for_worker(i).refresh_waf(force=True)
            if bundle is not None and not bundle.is_empty():
                ok += 1
        return ok
