"""HTTP fetch wrapper: per-worker proxy + retry on 403/429/202."""

from __future__ import annotations

import logging
import time
from typing import Optional

from buyee_http import settings as config
from buyee_http.proxy_manager import WorkerProxy

log = logging.getLogger("http")

_USE_CURL_CFFI = True
try:
    from curl_cffi import requests as _cr  # type: ignore
except Exception:  # noqa: BLE001
    _USE_CURL_CFFI = False
    import requests as _cr  # type: ignore


class FetchError(Exception):
    pass


def _do_get(
    url: str,
    headers: dict,
    proxies: Optional[dict],
    cookies: Optional[dict] = None,
) -> tuple[int, str]:
    impersonate = config.BUYEE_CURL_IMPERSONATE if _USE_CURL_CFFI else None
    kwargs = dict(
        url=url,
        headers=headers,
        proxies=proxies,
        cookies=cookies,
        timeout=config.REQUEST_TIMEOUT,
        allow_redirects=True,
    )
    if _USE_CURL_CFFI:
        kwargs["impersonate"] = impersonate or "chrome"
        r = _cr.get(**kwargs)
    else:
        r = _cr.get(**kwargs)
    return r.status_code, r.text


def fetch_response(
    url: str,
    worker_proxy: Optional[WorkerProxy] = None,
    extra_headers: Optional[dict] = None,
) -> tuple[int, str]:
    """GET url; return (status_code, text). Raises FetchError after retries."""
    headers = {**config.DEFAULT_HEADERS, **(extra_headers or {})}
    last_err: Optional[str] = None
    force_new_proxy = False

    for attempt in range(1, config.PROXY_MAX_RETRIES + 1):
        proxy_obj = worker_proxy.get(force_new=force_new_proxy) if worker_proxy else None
        if worker_proxy is not None and proxy_obj is None:
            last_err = "proxy not yet available"
            log.info(
                "[%s try %s/%s] %s — %s, sleeping %ss",
                worker_proxy.owner,
                attempt,
                config.PROXY_MAX_RETRIES,
                url,
                last_err,
                config.PROXY_FAILURE_PAUSE_SEC,
            )
            time.sleep(config.PROXY_FAILURE_PAUSE_SEC)
            force_new_proxy = False
            continue

        proxies = proxy_obj.as_requests_dict() if proxy_obj else None
        cookies = worker_proxy.waf_cookies() if worker_proxy else {}
        peer = f"{proxy_obj.host}:{proxy_obj.port}" if proxy_obj else "direct"
        owner = worker_proxy.owner if worker_proxy else "no-proxy"
        cookie_tag = "+waf" if cookies.get("aws-waf-token") else "no-waf"

        try:
            status, text = _do_get(url, headers, proxies, cookies or None)
        except Exception as exc:  # noqa: BLE001
            last_err = f"network error: {exc}"
            log.warning(
                "[%s try %s/%s] %s via %s (%s) -> %s",
                owner,
                attempt,
                config.PROXY_MAX_RETRIES,
                url,
                peer,
                cookie_tag,
                last_err,
            )
            time.sleep(config.PROXY_FAILURE_PAUSE_SEC)
            force_new_proxy = True
            continue

        if status in config.BAD_STATUSES:
            last_err = f"HTTP {status}"
            log.warning(
                "[%s try %s/%s] %s via %s (%s) -> %s",
                owner,
                attempt,
                config.PROXY_MAX_RETRIES,
                url,
                peer,
                cookie_tag,
                last_err,
            )
            time.sleep(config.PROXY_FAILURE_PAUSE_SEC)

            if worker_proxy and status in (202, 403):
                bundle = worker_proxy.refresh_waf(force=True)
                if bundle is not None and not bundle.is_empty():
                    log.info(
                        "[%s] WAF cookie refreshed; retrying without proxy rotation",
                        owner,
                    )
                    force_new_proxy = False
                    continue
                log.warning("[%s] WAF cookie refresh failed; rotating proxy", owner)

            force_new_proxy = True
            continue

        if status != 200:
            raise FetchError(f"HTTP {status} for {url}")

        return status, text

    raise FetchError(f"Exhausted retries for {url}: {last_err}")


def fetch(
    url: str,
    worker_proxy: Optional[WorkerProxy] = None,
    extra_headers: Optional[dict] = None,
) -> str:
    """GET url; return response body text."""
    return fetch_response(url, worker_proxy, extra_headers)[1]
