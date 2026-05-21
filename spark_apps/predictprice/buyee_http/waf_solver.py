"""AWS WAF cookie harvester (one-shot Playwright bootstrap per proxy).

Browser install (one-time):
    python -m playwright install chromium
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Optional

from buyee_http import settings as config

log = logging.getLogger("waf")

try:
    from playwright.sync_api import sync_playwright  # noqa: F401

    _HAS_PW = True
except Exception:  # noqa: BLE001
    _HAS_PW = False


@dataclass
class WafBundle:
    """Cookies + UA captured from a successful browser session."""

    cookies: dict[str, str] = field(default_factory=dict)
    user_agent: str = ""
    fetched_at: float = 0.0
    proxy_endpoint: str = ""

    def is_empty(self) -> bool:
        return not self.cookies or "aws-waf-token" not in self.cookies


def solve_via_browser(
    proxy_server: str,
    proxy_username: str = "",
    proxy_password: str = "",
    target_url: Optional[str] = None,
    headless: Optional[bool] = None,
    timeout_sec: Optional[int] = None,
) -> Optional[WafBundle]:
    if not _HAS_PW:
        log.error(
            "Playwright not available — install with `pip install playwright` "
            "and `python -m playwright install chromium`"
        )
        return None

    if headless is None:
        headless = config.WAF_HEADLESS
    if timeout_sec is None:
        timeout_sec = config.WAF_BROWSER_TIMEOUT_SEC

    seeds = config.WAF_SEED_ITEM_IDS or [
        "75721385d5c1201c811f5dc57ff15bd2",
    ]
    url = target_url or f"{config.BASE_URL}/rakuma/item/{random.choice(seeds)}"

    proxy_cfg: dict = {"server": proxy_server}
    if proxy_username:
        proxy_cfg["username"] = proxy_username
        proxy_cfg["password"] = proxy_password

    from playwright.sync_api import sync_playwright

    t0 = time.time()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                proxy=proxy_cfg,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            ctx = browser.new_context(user_agent=config.USER_AGENT)
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_sec * 1000)
            try:
                page.wait_for_function(
                    "document.cookie.includes('aws-waf-token')",
                    timeout=timeout_sec * 1000,
                )
            except Exception:
                log.warning(
                    "WAF token did not appear within %ss for %s",
                    timeout_sec,
                    proxy_server,
                )
                browser.close()
                return None
            cookies = ctx.cookies("https://buyee.jp")
            ua = page.evaluate("() => navigator.userAgent")
            browser.close()
    except Exception as exc:  # noqa: BLE001
        log.warning("Playwright error for %s: %s", proxy_server, exc)
        return None

    jar = {c["name"]: c["value"] for c in cookies}
    log.info(
        "WAF solved via %s in %.1fs (cookies=%s)",
        proxy_server,
        time.time() - t0,
        list(jar.keys()),
    )
    return WafBundle(
        cookies=jar,
        user_agent=ua or config.USER_AGENT,
        fetched_at=time.time(),
        proxy_endpoint=proxy_server.replace("http://", "").split("@")[-1],
    )
