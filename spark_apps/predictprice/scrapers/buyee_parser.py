"""HTML parsers for the three buyee.jp surfaces.

Search pages
------------
`parse_search_page(html, platform)` returns (product_ids, last_page_number).
`platform` is one of "rakuma", "jdirectitems", "mercari".

Item pages
----------
`parse_item_page(html, product_id, platform, fetcher=None)` returns a dict
whose keys match `CSV_FIELDS`. `fetcher` is an optional callable
`fetcher(url) -> str` that the parser may use for follow-up requests
(currently used by mercari to fetch the description iframe).
"""

from __future__ import annotations

import logging
import re
from typing import Callable, Optional

from bs4 import BeautifulSoup

import config

log = logging.getLogger("parser")

# Pre-compile per-platform item-id regexes once.
_ITEM_REGEXES = {name: re.compile(p.item_id_regex) for name, p in config.PLATFORMS.items()}
_PAGE_QS_RE = re.compile(r"[?&]page=(\d+)")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""


def _attr_pairs(dl) -> dict[str, list[str]]:
    """Walk a <dl> and group <dt>/<dd> into label -> [values]."""
    out: dict[str, list[str]] = {}
    current: Optional[str] = None
    for child in dl.children:
        name = getattr(child, "name", None)
        if name == "dt":
            current = _text(child)
            out.setdefault(current, [])
        elif name == "dd" and current is not None:
            out[current].append(_text(child))
    return out


_DATA_BIND_PAGE_RE = re.compile(r'"page"\s*:\s*(\d+)')


def _find_max_page(soup: BeautifulSoup, container_sel: str) -> int:
    pag = soup.select_one(container_sel)
    if not pag:
        return 1
    nums: list[int] = []
    # Standard hrefs (rakuma, mercari)
    for a in pag.find_all("a", href=True):
        m = _PAGE_QS_RE.search(a["href"])
        if m:
            nums.append(int(m.group(1)))
    # JS pagination (jdirectitems uses data-bind='click: search({"page":N})')
    for el in pag.find_all(attrs={"data-bind": True}):
        for m in _DATA_BIND_PAGE_RE.finditer(el["data-bind"]):
            nums.append(int(m.group(1)))
    return max(nums) if nums else 1


def _clean_desc(el) -> str:
    if el is None:
        return ""
    for br in el.find_all("br"):
        br.replace_with("\n")
    return el.get_text("\n", strip=True)


# ---------------------------------------------------------------------------
# Search-page parsers
# ---------------------------------------------------------------------------
def parse_search_page(html: str, platform: str) -> tuple[list[str], int]:
    soup = BeautifulSoup(html, "lxml")
    pattern = _ITEM_REGEXES[platform]

    ids: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if m:
            pid = m.group(1)
            if pid not in seen:
                seen.add(pid)
                ids.append(pid)

    # Pagination container selector differs slightly across platforms.
    last_page = max(
        _find_max_page(soup, ".pagination"),
        _find_max_page(soup, ".page_navi"),
    )
    return ids, last_page


# ---------------------------------------------------------------------------
# Item-page parsers — one per platform
# ---------------------------------------------------------------------------
def _parse_rakuma(soup: BeautifulSoup, pid: str) -> dict:
    name = _text(soup.select_one("h1.rakuma__itemName"))
    price = _text(soup.select_one(".attrContainer__price"))

    attrs: dict[str, list[str]] = {}
    for dl in soup.select("dl.attrContainer__detail"):
        for k, v in _attr_pairs(dl).items():
            attrs.setdefault(k, []).extend(v)

    def first(prefix: str) -> str:
        for k, vs in attrs.items():
            if k.lower().startswith(prefix.lower()):
                return vs[0] if vs else ""
        return ""

    brand = first("Brand")
    condition = first("Item Condition")
    shipping_paid_by = first("Shipping Paid By")

    # Seller name + ID
    seller_name = ""
    seller_id = ""
    for dl in soup.select("dl.attrContainer__detail"):
        link = dl.select_one('a[href*="seller_id="]')
        if link:
            seller_name = _text(link)
            m = re.search(r"seller_id=([a-zA-Z0-9]+)", link.get("href", ""))
            if m:
                seller_id = m.group(1)
            break

    # Rating (good/normal/bad counts)
    rating_good = rating_normal = rating_bad = ""
    rating_list = soup.select(".attrContainer__ratingList .attrContainer__rating")
    if len(rating_list) >= 3:
        nums = [re.sub(r"\D", "", _text(li)) for li in rating_list[:3]]
        rating_good, rating_normal, rating_bad = nums

    description = _clean_desc(soup.select_one(".itemDetail__content"))

    return {
        "name": name, "price": price, "brand": brand,
        "item_condition": condition, "shipping_paid_by": shipping_paid_by,
        "seller_name": seller_name, "seller_id": seller_id,
        "rating_good": rating_good, "rating_normal": rating_normal, "rating_bad": rating_bad,
        "item_explanation": description,
    }


def _yahoo_auction_price(soup: BeautifulSoup) -> str:
    """Prefer Buyout Price; fall back to Current Price from dl.current_price."""
    buyout = current = ""
    for dl in soup.select("dl.current_price"):
        for dt in dl.find_all("dt", recursive=False):
            label = _text(dt).lower()
            dd = dt.find_next_sibling("dd")
            if dd is None:
                continue
            price_el = dd.select_one(".price-tax") or dd.select_one(".price")
            raw = _text(price_el) if price_el else _text(dd)
            m = re.search(r"[\d,]+\s*YEN", raw)
            if not m:
                continue
            val = m.group(0)
            if "buyout" in label:
                buyout = val
            elif "current" in label:
                current = val
    if buyout or current:
        return buyout or current
    legacy = soup.select_one(".current_price .price")
    if legacy:
        m = re.search(r"[\d,]+\s*YEN", _text(legacy))
        if m:
            return m.group(0)
    return ""


def _parse_jdirectitems(soup: BeautifulSoup, pid: str) -> dict:
    name = _text(soup.select_one("h1.itemInformation__itemName"))
    price = _yahoo_auction_price(soup)

    # Seller block
    seller_name = ""
    seller_id = ""
    seller_block = soup.select_one(".itemSeller")
    pct_good = ""
    rating_good = ""
    rating_bad = ""
    if seller_block:
        for item in seller_block.select(".itemSeller__item"):
            label = _text(item.select_one(".itemSeller__itemName"))
            value_el = item.select_one(".itemSeller__itemValue")
            value = _text(value_el)
            if label.lower().startswith("seller") and not seller_name:
                a = value_el.select_one("a") if value_el else None
                seller_name = _text(a) if a else value
                if a:
                    m = re.search(r"customer/([^/?#]+)", a.get("href", ""))
                    if m:
                        seller_id = m.group(1)
            elif "percentage" in label.lower():
                pct_good = value
            elif label.lower() == "good":
                rating_good = re.sub(r"\D", "", value)
            elif label.lower() == "bad":
                rating_bad = re.sub(r"\D", "", value)

    # Item-detail dl-style list. The .itemDetail__listName div includes a
    # tooltip <a><p>... inline, so plain _text() returns "label + tooltip"
    # mashed together. We only want the leading text node of the label div.
    def _label_text(el) -> str:
        if el is None:
            return ""
        # First non-empty text node, stop at any nested <a>/<p>
        for node in el.descendants:
            if isinstance(node, str):
                s = node.strip()
                if s:
                    return s
        return ""

    details: dict[str, str] = {}
    for li in soup.select("#itemDetail_sec .itemDetail__list"):
        k = _label_text(li.select_one(".itemDetail__listName"))
        v = _text(li.select_one(".itemDetail__listValue"))
        if k:
            details[k] = v

    condition = details.get("Item Condition", "")
    shipping_paid_by = details.get(
        "Domestic Shipping Fee Responsibility",
        details.get("Shipping Paid By", ""),
    )
    # Yahoo has no "Brand" field on item page — leave blank, GA JSON has category
    # crumbs but those aren't a brand. Could be enriched later.
    brand = ""

    # Description text is embedded inside a JS .append(" ... "); block.
    # Extract everything between the first .append(" and the closing "); and
    # strip HTML tags for a readable explanation.
    explanation = ""
    raw_html = str(soup)
    m = re.search(r"#item_description_viewer.*?\.append\(\"(.+?)\"\);", raw_html, re.DOTALL)
    if m:
        # Only unescape the JS-level escapes; the Japanese characters are
        # already real UTF-8 in the page, so leave them alone (do NOT round-
        # trip through "unicode_escape" — that codec is Latin-1-based and
        # would mojibake all non-ASCII text).
        inner = (m.group(1)
                 .replace("\\\"", "\"")
                 .replace("\\'", "'")
                 .replace("\\\\", "\\")
                 .replace("\\n", "\n")
                 .replace("\\t", "\t")
                 .replace("\\/", "/"))
        inner_soup = BeautifulSoup(inner, "lxml")
        explanation = _clean_desc(inner_soup)

    return {
        "name": name, "price": price, "brand": brand,
        "item_condition": condition, "shipping_paid_by": shipping_paid_by,
        "seller_name": seller_name, "seller_id": seller_id,
        "rating_good": rating_good, "rating_normal": pct_good, "rating_bad": rating_bad,
        "item_explanation": explanation,
    }


def _parse_mercari(soup: BeautifulSoup, pid: str,
                   fetcher: Optional[Callable[[str], str]] = None) -> dict:
    name = _text(soup.select_one("h1.m-goodsName"))

    # Price: ".m-goodsDetail__price" includes FX text — take the first YEN bit
    price_el = soup.select_one(".m-goodsDetail__price")
    price = ""
    if price_el:
        m = re.search(r"[\d,]+\s*YEN", price_el.get_text(" ", strip=True))
        price = m.group(0) if m else _text(price_el)

    attrs: dict[str, list[str]] = {}
    for dl in soup.select("dl.m-goodsTable"):
        for k, v in _attr_pairs(dl).items():
            attrs.setdefault(k, []).extend(v)

    def first(prefix: str) -> str:
        for k, vs in attrs.items():
            if k.lower().startswith(prefix.lower()):
                return vs[0] if vs else ""
        return ""

    brand = first("Brand")
    condition = first("Item Condition")
    shipping_paid_by = first("Shipping Paid By")

    seller_name = ""
    seller_id = ""
    seller_link = soup.select_one('.m-goodsDetail__avatarSeller a[href*="seller="]') \
        or soup.select_one('dl.m-goodsTable a[href*="seller="]')
    if seller_link is not None:
        seller_name = _text(seller_link)
        m = re.search(r"seller=([a-zA-Z0-9]+)", seller_link.get("href", ""))
        if m:
            seller_id = m.group(1)

    rating_good = rating_normal = rating_bad = ""
    good_el = soup.select_one(".attrContainer__ratings--good")
    normal_el = soup.select_one(".attrContainer__ratings--normal")
    bad_el = soup.select_one(".attrContainer__ratings--bad")
    rating_good = re.sub(r"\D", "", _text(good_el))
    rating_normal = re.sub(r"\D", "", _text(normal_el))
    rating_bad = re.sub(r"\D", "", _text(bad_el))

    # Description is in an iframe served separately. Try fetching it.
    explanation = ""
    desc_url_tpl = config.PLATFORMS["mercari"].description_url_template
    if fetcher is not None and desc_url_tpl:
        desc_url = desc_url_tpl.format(product_id=pid)
        try:
            desc_html = fetcher(desc_url)
            desc_soup = BeautifulSoup(desc_html, "lxml")
            # iframe page usually contains the description directly in body
            cand = (desc_soup.select_one("#item_description")
                    or desc_soup.select_one(".m-itemDetail__content")
                    or desc_soup.body)
            explanation = _clean_desc(cand)
        except Exception as exc:  # noqa: BLE001
            log.warning("[mercari] description fetch failed for %s: %s", pid, exc)

    return {
        "name": name, "price": price, "brand": brand,
        "item_condition": condition, "shipping_paid_by": shipping_paid_by,
        "seller_name": seller_name, "seller_id": seller_id,
        "rating_good": rating_good, "rating_normal": rating_normal, "rating_bad": rating_bad,
        "item_explanation": explanation,
    }


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------
def parse_item_page(html: str, product_id: str, platform: str,
                    fetcher: Optional[Callable[[str], str]] = None,
                    category_id: str = "") -> dict:
    soup = BeautifulSoup(html, "lxml")
    if platform == "rakuma":
        row = _parse_rakuma(soup, product_id)
    elif platform == "jdirectitems":
        row = _parse_jdirectitems(soup, product_id)
    elif platform == "mercari":
        row = _parse_mercari(soup, product_id, fetcher=fetcher)
    else:
        raise ValueError(f"Unknown platform: {platform}")

    item_url = config.PLATFORMS[platform].item_url_template.format(product_id=product_id)
    row["rating_summary"] = (
        f"good:{row.get('rating_good','')} "
        f"normal:{row.get('rating_normal','')} "
        f"bad:{row.get('rating_bad','')}".strip()
    )
    row["product_id"] = product_id
    row["platform"] = platform
    row["category_id"] = category_id
    row["url"] = item_url
    return row


# Columns written to items.csv. Internal fields like seller / rating / url
# are still parsed (for logging & possible re-use) but kept out of the CSV
# to keep the export clean.
CSV_FIELDS = [
    "platform",
    "product_id",
    "name",
    "price",
    "brand",
    "item_condition",
    "item_explanation",
]
