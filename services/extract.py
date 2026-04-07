from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup, NavigableString, Tag

FOOTER_HINTS = re.compile(
    r"footer|related|cross-sell|upsell|widget|sidebar|comment|review|rating|ציון",
    re.I,
)
MAIN_HINTS = re.compile(r"price|product|money|amount|woocommerce|shopify|sale|מחיר", re.I)


def normalize_domain(url: str) -> str:
    raw = url if "://" in url else f"https://{url}"
    p = urlparse(raw)
    host = (p.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def parse_price_number(text: str) -> float | None:
    """מחיר אנושי מטקסט מלא — תומך ב־1,234.56 / 1.234,56 / ₪ 199 וכו׳."""
    if not text:
        return None
    t = str(text).strip().replace("\u200f", "").replace("\u200e", "").replace("\xa0", " ")
    # קודם ניסיון: מספר גלוי בתוך טקסט ארוך
    for m in re.finditer(
        r"(?:₪|ש[\"׳]ח|ILS|NIS|\$|€|£)\s*[\d.,\s]+|[\d.,\s]+(?:\s*(?:₪|ש[\"׳]ח|ILS|USD|EUR)?)?",
        t,
        re.I,
    ):
        inner = re.sub(r"[^\d.,]", "", m.group(0))
        v = _parse_normalized_number(inner)
        if v is not None:
            return v
    inner = re.sub(r"[^\d.,]", "", t)
    return _parse_normalized_number(inner)


def _parse_normalized_number(t: str) -> float | None:
    if not t:
        return None
    last_c = t.rfind(",")
    last_d = t.rfind(".")
    dec_pos = max(last_c, last_d)
    if dec_pos == -1:
        try:
            v = float(t)
        except ValueError:
            return None
        return v if 0 < v < 1_000_000 else None
    after = t[dec_pos + 1 :]
    # מפריד אחרון עם 1–2 ספרות אחריו = עשרוני (אירופאי או אמריקאי)
    if len(after) <= 2 and after.isdigit():
        if last_c > last_d:
            cleaned = t.replace(".", "").replace(",", ".")
        else:
            cleaned = t.replace(",", "")
        try:
            v = float(cleaned)
            return v if 0 < v < 1_000_000 else None
        except ValueError:
            return None
    # אלפים בלבד ללא עשרוני — מסירים כל מפרידים
    compact = re.sub(r"[.,]", "", t)
    try:
        v = float(compact)
    except ValueError:
        return None
    return v if 0 < v < 1_000_000 else None


def _type_names(obj: dict[str, Any]) -> set[str]:
    raw = obj.get("@type")
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list):
        return {str(x) for x in raw if x}
    return set()


def _offer_price(obj: dict[str, Any]) -> tuple[Any, Any]:
    """price, currency מתוך אובייקט Offer / AggregateOffer."""
    price = obj.get("price") or obj.get("lowPrice") or obj.get("highPrice")
    cur = obj.get("priceCurrency")
    return price, cur


def _walk_json(obj: Any, out: list[tuple[float, str]]) -> None:
    if isinstance(obj, dict):
        types = _type_names(obj)
        commerce = bool(
            types & {"Product", "Offer", "AggregateOffer", "ProductGroup"}
            or any("Product" in t or "Offer" in t for t in types),
        )
        price: Any = None
        cur: Any = None
        if commerce:
            price = obj.get("price") or obj.get("lowPrice") or obj.get("highPrice")
            cur = obj.get("priceCurrency")
        if price is None and "offers" in obj:
            off = obj["offers"]
            if isinstance(off, dict):
                price, cur = _offer_price(off)
            elif isinstance(off, list) and off:
                first = off[0]
                if isinstance(first, dict):
                    price, cur = _offer_price(first)
        if price is not None:
            if isinstance(price, (int, float)):
                out.append((float(price), str(cur or "")))
            elif isinstance(price, str) and (pn := parse_price_number(price)):
                out.append((pn, str(cur or "")))
        for v in obj.values():
            _walk_json(v, out)
    elif isinstance(obj, list):
        for v in obj:
            _walk_json(v, out)


def extract_json_ld(html: str) -> tuple[float | None, str | None, str]:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type=lambda x: x and "ld+json" in x):
        raw = script.string or script.get_text() or ""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            continue
        found: list[tuple[float, str]] = []
        for block in data:
            _walk_json(block, found)
        if found:
            p, c = found[0]
            return p, (c or None), "json_ld"
    return None, None, "json_ld"


def extract_meta(html: str) -> tuple[float | None, str | None, str]:
    soup = BeautifulSoup(html, "html.parser")
    for prop in ("product:price:amount", "og:price:amount"):
        m = soup.find("meta", property=prop) or soup.find("meta", attrs={"property": prop})
        if m and m.get("content"):
            if pn := parse_price_number(m["content"]):
                return pn, None, "meta"
    m = soup.find("meta", attrs={"name": "twitter:data1"})
    if m and m.get("content") and (pn := parse_price_number(m["content"])):
        return pn, None, "meta"
    return None, None, "meta"


def _element_raw_price_text(el: Tag) -> str:
    """טקסט מחיר מימי מחיר בפועל (bdi, .amount, מבפנים)."""
    for sub in el.select("bdi, .amount, .money, [class*='price']"):
        if isinstance(sub, Tag):
            t = sub.get_text(" ", strip=True)
            if t and re.search(r"\d", t):
                return t
    return el.get_text(" ", strip=True)


_WOO_MAIN_SELECTORS = (
    ".single-product .summary .woocommerce-Price-amount bdi",
    ".single-product .summary .woocommerce-Price-amount",
    "main .product .woocommerce-Price-amount bdi",
    "main .product .woocommerce-Price-amount",
    "#primary .woocommerce-Price-amount bdi",
    ".entry-summary .woocommerce-Price-amount bdi",
    ".woocommerce div.product .woocommerce-Price-amount bdi",
    ".woocommerce-Price-amount bdi",
    ".woocommerce-Price-amount",
)


def extract_woocommerce_amount(html: str) -> tuple[float | None, str | None, str]:
    soup = BeautifulSoup(html, "html.parser")
    for q in _WOO_MAIN_SELECTORS:
        sel = soup.select_one(q)
        if not sel:
            continue
        txt = _element_raw_price_text(sel)
        if pn := parse_price_number(txt):
            return pn, None, "woocommerce_class"
    return None, None, "woocommerce_class"


_SHOPIFY_SELECTORS = (
    "[data-product-price] .money",
    "[data-product-price]",
    ".product__price .price-item--sale",
    ".product__price .price-item--regular",
    ".price--large",
    ".product-single__price .money",
    ".price-item--regular",
    ".price-item--sale",
    'span[data-type="money"]',
    "#ProductPrice",
    "[id^='ProductPrice-'] .money",
)


def extract_shopify_amount(html: str) -> tuple[float | None, str | None, str]:
    soup = BeautifulSoup(html, "html.parser")
    for q in _SHOPIFY_SELECTORS:
        sel = soup.select_one(q)
        if not sel:
            continue
        for attr in ("content", "data-price", "data-money"):
            raw = sel.get(attr)
            if raw and (pn := parse_price_number(str(raw))):
                return pn, None, "shopify_class"
        txt = _element_raw_price_text(sel)
        if pn := parse_price_number(txt):
            return pn, None, "shopify_class"
    return None, None, "shopify_class"


def extract_microdata_price(html: str) -> tuple[float | None, str | None, str]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(attrs={"itemprop": re.compile(r"^price$", re.I)}):
        c = tag.get("content")
        if c and (pn := parse_price_number(str(c))):
            return pn, None, "microdata"
        if pn := parse_price_number(tag.get_text(" ", strip=True)):
            return pn, None, "microdata"
    meta = soup.find("meta", attrs={"itemprop": re.compile(r"price", re.I)})
    if meta and meta.get("content") and (pn := parse_price_number(meta["content"])):
        return pn, None, "microdata"
    return None, None, "microdata"


def _element_depth(el: Tag) -> int:
    d = 0
    p = el.parent
    while p and isinstance(p, Tag):
        d += 1
        p = p.parent
    return d


def _score_element(el: Tag) -> float:
    text = el.get_text(" ", strip=True)
    score = 6.0
    cls = " ".join(el.get("class", [])).lower()
    el_id = (el.get("id") or "").lower()
    combined = f"{cls} {el_id}"
    if FOOTER_HINTS.search(combined) or FOOTER_HINTS.search(text[:80]):
        score -= 4.0
    if MAIN_HINTS.search(combined):
        score += 2.5
    # עדיפות לאזורי מוצר / מחיר מסחר
    for hint in (
        "product",
        "price",
        "entry-summary",
        "woocommerce",
        "shopify",
        "purchase",
        "offer",
    ):
        if hint in combined:
            score += 1.2
            break
    depth = _element_depth(el)
    if depth > 14:
        score -= 2.0
    if depth < 4:
        score -= 1.0
    if el.name in ("del", "s", "strike"):
        score -= 3.0
    return score


def _css_escape_ident(s: str) -> str:
    return re.sub(r"([^a-zA-Z0-9_-])", lambda m: "\\" + m.group(1), s)


def build_unique_css_selector(el: Tag) -> str:
    parts: list[str] = []
    cur: Tag | None = el
    for _ in range(12):
        if cur is None or cur.name in ("html", "[document]"):
            break
        if not isinstance(cur, Tag):
            break
        tag = cur.name
        parent = cur.parent
        if isinstance(parent, Tag):
            same = [c for c in parent.children if isinstance(c, Tag) and c.name == tag]
            idx = same.index(cur) + 1
            sel = f"{tag}:nth-of-type({idx})"
        else:
            sel = tag
        eid = cur.get("id")
        if eid and isinstance(eid, str) and eid.strip():
            parts.append(f"#{_css_escape_ident(eid.strip())}")
            break
        cls = cur.get("class")
        if isinstance(cls, list) and cls:
            stable = [c for c in cls if c and len(c) > 2][:2]
            if stable:
                sel = tag + "".join(f".{_css_escape_ident(c)}" for c in stable)
        parts.append(sel)
        cur = parent if isinstance(parent, Tag) else None
    parts.reverse()
    return " > ".join(parts) if parts else el.name


# מספרים שנראים כמחיר בעמוד מוצר (לא שנים / מיקוד)
_PRICE_IN_TEXT = re.compile(
    r"\d{1,3}(?:[.,\s]\d{3})*(?:[.,]\d{1,2})\b"  # 1,234.56 / 1.234,56
    r"|\d+[.,]\d{1,2}\b"
    r"|\b\d{2,5}\b",
)


def _push_candidate(
    candidates: list[tuple[float, float, str, str, list[str]]],
    seen: set[str],
    parent: Tag,
    chunk: str,
    *,
    bonus: float = 0,
) -> None:
    chunk = chunk.strip()
    if not chunk:
        return
    if not (pn := parse_price_number(chunk)):
        return
    sel = build_unique_css_selector(parent)
    key = f"{sel}|{chunk}|{pn:.4f}"
    if key in seen:
        return
    seen.add(key)
    sc = _score_element(parent) + bonus
    candidates.append((sc, pn, chunk, sel, []))


def _collect_attribute_price_candidates(
    soup: BeautifulSoup,
    candidates: list[tuple[float, float, str, str, list[str]]],
    seen: set[str],
) -> None:
    def consider_tag(tag: Tag, bonus: float, attr_val: str | None) -> None:
        if not attr_val or not str(attr_val).strip():
            return
        val = str(attr_val).strip()
        if val.isdigit() and len(val) >= 5:
            # לרוב סנטים (Shopify וכו׳)
            try:
                cents = int(val)
                if cents > 100 and cents < 50_000_000:
                    v = cents / 100.0
                    if 0 < v < 1_000_000:
                        sel = build_unique_css_selector(tag)
                        key = f"{sel}|{val}|{v:.4f}|cents"
                        if key not in seen:
                            seen.add(key)
                            candidates.append((_score_element(tag) + bonus + 2.0, v, f"{v:.2f}", sel, []))
                        return
            except ValueError:
                pass
        _push_candidate(candidates, seen, tag, val, bonus=bonus + 2.5)

    for tag in soup.find_all(True):
        if not isinstance(tag, Tag):
            continue
        for attr in ("data-price", "data-product-price", "data-money", "data-compare-price"):
            v = tag.get(attr)
            if v:
                consider_tag(tag, 3.0, v)
                break
        ip = tag.get("itemprop")
        if ip and str(ip).lower() == "price":
            c = tag.get("content")
            if c:
                consider_tag(tag, 2.0, c)
            else:
                _push_candidate(candidates, seen, tag, tag.get_text(" ", strip=True), bonus=2.0)


def collect_price_candidates(html: str, limit: int = 40) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[tuple[float, float, str, str, list[str]]] = []
    seen: set[str] = set()

    _collect_attribute_price_candidates(soup, candidates, seen)

    for el in soup.find_all(string=True):
        if not isinstance(el, NavigableString):
            continue
        parent = el.parent
        if not isinstance(parent, Tag):
            continue
        raw = str(el)
        if not raw.strip():
            continue
        for m in _PRICE_IN_TEXT.finditer(raw):
            chunk = m.group(0).strip()
            _push_candidate(candidates, seen, parent, chunk, bonus=0)

    candidates.sort(key=lambda x: (-x[0], -x[1]))
    out: list[dict[str, Any]] = []
    for sc, _pn, chunk, sel, alts in candidates[:limit]:
        out.append(
            {
                "price_text": chunk,
                "score": round(sc, 2),
                "selector": sel,
                "selector_alternates": alts,
            },
        )
    return out


def _prioritize_candidates_near(
    candidates: list[dict[str, Any]],
    ref: float | None,
    limit: int,
) -> list[dict[str, Any]]:
    if ref is None or not candidates:
        return candidates[:limit]

    def dist(c: dict[str, Any]) -> float:
        pn = parse_price_number(str(c.get("price_text") or ""))
        if pn is None:
            return 1e9
        return abs(float(pn) - float(ref))

    merged = sorted(candidates, key=lambda c: (dist(c), -(c.get("score") or 0)))
    return merged[:limit]


def run_extraction_pipeline(html: str) -> dict[str, Any]:
    cand_pool = collect_price_candidates(html, limit=72)

    def finish(price: float | None, currency: str | None, source: str) -> dict[str, Any]:
        cand = _prioritize_candidates_near(cand_pool, price, 40)
        return {"price": price, "currency": currency, "source": source, "candidates": cand}

    p, c, src = extract_json_ld(html)
    if p:
        return finish(p, c, src)
    p, c, src = extract_meta(html)
    if p:
        return finish(p, c, src)
    p, c, src = extract_shopify_amount(html)
    if p:
        return finish(p, c, src)
    p, c, src = extract_woocommerce_amount(html)
    if p:
        return finish(p, c, src)
    p, c, src = extract_microdata_price(html)
    if p:
        return finish(p, c, src)

    cand = cand_pool[:40]
    best = cand[0] if cand else None
    if best and (pn := parse_price_number(str(best.get("price_text") or ""))):
        return {"price": pn, "currency": None, "source": "heuristic", "candidates": cand}
    return {"price": None, "currency": None, "source": None, "candidates": cand}


def _price_from_selected_element(el: Tag) -> float | None:
    for attr in ("content", "data-price", "data-product-price", "data-money"):
        raw = el.get(attr)
        if raw and (pn := parse_price_number(str(raw))):
            return pn
    return parse_price_number(_element_raw_price_text(el))


def apply_saved_selector(html: str, css_selector: str) -> float | None:
    soup = BeautifulSoup(html, "html.parser")
    el = soup.select_one(css_selector)
    if not el:
        return None
    return _price_from_selected_element(el)


def validate_selector_with_fallbacks(
    html: str,
    primary: str,
    alternates: list[str] | None,
) -> tuple[float | None, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    for sel in [primary, *(alternates or [])]:
        el = soup.select_one(sel)
        if not el:
            continue
        if pn := _price_from_selected_element(el):
            return pn, sel
    return None, None


@dataclass
class ConfirmBody:
    url: str
    css_selector: str
    selector_alternates: list[str] | None = None
