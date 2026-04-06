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
    t = text.strip()
    t = re.sub(r"[^\d.,]", "", t)
    if not t:
        return None
    t = t.replace(",", ".")
    parts = t.split(".")
    if len(parts) > 2:
        t = "".join(parts[:-1]) + "." + parts[-1]
    elif len(parts) == 2 and len(parts[1]) == 3 and len(parts[0]) <= 3:
        t = parts[0] + parts[1]
    try:
        v = float(t)
        return v if 0 < v < 1_000_000 else None
    except ValueError:
        return None


def _walk_json(obj: Any, out: list[tuple[float, str]]) -> None:
    if isinstance(obj, dict):
        if obj.get("@type") in ("Product", "Offer") or "offers" in obj:
            price = obj.get("price") or obj.get("lowPrice") or obj.get("highPrice")
            cur = obj.get("priceCurrency")
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


def extract_woocommerce_amount(html: str) -> tuple[float | None, str | None, str]:
    soup = BeautifulSoup(html, "html.parser")
    sel = soup.select_one(".woocommerce-Price-amount bdi, .woocommerce-Price-amount")
    if sel:
        txt = sel.get_text(" ", strip=True)
        if pn := parse_price_number(txt):
            return pn, None, "woocommerce_class"
    return None, None, "woocommerce_class"


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
        score += 2.0
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


def collect_price_candidates(html: str, limit: int = 40) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[tuple[float, float, str, str, list[str]]] = []
    seen: set[str] = set()

    for el in soup.find_all(string=True):
        if not isinstance(el, NavigableString):
            continue
        parent = el.parent
        if not isinstance(parent, Tag):
            continue
        raw = str(el)
        if not raw.strip():
            continue
        for m in re.finditer(r"[\d]{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})|[\d]+[.,]\d{2}|[\d]{2,6}", raw):
            chunk = m.group(0)
            if not (pn := parse_price_number(chunk)):
                continue
            sel = build_unique_css_selector(parent)
            if sel in seen:
                continue
            seen.add(sel)
            sc = _score_element(parent)
            candidates.append((sc, pn, chunk, sel, []))

    candidates.sort(key=lambda x: (-x[0], -x[1]))
    out: list[dict[str, Any]] = []
    for sc, _pn, chunk, sel, alts in candidates[:limit]:
        out.append(
            {
                "price_text": chunk,
                "score": round(sc, 2),
                "selector": sel,
                "selector_alternates": alts,
            }
        )
    return out


def run_extraction_pipeline(html: str) -> dict[str, Any]:
    p, c, src = extract_json_ld(html)
    if p:
        return {"price": p, "currency": c, "source": src, "candidates": collect_price_candidates(html)}
    p, c, src = extract_meta(html)
    if p:
        return {"price": p, "currency": c, "source": src, "candidates": collect_price_candidates(html)}
    p, c, src = extract_woocommerce_amount(html)
    if p:
        return {"price": p, "currency": c, "source": src, "candidates": collect_price_candidates(html)}
    cand = collect_price_candidates(html)
    best = cand[0] if cand else None
    if best and (pn := parse_price_number(best["price_text"])):
        return {"price": pn, "currency": None, "source": "heuristic", "candidates": cand}
    return {"price": None, "currency": None, "source": None, "candidates": cand}


def apply_saved_selector(html: str, css_selector: str) -> float | None:
    soup = BeautifulSoup(html, "html.parser")
    el = soup.select_one(css_selector)
    if not el:
        return None
    txt = el.get_text(" ", strip=True)
    return parse_price_number(txt)


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
        txt = el.get_text(" ", strip=True)
        if pn := parse_price_number(txt):
            return pn, sel
    return None, None


@dataclass
class ConfirmBody:
    url: str
    css_selector: str
    selector_alternates: list[str] | None = None
