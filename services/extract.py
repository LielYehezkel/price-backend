from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup, NavigableString, Tag

# לא לכלול "widget" גולמי — Elementor משתמש ב־elementor-widget-* גם באזור המחיר הראשי
FOOTER_HINTS = re.compile(
    r"footer|related|cross-sell|upsell|widget-area|sidebar|comment|review|rating|ציון|"
    r"elementor-loop|recently-viewed|compare-",
    re.I,
)
MAIN_HINTS = re.compile(r"price|product|money|amount|woocommerce|shopify|sale|מחיר", re.I)
_LOOP_ANCESTOR = re.compile(
    r"related|cross-sells|upsells|elementor-loop|products-related|yith-wcwl|compare-products",
    re.I,
)


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


def _is_inside_script_style(tag: Tag | None) -> bool:
    p = tag
    while p and isinstance(p, Tag):
        if p.name in ("script", "style", "noscript"):
            return True
        p = p.parent
    return False


def _inside_wc_loop_product(el: Tag) -> bool:
    """מחיר בתוך li.product ברשימת מוצרים (קשורים / גריד) — לא המוצר הראשי."""
    li = el.find_parent("li")
    if not li or not isinstance(li, Tag):
        return False
    lic = li.get("class") or []
    if not isinstance(lic, list):
        lic = [str(lic)]
    if "product" not in lic:
        return False
    ul = li.find_parent("ul")
    if not ul or not isinstance(ul, Tag):
        return False
    ucls = " ".join(ul.get("class") or []).lower()
    return "products" in ucls


def _woo_price_node_excluded(el: Tag) -> bool:
    if _is_inside_script_style(el):
        return True
    if _inside_wc_loop_product(el):
        return True
    a: Tag | None = el.parent
    while a and isinstance(a, Tag):
        cls = " ".join(a.get("class") or []).strip()
        aid = (a.get("id") or "").strip()
        hay = f"{cls} {aid}"
        if _LOOP_ANCESTOR.search(hay) or FOOTER_HINTS.search(hay):
            return True
        a = a.parent
    return False


def _main_woocommerce_context_score(el: Tag) -> float:
    """ציון גבוה = סבירות גבוהה שזה מחיר המוצר הראשי בעמוד."""
    s = 0.0
    for a in el.parents:
        if not isinstance(a, Tag):
            continue
        cls_l = " ".join(a.get("class") or []).lower()
        if "elementor-widget-woocommerce-product-price" in cls_l:
            s += 25.0
        if "entry-summary" in cls_l:
            s += 18.0
        if "single-product" in cls_l:
            s += 6.0
        if a.name == "main" or (a.get("id") or "").lower() == "main":
            s += 4.0
        if "summary" in cls_l and "widget" not in cls_l:
            s += 3.0
        if any(
            x in cls_l
            for x in (
                "mini-cart",
                "widget_shopping_cart",
                "cart-dropdown",
                "header-cart",
            )
        ):
            s -= 20.0
    if el.find_parent("ins"):
        s += 4.0
    if el.find_parent("del"):
        s -= 6.0
    return s


def _best_woocommerce_amount_node(soup: BeautifulSoup) -> Tag | None:
    """בוחר את span.woocommerce-Price-amount המתאים למוצר הראשי (לא לולאה / קשורים)."""
    best_sc = -1e9
    best_el: Tag | None = None
    for el in soup.select(".woocommerce-Price-amount"):
        if not isinstance(el, Tag):
            continue
        if _woo_price_node_excluded(el):
            continue
        txt = _element_raw_price_text(el)
        if not parse_price_number(txt):
            continue
        sc = _main_woocommerce_context_score(el)
        if sc > best_sc:
            best_sc = sc
            best_el = el
    return best_el


_WOO_MAIN_SELECTORS = (
    ".elementor-widget-woocommerce-product-price ins .woocommerce-Price-amount bdi",
    ".elementor-widget-woocommerce-product-price ins .woocommerce-Price-amount",
    ".elementor-widget-woocommerce-product-price .woocommerce-Price-amount bdi",
    ".elementor-widget-woocommerce-product-price .woocommerce-Price-amount",
    ".single-product .summary ins .woocommerce-Price-amount bdi",
    ".single-product .summary ins .woocommerce-Price-amount",
    ".single-product .summary .woocommerce-Price-amount bdi",
    ".single-product .summary .woocommerce-Price-amount",
    "main .product .summary ins .woocommerce-Price-amount bdi",
    "main .product .summary .woocommerce-Price-amount bdi",
    "main .product .woocommerce-Price-amount bdi",
    "main .product .woocommerce-Price-amount",
    "#primary .summary .woocommerce-Price-amount bdi",
    "#primary .summary .woocommerce-Price-amount",
    "#primary .woocommerce-Price-amount bdi",
    ".entry-summary ins .woocommerce-Price-amount bdi",
    ".entry-summary .woocommerce-Price-amount bdi",
    ".woocommerce div.product .summary .woocommerce-Price-amount bdi",
    ".woocommerce div.product div.summary .woocommerce-Price-amount bdi",
)


def extract_woocommerce_amount(html: str) -> tuple[float | None, str | None, str]:
    soup = BeautifulSoup(html, "html.parser")
    for q in _WOO_MAIN_SELECTORS:
        sel = soup.select_one(q)
        if not sel or _woo_price_node_excluded(sel):
            continue
        txt = _element_raw_price_text(sel)
        if pn := parse_price_number(txt):
            return pn, None, "woocommerce_class"
    best_el = _best_woocommerce_amount_node(soup)
    if best_el:
        txt = _element_raw_price_text(best_el)
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
    if "elementor-widget-woocommerce-product-price" in combined:
        score += 10.0
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


def _is_dynamic_id(eid: str) -> bool:
    if not eid:
        return True
    if len(eid) > 36:
        return True
    if re.search(r"\d{4,}", eid):
        return True
    return bool(re.search(r"(elementor-|post-\d+|product-\d+|uid-|__|tmp|react)", eid, re.I))


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


def _stable_classes(tag: Tag, *, max_items: int = 2) -> list[str]:
    raw = tag.get("class") or []
    if not isinstance(raw, list):
        raw = [str(raw)]
    out: list[str] = []
    for c in raw:
        c = str(c).strip()
        if not c or len(c) < 3:
            continue
        if re.search(r"(^js-|^is-|^has-|active|selected|current|loaded|hover|focus|open|close)", c, re.I):
            continue
        if re.search(r"\d{4,}", c):
            continue
        out.append(c)
        if len(out) >= max_items:
            break
    return out


def _canonical_selector_for_price_node(el: Tag) -> tuple[str, list[str], str]:
    """
    Selector יציב לשמירה בדומיין:
    - מעדיף span.woocommerce-Price-amount (לא bdi פנימי)
    - מעדיף class paths יציבים
    - נמנע מ-id דינמי
    """
    target = el
    if el.name == "bdi":
        p = el.parent
        if isinstance(p, Tag) and "woocommerce-Price-amount" in " ".join(p.get("class") or []):
            target = p

    tcls = " ".join(target.get("class") or []).lower()
    if "woocommerce-price-amount" in tcls:
        if target.find_parent(class_=re.compile(r"elementor-widget-woocommerce-product-price", re.I)):
            primary = ".elementor-widget-woocommerce-product-price .woocommerce-Price-amount"
            alts = [
                ".single-product .summary .woocommerce-Price-amount",
                ".entry-summary .woocommerce-Price-amount",
                "div.product .summary .woocommerce-Price-amount",
            ]
            return primary, alts, "woo_amount"
        if target.find_parent(class_=re.compile(r"entry-summary|summary", re.I)):
            primary = ".single-product .summary .woocommerce-Price-amount"
            alts = [
                ".entry-summary .woocommerce-Price-amount",
                "div.product .summary .woocommerce-Price-amount",
                ".woocommerce div.product .summary .woocommerce-Price-amount",
            ]
            return primary, alts, "woo_amount"
        return ".woocommerce-Price-amount", [".price .woocommerce-Price-amount"], "woo_amount"

    parts: list[str] = []
    cur: Tag | None = target
    for _ in range(6):
        if cur is None or not isinstance(cur, Tag) or cur.name in ("html", "[document]"):
            break
        eid = (cur.get("id") or "").strip()
        if eid and not _is_dynamic_id(eid):
            parts.append(f"#{_css_escape_ident(eid)}")
            break
        sc = _stable_classes(cur, max_items=2)
        if sc:
            parts.append(cur.name + "".join(f".{_css_escape_ident(c)}" for c in sc))
        else:
            parts.append(cur.name)
        cur = cur.parent if isinstance(cur.parent, Tag) else None
    parts.reverse()
    primary = " > ".join(parts) if parts else build_unique_css_selector(target)
    return primary, [build_unique_css_selector(target)], "class_path"


# מספרים שנראים כמחיר בעמוד מוצר (לא שנים / מיקוד)
_PRICE_IN_TEXT = re.compile(
    r"\d{1,3}(?:[.,\s]\d{3})*(?:[.,]\d{1,2})\b"  # 1,234.56 / 1.234,56
    r"|\d+[.,]\d{1,2}\b"
    r"|\b\d{2,5}\b",
)


def _push_candidate(
    candidates: list[dict[str, Any]],
    seen: set[str],
    parent: Tag,
    chunk: str,
    *,
    bonus: float = 0,
    selector_type: str = "text",
) -> None:
    chunk = chunk.strip()
    if not chunk:
        return
    if not (pn := parse_price_number(chunk)):
        return
    sel, alts, stable_kind = _canonical_selector_for_price_node(parent)
    key = f"{sel}|{pn:.4f}"
    if key in seen:
        return
    seen.add(key)
    inside_summary = bool(parent.find_parent(class_=re.compile(r"\b(summary|entry-summary)\b", re.I)))
    inside_form_cart = bool(parent.find_parent("form", class_=re.compile(r"\bcart\b", re.I)))
    inside_related = bool(parent.find_parent(class_=_LOOP_ANCESTOR))
    inside_loop = _inside_wc_loop_product(parent) or bool(parent.find_parent(class_=re.compile(r"loop|products", re.I)))
    inside_sale = bool(parent.find_parent("ins"))
    inside_strike = bool(parent.find_parent(["del", "s", "strike"])) or parent.name in ("del", "s", "strike")
    focus_depth = _element_depth(parent)

    context_boost = 0.0
    if inside_summary:
        context_boost += 2.2
    if inside_form_cart:
        context_boost += 2.4
    if parent.find_parent(class_=re.compile(r"\b(product|type-product)\b", re.I)):
        context_boost += 2.0
    if parent.find_parent(class_=re.compile(r"elementor-widget-woocommerce-product-price", re.I)):
        context_boost += 3.0
    if parent.find_parent(["article", "div"], class_=re.compile(r"\bproduct\b", re.I)):
        context_boost += 1.3

    noise_score = 0.0
    if inside_related:
        noise_score += 2.5
    if inside_loop:
        noise_score += 2.5
    if inside_strike:
        noise_score += 1.8

    base_score = _score_element(parent)
    final_score = base_score + bonus + context_boost - noise_score
    candidates.append(
        {
            "price_text": chunk,
            "score": round(final_score, 2),
            "selector": sel,
            "selector_alternates": alts,
            "inside_summary": inside_summary,
            "inside_form_cart": inside_form_cart,
            "inside_related": inside_related,
            "inside_loop": inside_loop,
            "inside_sale": inside_sale,
            "inside_strike": inside_strike,
            "focus_depth": focus_depth,
            "noise_score": round(noise_score, 2),
            "selector_type": selector_type if selector_type else stable_kind,
            "score_breakdown": {
                "base": round(base_score, 2),
                "bonus": round(bonus, 2),
                "context_boost": round(context_boost, 2),
                "noise_penalty": round(noise_score, 2),
                "final": round(final_score, 2),
            },
        },
    )


def _collect_attribute_price_candidates(
    soup: BeautifulSoup,
    candidates: list[dict[str, Any]],
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
                        sel, _alts, _kind = _canonical_selector_for_price_node(tag)
                        key = f"{sel}|{v:.4f}|cents"
                        if key not in seen:
                            seen.add(key)
                            _push_candidate(
                                candidates,
                                seen,
                                tag,
                                f"{v:.2f}",
                                bonus=bonus + 2.0,
                                selector_type="attr_cents",
                            )
                        return
            except ValueError:
                pass
        _push_candidate(candidates, seen, tag, val, bonus=bonus + 2.5, selector_type="attr")

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
                _push_candidate(
                    candidates,
                    seen,
                    tag,
                    tag.get_text(" ", strip=True),
                    bonus=2.0,
                    selector_type="microdata",
                )


_EXPLICIT_PRICE_SELECTORS: tuple[tuple[str, float, str], ...] = (
    ("ins .woocommerce-Price-amount", 7.0, "ins_woo"),
    (".woocommerce-Price-amount", 6.0, "woo_amount"),
    ("[itemprop='price']", 5.0, "itemprop_price"),
    ("[data-product-price]", 5.0, "data_product_price"),
    ("[data-price]", 4.5, "data_price"),
    (".money", 4.5, "money"),
    (".amount", 3.8, "amount"),
    (".price", 3.2, "price"),
)


def _collect_explicit_price_like_candidates(
    soup: BeautifulSoup,
    candidates: list[dict[str, Any]],
    seen: set[str],
) -> None:
    for q, bonus, kind in _EXPLICIT_PRICE_SELECTORS:
        for node in soup.select(q):
            if not isinstance(node, Tag):
                continue
            if _is_inside_script_style(node):
                continue
            txt = _element_raw_price_text(node)
            if not txt:
                txt = node.get_text(" ", strip=True)
            if not txt:
                continue
            _push_candidate(candidates, seen, node, txt, bonus=bonus, selector_type=kind)


def collect_price_candidates(html: str, limit: int = 40) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    # שלב 1: explicit מחיריים
    _collect_explicit_price_like_candidates(soup, candidates, seen)
    # שלב 2: attributes (itemprop/data-*)
    _collect_attribute_price_candidates(soup, candidates, seen)
    # שלב 3: text-node רחב (fallback בלבד)
    for el in soup.find_all(string=True):
        if not isinstance(el, NavigableString):
            continue
        parent = el.parent
        if not isinstance(parent, Tag):
            continue
        if _is_inside_script_style(parent):
            continue
        if parent.find_parent(class_=re.compile(r"woocommerce-Price-amount", re.I)):
            continue
        raw = str(el)
        if not raw.strip():
            continue
        for m in _PRICE_IN_TEXT.finditer(raw):
            chunk = m.group(0).strip()
            _push_candidate(candidates, seen, parent, chunk, bonus=0, selector_type="text")

    def _pn(c: dict[str, Any]) -> float:
        v = parse_price_number(str(c.get("price_text") or ""))
        return float(v) if v is not None else 0.0

    candidates.sort(key=lambda c: (-(c.get("score") or 0), -_pn(c)))
    out: list[dict[str, Any]] = []
    for c in candidates[:limit]:
        out.append(c)
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
