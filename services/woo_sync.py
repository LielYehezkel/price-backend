import re
from typing import Any

import httpx


def fetch_wc_products(site_url: str, consumer_key: str, consumer_secret: str) -> list[dict[str, Any]]:
    base = site_url.rstrip("/")
    url = f"{base}/wp-json/wc/v3/products"
    params = {"per_page": 100, "consumer_key": consumer_key, "consumer_secret": consumer_secret}
    out: list[dict[str, Any]] = []
    page = 1
    with httpx.Client(timeout=40.0, follow_redirects=True) as client:
        while True:
            r = client.get(url, params={**params, "page": page})
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            if not isinstance(batch, list):
                break
            out.extend(batch)
            if len(batch) < 100:
                break
            page += 1
            if page > 50:
                break
    return out


def fetch_wc_products_by_ids(
    site_url: str,
    consumer_key: str,
    consumer_secret: str,
    ids: list[int],
) -> dict[int, dict[str, Any]]:
    """משיכה לפי מזהי WooCommerce (`include`). מחזיר מילון id -> אובייקט מוצר מה־API."""
    if not ids:
        return {}
    base = site_url.rstrip("/")
    url = f"{base}/wp-json/wc/v3/products"
    out: dict[int, dict[str, Any]] = {}
    # WooCommerce: עד ~100 פריטים לבקשה יציב
    chunk_size = 100
    with httpx.Client(timeout=90.0, follow_redirects=True) as client:
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i : i + chunk_size]
            params = {
                "consumer_key": consumer_key,
                "consumer_secret": consumer_secret,
                "include": ",".join(str(x) for x in chunk),
                "per_page": 100,
            }
            r = client.get(url, params=params)
            r.raise_for_status()
            batch = r.json()
            if not isinstance(batch, list):
                continue
            for row in batch:
                if isinstance(row, dict) and row.get("id") is not None:
                    try:
                        out[int(row["id"])] = row
                    except (TypeError, ValueError):
                        pass
    return out


def fetch_wc_product_by_id(
    site_url: str,
    consumer_key: str,
    consumer_secret: str,
    woo_product_id: int,
) -> dict[str, Any]:
    base = site_url.rstrip("/")
    url = f"{base}/wp-json/wc/v3/products/{int(woo_product_id)}"
    params = {"consumer_key": consumer_key, "consumer_secret": consumer_secret}
    with httpx.Client(timeout=25.0, follow_redirects=True) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    return data if isinstance(data, dict) else {}


def parse_price(val: Any) -> float | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(s.replace(",", "."))
    except ValueError:
        pass
    # מחיר עם סמל מטבע / טקסט נלווה: "₪ 12.90", "12,90 ₪"
    m = re.search(r"[-+]?\d+(?:[.,]\d+)?", s.replace(",", "."))
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            return None
    return None


def _price_from_range_or_plain(val: Any) -> float | None:
    """טווח מחירים של מוצר משתנה ב־price, למשל '199 – 299' או '199-299'."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    for sep in ("\u2013", "\u2014", " - ", "-", "\u00a0-\u00a0"):
        if sep in s:
            left = s.split(sep, 1)[0].strip()
            return parse_price(left)
    return None


def effective_wc_price(row: dict[str, Any]) -> float | None:
    """מחיר מכירה בפועל: price (כולל מבצע), ואם ריק — sale_price, ואז regular_price.

    מוצר משתנה (variable): לרוב price/range; אם ריק משתמשים ב-min של טווח המחירים.
    """
    raw_price = row.get("price")
    p_now = parse_price(raw_price)
    if p_now is None and raw_price:
        p_now = _price_from_range_or_plain(raw_price)
    if p_now is not None:
        return p_now
    p_sale = parse_price(row.get("sale_price"))
    if p_sale is not None:
        return p_sale
    p_reg = parse_price(row.get("regular_price"))
    if p_reg is not None:
        return p_reg
    # Woo variable product / חלק מהתבניות: טווח בטקסט או שדות min
    p_min_s = parse_price(row.get("min_price"))
    if p_min_s is not None:
        return p_min_s
    p_min_reg = parse_price(row.get("regular_min_price"))
    if p_min_reg is not None:
        return p_min_reg
    return None


def _wc_auth_params(consumer_key: str, consumer_secret: str) -> dict[str, str]:
    return {"consumer_key": consumer_key, "consumer_secret": consumer_secret}


def fetch_wc_store_currency(site_url: str, consumer_key: str, consumer_secret: str) -> str | None:
    """קורא את מטבע ברירת המחדל של החנות מ־WooCommerce REST."""
    base = site_url.rstrip("/")
    url = f"{base}/wp-json/wc/v3/settings/general"
    params = _wc_auth_params(consumer_key, consumer_secret)
    try:
        with httpx.Client(timeout=25.0, follow_redirects=True) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return None
    if not isinstance(data, list):
        return None
    for row in data:
        if not isinstance(row, dict):
            continue
        if row.get("id") == "woocommerce_currency":
            v = row.get("value")
            if isinstance(v, str) and v.strip():
                return v.strip().upper()
    return None


def patch_wc_product_regular_price(
    site_url: str,
    consumer_key: str,
    consumer_secret: str,
    woo_product_id: int,
    regular_price: float,
) -> None:
    base = site_url.rstrip("/")
    url = f"{base}/wp-json/wc/v3/products/{woo_product_id}"
    params = _wc_auth_params(consumer_key, consumer_secret)
    # WooCommerce expects string price
    body = {"regular_price": f"{regular_price:.2f}"}
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        r = client.put(url, params=params, json=body)
        r.raise_for_status()


def patch_wc_product_out_of_stock(
    site_url: str,
    consumer_key: str,
    consumer_secret: str,
    woo_product_id: int,
) -> None:
    base = site_url.rstrip("/")
    url = f"{base}/wp-json/wc/v3/products/{woo_product_id}"
    params = _wc_auth_params(consumer_key, consumer_secret)
    body = {
        "stock_status": "outofstock",
        # For shops that manage stock quantities, this prevents stale in_stock state.
        "manage_stock": False,
    }
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        r = client.put(url, params=params, json=body)
        r.raise_for_status()


def first_product_image_url(row: dict[str, Any]) -> str | None:
    imgs = row.get("images")
    if not isinstance(imgs, list) or not imgs:
        return None
    first = imgs[0]
    if isinstance(first, dict):
        src = first.get("src")
        if isinstance(src, str) and src.startswith(("http://", "https://")):
            return src
    return None
