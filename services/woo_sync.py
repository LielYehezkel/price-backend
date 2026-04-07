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


def parse_price(val: Any) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", "."))
    except ValueError:
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
