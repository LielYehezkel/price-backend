"""Shopify Admin REST API helpers (Custom App access token)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
from typing import Any

import httpx

from backend.models import Shop

log = logging.getLogger(__name__)

DEFAULT_API_VERSION = "2024-10"


def normalize_shopify_domain(raw: str) -> str:
    s = (raw or "").strip().lower()
    if s.startswith("https://"):
        s = s[len("https://") :]
    if s.startswith("http://"):
        s = s[len("http://") :]
    s = s.split("/")[0].strip()
    return s


def _api_version(shop: Shop) -> str:
    v = (getattr(shop, "shopify_api_version", None) or "").strip()
    return v or DEFAULT_API_VERSION


def admin_base_url(shop: Shop) -> str:
    dom = normalize_shopify_domain(getattr(shop, "shopify_shop_domain", None) or "")
    if not dom:
        raise ValueError("shopify_shop_domain missing")
    return f"https://{dom}/admin/api/{_api_version(shop)}"


def _headers(shop: Shop) -> dict[str, str]:
    tok = (getattr(shop, "shopify_admin_access_token", None) or "").strip()
    if not tok:
        raise ValueError("shopify_admin_access_token missing")
    return {
        "X-Shopify-Access-Token": tok,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Cache-Control": "no-cache",
    }


def shopify_request(
    shop: Shop,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: float = 45.0,
) -> httpx.Response:
    base = admin_base_url(shop)
    url = f"{base}{path if path.startswith('/') else '/' + path}"
    if params is None:
        params = {}
    params = {**params, "_ts": int(time.time() * 1000)}
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.request(method.upper(), url, headers=_headers(shop), params=params, json=json_body)
    return r


def verify_shop_credentials(shop_domain: str, access_token: str, api_version: str | None = None) -> dict[str, Any]:
    dom = normalize_shopify_domain(shop_domain)
    ver = (api_version or DEFAULT_API_VERSION).strip() or DEFAULT_API_VERSION
    url = f"https://{dom}/admin/api/{ver}/shop.json"
    headers = {
        "X-Shopify-Access-Token": access_token.strip(),
        "Accept": "application/json",
        "Cache-Control": "no-cache",
    }
    with httpx.Client(timeout=25.0, follow_redirects=True) as client:
        r = client.get(url, headers=headers, params={"_ts": int(time.time() * 1000)})
    r.raise_for_status()
    data = r.json()
    shop = data.get("shop") if isinstance(data, dict) else None
    if not isinstance(shop, dict):
        raise ValueError("invalid shop response")
    return shop


def fetch_shop_currency_code(shop: Shop) -> str | None:
    try:
        row = verify_shop_credentials(
            getattr(shop, "shopify_shop_domain", "") or "",
            getattr(shop, "shopify_admin_access_token", "") or "",
            getattr(shop, "shopify_api_version", None),
        )
        cur = str(row.get("currency") or "").strip().upper()
        return cur or None
    except Exception:
        log.exception("fetch_shop_currency_code failed")
        return None


def parse_shopify_money(val: Any) -> float | None:
    if val is None:
        return None
    try:
        s = str(val).strip().replace(",", "")
        if not s:
            return None
        return float(s)
    except ValueError:
        return None


def variant_to_woo_like_row(variant: dict[str, Any]) -> dict[str, Any]:
    """Map Shopify variant JSON to keys similar to Woo product for shared AI logic."""
    price_f = parse_shopify_money(variant.get("price"))
    comp_f = parse_shopify_money(variant.get("compare_at_price"))
    on_sale = False
    if comp_f is not None and price_f is not None and comp_f > price_f + 1e-9:
        on_sale = True
    regular = comp_f if on_sale else price_f
    sale = price_f if on_sale else None
    inv = variant.get("inventory_quantity")
    try:
        inv_n = int(inv) if inv is not None else None
    except (TypeError, ValueError):
        inv_n = None
    stock_status = "instock"
    if inv_n is not None and inv_n <= 0:
        stock_status = "outofstock"
    return {
        "regular_price": regular,
        "sale_price": sale,
        "on_sale": on_sale,
        "type": "simple",
        "stock_status": stock_status,
        "inventory_quantity": inv_n,
    }


def first_variant(product: dict[str, Any]) -> dict[str, Any] | None:
    vars_ = product.get("variants")
    if not isinstance(vars_, list) or not vars_:
        return None
    v0 = vars_[0]
    return v0 if isinstance(v0, dict) else None


def first_product_image_url(product: dict[str, Any]) -> str | None:
    imgs = product.get("images")
    if isinstance(imgs, list) and imgs:
        im0 = imgs[0]
        if isinstance(im0, dict):
            return str(im0.get("src") or "") or None
    return None


def list_all_products(shop: Shop) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    since_id: int | None = None
    for _ in range(80):
        params: dict[str, Any] = {"limit": 250, "status": "active"}
        if since_id is not None:
            params["since_id"] = since_id
        r = shopify_request(shop, "GET", "/products.json", params=params)
        r.raise_for_status()
        data = r.json()
        batch = data.get("products") if isinstance(data, dict) else None
        if not isinstance(batch, list) or not batch:
            break
        out.extend([p for p in batch if isinstance(p, dict)])
        if len(batch) < 250:
            break
        last = batch[-1].get("id")
        try:
            since_id = int(last) if last is not None else None
        except (TypeError, ValueError):
            break
        if since_id is None:
            break
    return out


def get_variant(shop: Shop, variant_id: int) -> dict[str, Any]:
    r = shopify_request(shop, "GET", f"/variants/{int(variant_id)}.json")
    r.raise_for_status()
    data = r.json()
    v = data.get("variant") if isinstance(data, dict) else None
    if not isinstance(v, dict):
        raise ValueError("variant not found")
    return v


def update_variant_prices(
    shop: Shop,
    variant_id: int,
    *,
    price: float,
    compare_at_price: str | None,
) -> dict[str, Any]:
    body = {
        "variant": {
            "id": int(variant_id),
            "price": f"{float(price):.2f}",
        },
    }
    if compare_at_price is None:
        body["variant"]["compare_at_price"] = None
    else:
        body["variant"]["compare_at_price"] = compare_at_price
    r = shopify_request(shop, "PUT", f"/variants/{int(variant_id)}.json", json_body=body)
    r.raise_for_status()
    data = r.json()
    v = data.get("variant") if isinstance(data, dict) else None
    if not isinstance(v, dict):
        raise ValueError("variant update response invalid")
    return v


def get_primary_location_id(shop: Shop) -> int:
    r = shopify_request(shop, "GET", "/locations.json")
    r.raise_for_status()
    data = r.json()
    locs = data.get("locations") if isinstance(data, dict) else None
    if not isinstance(locs, list) or not locs:
        raise ValueError("no Shopify locations")
    for loc in locs:
        if not isinstance(loc, dict):
            continue
        if loc.get("active") is False:
            continue
        lid = loc.get("id")
        if lid is not None:
            return int(lid)
    lid0 = locs[0].get("id") if isinstance(locs[0], dict) else None
    if lid0 is None:
        raise ValueError("invalid locations payload")
    return int(lid0)


def get_inventory_level_available(shop: Shop, inventory_item_id: int) -> int:
    r = shopify_request(
        shop,
        "GET",
        "/inventory_levels.json",
        params={"inventory_item_ids": int(inventory_item_id)},
    )
    r.raise_for_status()
    data = r.json()
    levels = data.get("inventory_levels") if isinstance(data, dict) else None
    if not isinstance(levels, list) or not levels:
        return 0
    total = 0
    for lv in levels:
        if isinstance(lv, dict) and lv.get("available") is not None:
            try:
                total += int(lv["available"])
            except (TypeError, ValueError):
                pass
    return total


def set_inventory_available(shop: Shop, inventory_item_id: int, location_id: int, available: int) -> None:
    body = {
        "location_id": int(location_id),
        "inventory_item_id": int(inventory_item_id),
        "available": max(0, int(available)),
    }
    r = shopify_request(shop, "POST", "/inventory_levels/set.json", json_body=body)
    r.raise_for_status()


def verify_webhook_hmac(raw_body: bytes, hmac_header: str | None, secret: str) -> bool:
    if not secret or not hmac_header:
        return False
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected.strip(), (hmac_header or "").strip())


def list_orders_since(shop: Shop, *, created_at_min_iso: str, max_pages: int = 40) -> list[dict[str, Any]]:
    """Orders for analytics (paginated by since_id, filtered by created_at_min on first page)."""
    out: list[dict[str, Any]] = []
    since_id: int | None = None
    for _ in range(max_pages):
        params: dict[str, Any] = {"limit": 250, "status": "any", "created_at_min": created_at_min_iso}
        if since_id is not None:
            params["since_id"] = since_id
        r = shopify_request(shop, "GET", "/orders.json", params=params)
        r.raise_for_status()
        data = r.json()
        batch = data.get("orders") if isinstance(data, dict) else None
        if not isinstance(batch, list) or not batch:
            break
        out.extend([o for o in batch if isinstance(o, dict)])
        if len(batch) < 250:
            break
        last = batch[-1].get("id")
        try:
            since_id = int(last) if last is not None else None
        except (TypeError, ValueError):
            break
        if since_id is None:
            break
    return out
