"""Unified store operations: WordPress/WooCommerce vs Shopify."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, status
from sqlmodel import Session, select

from backend.models import Product, Shop, utcnow
from backend.services import shopify_sync
from backend.services.woo_sync import (
    effective_wc_price,
    fetch_wc_product_by_id,
    fetch_wc_products,
    fetch_wc_products_by_ids,
    fetch_wc_store_currency,
    first_product_image_url as woo_first_image,
)

log = logging.getLogger(__name__)

PLATFORM_WORDPRESS = "wordpress"
PLATFORM_SHOPIFY = "shopify"


def store_platform(shop: Shop) -> str:
    s = (getattr(shop, "store_platform", None) or PLATFORM_WORDPRESS).strip().lower()
    return s if s in (PLATFORM_WORDPRESS, PLATFORM_SHOPIFY) else PLATFORM_WORDPRESS


def ensure_store_connected(shop: Shop) -> None:
    if store_platform(shop) == PLATFORM_SHOPIFY:
        dom = shopify_sync.normalize_shopify_domain(getattr(shop, "shopify_shop_domain", None) or "")
        tok = (getattr(shop, "shopify_admin_access_token", None) or "").strip()
        if not dom or not tok:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "יש לשמור דומיין Shopify (myshopify.com) וטוקן Admin API בהגדרות החנות.",
            )
        return
    if not shop.woo_site_url or not shop.woo_consumer_key or not shop.woo_consumer_secret:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "יש לשמור פרטי WooCommerce בהגדרות החנות כדי לבצע פעולה זו.",
        )


def product_has_store_link(shop: Shop, p: Product) -> bool:
    if store_platform(shop) == PLATFORM_SHOPIFY:
        return p.shopify_variant_id is not None
    return p.woo_product_id is not None


def fetch_catalog_row(shop: Shop, p: Product) -> dict[str, Any]:
    if store_platform(shop) == PLATFORM_SHOPIFY:
        if not p.shopify_variant_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "למוצר אין מזהה וריאנט Shopify.")
        v = shopify_sync.get_variant(shop, int(p.shopify_variant_id))
        row = shopify_sync.variant_to_woo_like_row(v)
        row["price"] = v.get("price")
        return row
    if not p.woo_product_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "למוצר אין מזהה WooCommerce.")
    return fetch_wc_product_by_id(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        int(p.woo_product_id),
    )


def effective_catalog_price(row: dict[str, Any]) -> float | None:
    return effective_wc_price(row)


def sync_products_from_store(session: Session, shop: Shop) -> int:
    if store_platform(shop) == PLATFORM_SHOPIFY:
        ensure_store_connected(shop)
        cur = shopify_sync.fetch_shop_currency_code(shop)
        if cur:
            shop.woo_currency = cur
            session.add(shop)
        rows = shopify_sync.list_all_products(shop)
        n = 0
        for r in rows:
            if not isinstance(r, dict):
                continue
            pid = r.get("id")
            v0 = shopify_sync.first_variant(r)
            if pid is None or not v0:
                continue
            try:
                wid = int(pid)
                vid = int(v0.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if vid <= 0:
                continue
            inv_item = v0.get("inventory_item_id")
            try:
                inv_item_i = int(inv_item) if inv_item is not None else None
            except (TypeError, ValueError):
                inv_item_i = None
            name = str(r.get("title") or "—")
            sku = str(v0.get("sku") or "").strip() or None
            handle = str(r.get("handle") or "").strip()
            dom = shopify_sync.normalize_shopify_domain(getattr(shop, "shopify_shop_domain", "") or "")
            permalink = f"https://{dom}/products/{handle}" if handle else None
            row = shopify_sync.variant_to_woo_like_row(v0)
            price = row.get("regular_price")
            if row.get("on_sale") and row.get("sale_price") is not None:
                price = effective_wc_price({**row, "price": v0.get("price")})
            img = shopify_sync.first_product_image_url(r)
            cat = str(r.get("product_type") or "").strip() or None
            existing = session.exec(
                select(Product).where(Product.shop_id == shop.id, Product.shopify_product_id == wid),
            ).first()
            if existing:
                existing.name = name
                existing.sku = sku
                existing.permalink = permalink
                old_price = existing.regular_price
                if old_price is None or price is None:
                    changed = old_price != price
                else:
                    changed = abs(float(old_price) - float(price)) > 0.005
                if changed:
                    existing.regular_price = price
                existing.last_price_sync_at = utcnow()
                existing.image_url = img
                existing.category_name = cat
                existing.category_path = cat
                existing.shopify_variant_id = vid
                existing.shopify_inventory_item_id = inv_item_i
                session.add(existing)
            else:
                session.add(
                    Product(
                        shop_id=shop.id,
                        woo_product_id=None,
                        shopify_product_id=wid,
                        shopify_variant_id=vid,
                        shopify_inventory_item_id=inv_item_i,
                        name=name,
                        sku=sku,
                        permalink=permalink,
                        image_url=img,
                        category_name=cat,
                        category_path=cat,
                        regular_price=price,
                        last_price_sync_at=utcnow(),
                    ),
                )
            n += 1
        session.commit()
        return n

    # WordPress / WooCommerce — caller may commit; mirror existing sync_shop body
    if not shop.woo_site_url or not shop.woo_consumer_key or not shop.woo_consumer_secret:
        raise HTTPException(400, "יש לשמור פרטי WooCommerce בהגדרות")
    rows = fetch_wc_products(shop.woo_site_url, shop.woo_consumer_key, shop.woo_consumer_secret)
    cur = fetch_wc_store_currency(shop.woo_site_url, shop.woo_consumer_key, shop.woo_consumer_secret)
    if cur:
        shop.woo_currency = cur
        session.add(shop)
    n = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        wid = r.get("id")
        name = r.get("name") or "—"
        sku = r.get("sku")
        link = r.get("permalink")
        from backend.services.woo_sync import effective_wc_price as eff

        price = eff(r)
        img = woo_first_image(r)
        cats_raw = r.get("categories")
        cat_names: list[str] = []
        if isinstance(cats_raw, list):
            for c in cats_raw:
                if isinstance(c, dict):
                    nm = str(c.get("name") or "").strip()
                    if nm:
                        cat_names.append(nm)
        category_name = cat_names[0] if cat_names else None
        category_path = " > ".join(cat_names) if cat_names else None
        existing = session.exec(
            select(Product).where(Product.shop_id == shop.id, Product.woo_product_id == wid),
        ).first()
        if existing:
            existing.name = str(name)
            existing.sku = str(sku) if sku else None
            existing.permalink = str(link) if link else None
            old_price = existing.regular_price
            if old_price is None or price is None:
                changed = old_price != price
            else:
                changed = abs(float(old_price) - float(price)) > 0.005
            if changed:
                existing.regular_price = price
            existing.last_price_sync_at = utcnow()
            existing.image_url = img
            existing.category_name = category_name
            existing.category_path = category_path
            session.add(existing)
        else:
            session.add(
                Product(
                    shop_id=shop.id,
                    woo_product_id=int(wid) if wid is not None else None,
                    name=str(name),
                    sku=str(sku) if sku else None,
                    permalink=str(link) if link else None,
                    image_url=img,
                    category_name=category_name,
                    category_path=category_path,
                    regular_price=price,
                    last_price_sync_at=utcnow(),
                ),
            )
        n += 1
    session.commit()
    return n


def refresh_product_price_if_stale(session: Session, shop: Shop, product: Product, *, ttl_seconds: int = 120) -> None:
    """Refresh local Product.regular_price from the remote catalog when stale."""
    last = getattr(product, "last_price_sync_at", None)
    if last is not None:
        from datetime import timezone as _tz

        la = last if last.tzinfo else last.replace(tzinfo=_tz.utc)
        if (utcnow() - la).total_seconds() < ttl_seconds:
            return
    if not product.woo_product_id and not product.shopify_variant_id:
        return
    try:
        ensure_store_connected(shop)
        row = fetch_catalog_row(shop, product)
        new_price = effective_wc_price(row)
        old_price = product.regular_price
        if old_price is None or new_price is None:
            changed = old_price != new_price
        else:
            changed = abs(float(old_price) - float(new_price)) > 0.005
        if changed:
            product.regular_price = new_price
        product.last_price_sync_at = utcnow()
        session.add(product)
        session.commit()
        session.refresh(product)
    except Exception:
        log.exception("refresh_product_price_if_stale failed product_id=%s", product.id)


def apply_regular_price(shop: Shop, p: Product, price: float) -> dict[str, Any]:
    if store_platform(shop) == PLATFORM_SHOPIFY:
        if not p.shopify_variant_id:
            raise HTTPException(400, "חסר מזהה וריאנט Shopify")
        shopify_sync.update_variant_prices(shop, int(p.shopify_variant_id), price=float(price), compare_at_price=None)
        return shopify_sync.get_variant(shop, int(p.shopify_variant_id))
    from backend.services.woo_sync import patch_wc_product_regular_price

    patch_wc_product_regular_price(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        int(p.woo_product_id),
        float(price),
    )
    return fetch_wc_product_by_id(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        int(p.woo_product_id),
    )


def apply_sale_prices(shop: Shop, p: Product, *, regular_price: float, sale_price: float) -> dict[str, Any]:
    if store_platform(shop) == PLATFORM_SHOPIFY:
        if not p.shopify_variant_id:
            raise HTTPException(400, "חסר מזהה וריאנט Shopify")
        comp = f"{float(regular_price):.2f}"
        shopify_sync.update_variant_prices(
            shop,
            int(p.shopify_variant_id),
            price=float(sale_price),
            compare_at_price=comp,
        )
        return shopify_sync.get_variant(shop, int(p.shopify_variant_id))
    from backend.services.woo_sync import patch_wc_product_prices

    patch_wc_product_prices(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        int(p.woo_product_id),
        regular_price=float(regular_price),
        sale_price=float(sale_price),
        clear_sale_schedule=True,
    )
    return fetch_wc_product_by_id(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        int(p.woo_product_id),
    )


def apply_sale_price_only(shop: Shop, p: Product, sale_price: float, *, regular_hint: float) -> dict[str, Any]:
    reg = max(float(regular_hint), float(sale_price) + 10.0)
    return apply_sale_prices(shop, p, regular_price=reg, sale_price=float(sale_price))


def set_product_out_of_stock(shop: Shop, p: Product) -> None:
    if store_platform(shop) == PLATFORM_SHOPIFY:
        if not p.shopify_inventory_item_id:
            v = shopify_sync.get_variant(shop, int(p.shopify_variant_id or 0))
            iid = v.get("inventory_item_id")
            p.shopify_inventory_item_id = int(iid) if iid is not None else None
            if not p.shopify_inventory_item_id:
                raise HTTPException(400, "למוצר Shopify אין inventory_item_id")
        loc = shopify_sync.get_primary_location_id(shop)
        shopify_sync.set_inventory_available(shop, int(p.shopify_inventory_item_id), loc, 0)
        return
    from backend.services.woo_sync import patch_wc_product_out_of_stock

    patch_wc_product_out_of_stock(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        int(p.woo_product_id),
    )


def set_product_in_stock(shop: Shop, p: Product, *, default_qty: int = 1) -> None:
    if store_platform(shop) == PLATFORM_SHOPIFY:
        if not p.shopify_inventory_item_id:
            v = shopify_sync.get_variant(shop, int(p.shopify_variant_id or 0))
            iid = v.get("inventory_item_id")
            p.shopify_inventory_item_id = int(iid) if iid is not None else None
        if not p.shopify_inventory_item_id:
            raise HTTPException(400, "למוצר Shopify אין inventory_item_id")
        loc = shopify_sync.get_primary_location_id(shop)
        shopify_sync.set_inventory_available(shop, int(p.shopify_inventory_item_id), loc, max(1, int(default_qty)))
        return
    from backend.services.woo_sync import patch_wc_product_in_stock

    patch_wc_product_in_stock(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        int(p.woo_product_id),
    )


def catalog_row_after_variant_update(shop: Shop, p: Product, v: dict[str, Any]) -> dict[str, Any]:
    row = shopify_sync.variant_to_woo_like_row(v)
    row["price"] = v.get("price")
    return row


def shopify_confirm_price_change(
    shop: Shop,
    p: Product,
    *,
    action: str,
    price_field: str,
    to_price: float,
    row_before: dict[str, Any],
) -> dict[str, Any]:
    """Apply AI chat price confirm for Shopify; returns woo-like row_after."""
    from backend.services.woo_sync import parse_price

    if not p.shopify_variant_id:
        raise HTTPException(400, "חסר מזהה וריאנט Shopify")
    sale_before = parse_price(row_before.get("sale_price"))
    reg_before = parse_price(row_before.get("regular_price"))

    if price_field == "sale_price":
        if action == "increase_price":
            spread = 10.0
            if reg_before is not None and sale_before is not None:
                spread = max(float(reg_before) - float(sale_before), 10.0)
            desired_regular = float(to_price + spread)
            if reg_before is None or to_price >= float(reg_before):
                apply_sale_prices(shop, p, regular_price=desired_regular, sale_price=float(to_price))
            else:
                apply_sale_prices(shop, p, regular_price=float(reg_before), sale_price=float(to_price))
        else:
            if reg_before is not None and reg_before > to_price + 1e-9:
                apply_sale_prices(shop, p, regular_price=float(reg_before), sale_price=float(to_price))
            else:
                apply_sale_prices(shop, p, regular_price=float(to_price + 10.0), sale_price=float(to_price))
    else:
        apply_regular_price(shop, p, float(to_price))

    v = shopify_sync.get_variant(shop, int(p.shopify_variant_id))
    return catalog_row_after_variant_update(shop, p, v)


def refresh_all_product_prices(session: Session, shop: Shop) -> dict[str, int]:
    """Refresh regular_price from remote catalog for all linked products (Woo or Shopify)."""
    ensure_store_connected(shop)
    if store_platform(shop) == PLATFORM_SHOPIFY:
        rows = list(
            session.exec(
                select(Product).where(
                    Product.shop_id == int(shop.id or 0),
                    Product.shopify_variant_id.is_not(None),
                ),
            ).all(),
        )
        if not rows:
            return {"checked": 0, "updated": 0, "missing_in_woo": 0}
        now = utcnow()
        updated = 0
        missing = 0
        for p in rows:
            try:
                v = shopify_sync.get_variant(shop, int(p.shopify_variant_id or 0))
            except Exception:
                missing += 1
                continue
            row = shopify_sync.variant_to_woo_like_row(v)
            row["price"] = v.get("price")
            new_price = effective_wc_price(row)
            old_price = p.regular_price
            if old_price is None or new_price is None:
                price_changed = old_price != new_price
            else:
                price_changed = abs(float(old_price) - float(new_price)) > 0.005
            if price_changed:
                p.regular_price = new_price
                updated += 1
            p.last_price_sync_at = now
            iid = v.get("inventory_item_id")
            if iid is not None:
                try:
                    p.shopify_inventory_item_id = int(iid)
                except (TypeError, ValueError):
                    pass
            session.add(p)
        cur = shopify_sync.fetch_shop_currency_code(shop)
        if cur:
            shop.woo_currency = cur
            session.add(shop)
        session.commit()
        return {"checked": len(rows), "updated": updated, "missing_in_woo": missing}

    rows = list(
        session.exec(
            select(Product).where(
                Product.shop_id == int(shop.id or 0),
                Product.woo_product_id.is_not(None),
            ),
        ).all(),
    )
    woo_ids = sorted({int(p.woo_product_id) for p in rows if p.woo_product_id is not None})
    if not woo_ids:
        return {"checked": 0, "updated": 0, "missing_in_woo": 0}
    wc_by_id = fetch_wc_products_by_ids(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        woo_ids,
    )
    cur = fetch_wc_store_currency(shop.woo_site_url, shop.woo_consumer_key, shop.woo_consumer_secret)
    if cur:
        shop.woo_currency = cur
        session.add(shop)
    now = utcnow()
    updated = 0
    missing = 0
    for p in rows:
        if p.woo_product_id is None:
            continue
        wid = int(p.woo_product_id)
        row = wc_by_id.get(wid)
        if not row:
            missing += 1
            continue
        new_price = effective_wc_price(row)
        old_price = p.regular_price
        if old_price is None or new_price is None:
            price_changed = old_price != new_price
        else:
            price_changed = abs(float(old_price) - float(new_price)) > 0.005
        if price_changed:
            p.regular_price = new_price
            updated += 1
        p.last_price_sync_at = now
        session.add(p)
    session.commit()
    return {"checked": len(rows), "updated": updated, "missing_in_woo": missing}


def restore_prices_from_snapshot(shop: Shop, p: Product, before: dict[str, Any]) -> None:
    """Restore remote catalog prices to a prior snapshot (used by AI action undo)."""
    from backend.services.woo_sync import parse_price

    if store_platform(shop) != PLATFORM_SHOPIFY:
        raise ValueError("restore_prices_from_snapshot is Shopify-only; use Woo patch helpers for WordPress.")
    if not p.shopify_variant_id:
        raise ValueError("missing shopify_variant_id")
    reg = parse_price(before.get("regular_price"))
    sale = parse_price(before.get("sale_price"))
    if sale is not None and float(sale) > 0 and reg is not None and float(reg) > float(sale) + 1e-9:
        apply_sale_prices(shop, p, regular_price=float(reg), sale_price=float(sale))
    elif reg is not None:
        apply_regular_price(shop, p, float(reg))
    else:
        raise ValueError("missing previous price snapshot for Shopify undo")


def woo_patch_sale_price_only(shop: Shop, p: Product, to_price: float) -> dict[str, Any]:
    from backend.services.woo_sync import fetch_wc_product_by_id, patch_wc_product_sale_price

    patch_wc_product_sale_price(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        int(p.woo_product_id),
        float(to_price),
    )
    return fetch_wc_product_by_id(
        shop.woo_site_url,
        shop.woo_consumer_key,
        shop.woo_consumer_secret,
        int(p.woo_product_id),
    )
