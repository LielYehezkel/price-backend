"""תמחור אוטומטי מול מתחרים — עדכון מחיר ב־WooCommerce כשמתקיימים חוקים."""

from __future__ import annotations

import logging

from sqlmodel import Session, select

from backend.models import Alert, CompetitorLink, Product, Shop
from backend.services.domain_policy import domain_from_url, domain_is_live
from backend.services.woo_sync import patch_wc_product_regular_price

logger = logging.getLogger(__name__)


def _live_competitor_lowest_price(session: Session, product_id: int) -> float | None:
    comps = session.exec(select(CompetitorLink).where(CompetitorLink.product_id == product_id)).all()
    prices: list[float] = []
    for c in comps:
        if not domain_is_live(session, domain_from_url(c.url)):
            continue
        if c.last_price is not None and c.last_price > 0:
            prices.append(float(c.last_price))
    if not prices:
        return None
    return min(prices)


def _anchor_target_price(product: Product, comp_low: float) -> float | None:
    """מחיר יעד לפי כלל הפעולה (אחוז או סכום מתחת למחיר המתחרה הנמוך)."""
    act = product.auto_pricing_action_value
    floor = product.auto_pricing_min_price
    if act is None or act < 0:
        return None
    if floor is None or floor <= 0:
        return None
    if product.auto_pricing_action_kind == "percent":
        target = comp_low * (1.0 - act / 100.0)
    else:
        target = comp_low - act
    target = max(target, float(floor))
    if target <= 0:
        return None
    return round(target, 2)


def _compute_new_price(product: Product, comp_low: float) -> float | None:
    """מחזיר מחיר יעד חדש או None אם אין שינוי."""
    if not product.auto_pricing_enabled:
        return None
    our = product.regular_price
    if our is None or our <= 0:
        return None
    trig = product.auto_pricing_trigger_value
    act = product.auto_pricing_action_value
    floor = product.auto_pricing_min_price
    if act is None or act < 0:
        return None
    if floor is None or floor <= 0:
        return None

    strategy = getattr(product, "auto_pricing_strategy", None) or "reactive_down"

    if strategy == "smart_anchor":
        target = _anchor_target_price(product, comp_low)
        if target is None:
            return None
        if abs(target - float(our)) < 0.005:
            return None
        return target

    # reactive_down — דורש תנאי trigger; משנה רק כשמורידים
    if trig is None or trig < 0:
        return None

    if product.auto_pricing_trigger_kind == "percent":
        gap_ratio = (our - comp_low) / our
        if gap_ratio < trig / 100.0 - 1e-9:
            return None
    else:
        if (our - comp_low) < trig - 1e-9:
            return None

    if product.auto_pricing_action_kind == "percent":
        target = comp_low * (1.0 - act / 100.0)
    else:
        target = comp_low - act

    target = max(target, float(floor))

    if target <= 0:
        return None

    if target >= our - 1e-6:
        return None

    return round(target, 2)


def maybe_apply_auto_pricing(session: Session, product_id: int) -> bool:
    """
    אחרי עדכון מחירי מתחרים: אם מופעל תמחור אוטומטי — מעדכן WooCommerce ומקומית.
    """
    product = session.get(Product, product_id)
    if not product or not product.auto_pricing_enabled:
        return False
    if not product.woo_product_id:
        return False

    shop = session.get(Shop, product.shop_id)
    if not shop or not shop.woo_site_url or not shop.woo_consumer_key or not shop.woo_consumer_secret:
        return False

    comp_low = _live_competitor_lowest_price(session, product_id)
    if comp_low is None:
        return False

    new_price = _compute_new_price(product, comp_low)
    if new_price is None:
        return False

    old = product.regular_price
    strategy = getattr(product, "auto_pricing_strategy", None) or "reactive_down"
    try:
        patch_wc_product_regular_price(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(product.woo_product_id),
            new_price,
        )
    except Exception:
        logger.exception("auto_pricing: WooCommerce update failed product_id=%s", product_id)
        return False

    product.regular_price = new_price
    session.add(product)
    if strategy == "smart_anchor" and new_price > float(old or 0) + 1e-6:
        msg = (
            f"תמחור חכם (עוגן): המחיר עודכן מ־{old} ל־{new_price} "
            f"— השוק עלה; שומרים על הפרש מול המחיר הנמוך ({comp_low}) — {product.name}"
        )
    elif new_price < float(old or 0) - 1e-6:
        msg = (
            f"תמחור אוטומטי: המחיר הותאם מ־{old} ל־{new_price} "
            f"(מתחרה נמוך ביותר בדומיינים פעילים: {comp_low}) — {product.name}"
        )
    else:
        msg = (
            f"תמחור אוטומטי: המחיר עודכן מ־{old} ל־{new_price} "
            f"(מתחרה נמוך ביותר: {comp_low}) — {product.name}"
        )
    session.add(
        Alert(
            shop_id=shop.id,
            product_id=product.id,
            message=msg,
            severity="info",
            kind="auto_pricing",
        ),
    )
    session.commit()
    return True
