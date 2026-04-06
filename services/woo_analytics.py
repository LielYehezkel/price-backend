"""אנליטיקת מכירות מ-WooCommerce — הזמנות אמיתיות למוצרים שמסונכרנים במערכת."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from sqlmodel import Session, select

from backend.models import Product, SalesInsightsCache, Shop, utcnow

log = logging.getLogger(__name__)

# מטמון: החזרה מהירה; רענון ברקע אחרי TTL
SALES_INSIGHTS_CACHE_TTL = timedelta(minutes=45)


def _utc_cutoff_iso(days: int) -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return cutoff.strftime("%Y-%m-%dT%H:%M:%S")


def _fetch_orders_page(
    client: httpx.Client,
    base: str,
    consumer_key: str,
    consumer_secret: str,
    page: int,
    per_page: int,
    after_iso: str,
) -> list[dict[str, Any]]:
    url = f"{base}/wp-json/wc/v3/orders"
    params: dict[str, Any] = {
        "consumer_key": consumer_key,
        "consumer_secret": consumer_secret,
        "page": page,
        "per_page": per_page,
        "after": after_iso,
        "status": "completed,processing,on-hold",
    }
    r = client.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def _line_woo_id(line: dict[str, Any]) -> int | None:
    vid = line.get("variation_id")
    if vid and int(vid) > 0:
        return int(vid)
    pid = line.get("product_id")
    if pid:
        return int(pid)
    return None


def _bucket_label(low: float, high: float, currency: str, last: bool) -> str:
    cur = (currency or "").strip()
    suf = f" {cur}" if cur else ""
    if last:
        return f"{low:.0f} ומעלה{suf}".strip()
    return f"{low:.0f}–{high:.0f}{suf}"


def compute_sales_insights(session: Session, shop_id: int, days: int = 90) -> dict[str, Any]:
    shop = session.get(Shop, shop_id)
    if not shop:
        return {"ok": False, "error": "shop_not_found"}
    if not shop.woo_site_url or not shop.woo_consumer_key or not shop.woo_consumer_secret:
        return {
            "ok": False,
            "error": "woocommerce_not_configured",
            "message_he": "חברו את WooCommerce בהגדרות החנות כדי לראות נתוני מכירות אמיתיים.",
        }

    days = max(7, min(days, 365))
    base = shop.woo_site_url.rstrip("/")
    after_iso = _utc_cutoff_iso(days)

    products = session.exec(select(Product).where(Product.shop_id == shop_id)).all()
    woo_map: dict[int, Product] = {}
    for p in products:
        if p.woo_product_id is not None:
            woo_map[int(p.woo_product_id)] = p

    if not woo_map:
        return {
            "ok": True,
            "woo_connected": True,
            "period_days": days,
            "currency": shop.woo_currency or "",
            "orders_fetched": 0,
            "orders_with_tracked_lines": 0,
            "tracked_line_items": 0,
            "total_revenue_tracked": 0.0,
            "total_units_tracked": 0.0,
            "auto_pricing_revenue": 0.0,
            "auto_pricing_units": 0.0,
            "top_products": [],
            "price_bands": [],
            "period_split": None,
            "methodology_he": "אין מוצרים עם מזהה WooCommerce אחרי סנכרון — סנכרנו מוצרים ונסו שוב.",
        }

    all_orders: list[dict[str, Any]] = []
    page = 1
    per_page = 100
    max_pages = 30
    with httpx.Client(timeout=60.0) as http_client:
        while page <= max_pages:
            batch = _fetch_orders_page(
                http_client,
                base,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                page,
                per_page,
                after_iso,
            )
            if not batch:
                break
            all_orders.extend(batch)
            if len(batch) < per_page:
                break
            page += 1

    by_product_rev: dict[int, float] = defaultdict(float)
    by_product_units: dict[int, float] = defaultdict(float)
    by_product_name: dict[int, str] = {}
    auto_pricing_rev = 0.0
    auto_pricing_units = 0.0
    hist_weighted: list[tuple[float, float]] = []

    mid = datetime.now(timezone.utc) - timedelta(days=days / 2)
    first_half_rev = 0.0
    second_half_rev = 0.0
    tracked_orders = 0

    for order in all_orders:
        if order.get("status") not in ("completed", "processing", "on-hold"):
            continue
        dt_s = order.get("date_created_gmt") or order.get("date_created")
        od: datetime | None = None
        if isinstance(dt_s, str):
            try:
                od = datetime.fromisoformat(dt_s.replace("Z", "+00:00"))
            except ValueError:
                continue
        else:
            continue

        order_tracked = False
        for line in order.get("line_items") or []:
            if not isinstance(line, dict):
                continue
            wid = _line_woo_id(line)
            if wid is None or wid not in woo_map:
                continue
            p = woo_map[wid]
            qty = float(line.get("quantity") or 0)
            total = float(line.get("total") or 0)
            if qty <= 0 or total <= 0:
                continue
            unit = total / qty
            order_tracked = True
            by_product_rev[p.id] += total
            by_product_units[p.id] += qty
            by_product_name[p.id] = p.name or f"#{p.id}"
            hist_weighted.append((unit, total))
            if p.auto_pricing_enabled:
                auto_pricing_rev += total
                auto_pricing_units += qty
            if od >= mid:
                second_half_rev += total
            else:
                first_half_rev += total

        if order_tracked:
            tracked_orders += 1

    total_rev = sum(by_product_rev.values())
    total_units = sum(by_product_units.values())

    top = sorted(by_product_rev.items(), key=lambda x: -x[1])[:15]
    top_products = [
        {
            "product_id": pid,
            "name": by_product_name.get(pid, str(pid)),
            "revenue": round(r, 2),
            "units": round(by_product_units[pid], 2),
        }
        for pid, r in top
    ]

    cur_code = shop.woo_currency or ""
    price_bands: list[dict[str, Any]] = []
    if hist_weighted:
        units_only = sorted(u for u, _ in hist_weighted)
        lo, hi = units_only[0], units_only[-1]
        if hi - lo < 1e-6:
            tot_r = sum(t for _, t in hist_weighted)
            tot_u = sum(t / u for u, t in hist_weighted if u > 0)
            price_bands.append(
                {
                    "label": f"{lo:.2f} {cur_code}".strip(),
                    "revenue": round(tot_r, 2),
                    "units": round(tot_u, 2),
                },
            )
        else:
            n_bins = min(8, max(3, len(units_only) // 4))
            step = (hi - lo) / n_bins
            for i in range(n_bins):
                b_lo = lo + i * step
                b_hi = lo + (i + 1) * step if i < n_bins - 1 else hi + 1e-6
                rev_b = 0.0
                u_b = 0.0
                for unit, tot in hist_weighted:
                    if b_lo <= unit < b_hi or (i == n_bins - 1 and b_lo <= unit <= hi):
                        rev_b += tot
                        u_b += tot / unit if unit > 0 else 0
                if rev_b > 0:
                    price_bands.append(
                        {
                            "label": _bucket_label(b_lo, b_hi, cur_code, i == n_bins - 1),
                            "revenue": round(rev_b, 2),
                            "units": round(u_b, 2),
                        },
                    )

    methodology = (
        "הדוח מבוסס על הזמנות מ-WooCommerce (סטטוסים: הושלם, בעיבוד, בהמתנה) בתוך חלון הימים שנבחר. "
        "נספרות רק שורות שמותאמות למוצרים שמסונכרנים אצלנו (מזהה WooCommerce, כולל וריאציות). "
        "מחיר ליחידה בשורה = סכום השורה חלקי כמות. "
        "טווחי המחיר מראים באיזה רמות מחיר (בפועל בשורת ההזמנה) נרשמה הכנסה — כדי להבין איפה נמכר הכי הרבה. "
        "״הכנסות תחת תמחור אוטומטי״ = מוצרים עם תמחור אוטומטי פעיל בזמן הצגת הדוח (לא היסטוריית הגדרות). "
        "חשוב: מתאם אינו מוכיח שינוי מחיר כסיבה למכירה — זו תמונת מצב עסקית מהדאטה שלך."
    )

    trend_note = ""
    if first_half_rev + second_half_rev > 1 and first_half_rev > 0:
        ch = (second_half_rev - first_half_rev) / first_half_rev * 100
        trend_note = (
            f"חציון זמן: בהשוואה גסה בין חצי התקופה הראשון לשני (לפי תאריך הזמנה), "
            f"ההכנסות מהשורות שעוקבות השתנו בכ־{ch:+.1f}%."
        )

    return {
        "ok": True,
        "woo_connected": True,
        "period_days": days,
        "currency": cur_code,
        "orders_fetched": len(all_orders),
        "orders_with_tracked_lines": tracked_orders,
        "tracked_line_items": len(hist_weighted),
        "total_revenue_tracked": round(total_rev, 2),
        "total_units_tracked": round(total_units, 2),
        "auto_pricing_revenue": round(auto_pricing_rev, 2),
        "auto_pricing_units": round(auto_pricing_units, 2),
        "top_products": top_products,
        "price_bands": price_bands,
        "period_split": {
            "first_half_revenue": round(first_half_rev, 2),
            "second_half_revenue": round(second_half_rev, 2),
            "comparison_note": trend_note or None,
        },
        "methodology_he": methodology,
    }


def attach_cache_meta(
    data: dict[str, Any],
    *,
    fresh: bool,
    stale: bool = False,
    computed_at: datetime | None = None,
) -> dict[str, Any]:
    out = dict(data)
    if out.get("ok") is True:
        ca = computed_at or utcnow()
        if ca.tzinfo is None:
            ca = ca.replace(tzinfo=timezone.utc)
        out["cache"] = {
            "fresh": fresh,
            "stale": stale,
            "computed_at": ca.isoformat(),
        }
    return out


def is_sales_cache_fresh(row: SalesInsightsCache, now: datetime) -> bool:
    u = row.updated_at
    if u.tzinfo is None:
        u = u.replace(tzinfo=timezone.utc)
    return (now - u) < SALES_INSIGHTS_CACHE_TTL


def get_sales_insights_cache_row(session: Session, shop_id: int, days: int) -> SalesInsightsCache | None:
    return session.exec(
        select(SalesInsightsCache).where(
            SalesInsightsCache.shop_id == shop_id,
            SalesInsightsCache.period_days == days,
        ),
    ).first()


def save_sales_insights_cache(session: Session, shop_id: int, days: int, data: dict[str, Any]) -> None:
    clean = {k: v for k, v in data.items() if k != "cache"}
    payload = json.dumps(clean, ensure_ascii=False)
    row = get_sales_insights_cache_row(session, shop_id, days)
    now = utcnow()
    if row:
        row.payload_json = payload
        row.updated_at = now
        session.add(row)
    else:
        session.add(
            SalesInsightsCache(shop_id=shop_id, period_days=days, payload_json=payload, updated_at=now),
        )
    session.commit()


def refresh_sales_insights_cache_task(shop_id: int, days: int) -> None:
    from backend.db import engine

    try:
        with Session(engine) as session:
            data = compute_sales_insights(session, shop_id, days)
            if data.get("ok"):
                save_sales_insights_cache(session, shop_id, days, data)
                log.info("sales insights cache refreshed shop=%s days=%s", shop_id, days)
    except Exception:
        log.exception("sales insights cache refresh failed shop=%s days=%s", shop_id, days)
