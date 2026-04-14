from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlmodel import Session, select

from backend.models import Alert, Shop, ShopSalesNotificationLog, ShopWhatsappConfig, UserShopPreferences, utcnow
from backend.services.whatsapp_cloud import send_test_text_message


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _resolve_alert_phone(session: Session, cfg: ShopWhatsappConfig) -> str | None:
    p = (cfg.alert_phone_e164 or "").strip()
    if p:
        return p
    return None


def _already_sent(session: Session, shop_id: int, kind: str, key: str) -> bool:
    row = session.exec(
        select(ShopSalesNotificationLog).where(
            ShopSalesNotificationLog.shop_id == shop_id,
            ShopSalesNotificationLog.event_kind == kind,
            ShopSalesNotificationLog.event_key == key,
        ),
    ).first()
    return row is not None


def _mark_sent(session: Session, shop_id: int, kind: str, key: str) -> None:
    session.add(ShopSalesNotificationLog(shop_id=shop_id, event_kind=kind, event_key=key))
    session.commit()


def _owner_prefs(session: Session, shop: Shop) -> UserShopPreferences | None:
    owner_id = int(shop.owner_id or 0)
    if owner_id <= 0 or not shop.id:
        return None
    return session.exec(
        select(UserShopPreferences).where(
            UserShopPreferences.user_id == owner_id,
            UserShopPreferences.shop_id == int(shop.id),
        ),
    ).first()


def _fmt_money(amount: float | int | None, currency: str | None) -> str:
    try:
        a = float(amount or 0.0)
    except Exception:
        a = 0.0
    cur = (currency or "").strip()
    return f"{a:,.2f} {cur}".strip()


def handle_woo_sale_event(
    session: Session,
    cfg: ShopWhatsappConfig,
    payload: dict[str, Any],
) -> bool:
    """Send live sale alert from Woo webhook payload."""
    if not cfg.enabled or not cfg.access_token or not cfg.phone_number_id or not cfg.shop_id:
        return False
    shop = session.get(Shop, cfg.shop_id)
    if not shop:
        return False
    prefs = _owner_prefs(session, shop)
    if not prefs or not prefs.notify_sale_live:
        return False
    to_phone = _resolve_alert_phone(session, cfg)
    if not to_phone:
        return False

    order_id = payload.get("id")
    if order_id is None:
        return False
    event_key = str(order_id)
    if _already_sent(session, int(cfg.shop_id), "sale_live", event_key):
        return True

    status = str(payload.get("status") or "")
    if status not in {"completed", "processing", "on-hold"}:
        return False
    total = payload.get("total")
    currency = payload.get("currency") or shop.woo_currency or ""
    line_items = payload.get("line_items") if isinstance(payload.get("line_items"), list) else []
    first_name = ""
    if line_items:
        li0 = line_items[0]
        if isinstance(li0, dict):
            first_name = str(li0.get("name") or "")
    items_count = len(line_items)
    msg = (
        "מכירה חדשה התקבלה!\n"
        f"הזמנה: #{order_id}\n"
        f"סכום: {_fmt_money(total, str(currency))}\n"
        f"פריטים: {items_count}\n"
    )
    if first_name:
        msg += f"מוצר ראשון: {first_name}"
    send_test_text_message(cfg.access_token, cfg.phone_number_id, to_phone, msg[:1900])
    session.add(
        Alert(
            shop_id=int(cfg.shop_id),
            message=f"התראת מכירה בלייב: הזמנה #{order_id} בסכום {_fmt_money(total, str(currency))}",
            severity="info",
            kind="sales_live",
        ),
    )
    session.commit()
    _mark_sent(session, int(cfg.shop_id), "sale_live", event_key)
    return True


def send_scheduled_sales_reports(session: Session) -> None:
    now = _as_utc(utcnow())
    day_key = now.strftime("%Y-%m-%d")
    month_key = now.strftime("%Y-%m")
    rows = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.enabled == True)).all()  # noqa: E712
    for cfg in rows:
        if not cfg.shop_id or not cfg.access_token or not cfg.phone_number_id:
            continue
        shop = session.get(Shop, cfg.shop_id)
        if not shop:
            continue
        prefs = _owner_prefs(session, shop)
        if not prefs:
            continue
        to_phone = _resolve_alert_phone(session, cfg)
        if not to_phone:
            continue
        from backend.services.woo_analytics import compute_sales_insights

        if prefs.notify_sales_daily and now.hour == 20 and not _already_sent(session, int(cfg.shop_id), "daily_report", day_key):
            data = compute_sales_insights(session, int(cfg.shop_id), days=1)
            if data.get("ok"):
                msg = (
                    "דוח יומי:\n"
                    f"מכירות היום: {int(data.get('orders_fetched') or 0)}\n"
                    f"הכנסות: {_fmt_money(float(data.get('total_revenue_tracked') or 0.0), str(data.get('currency') or ''))}"
                )
                send_test_text_message(cfg.access_token, cfg.phone_number_id, to_phone, msg[:1900])
                session.add(
                    Alert(
                        shop_id=int(cfg.shop_id),
                        message=f"דוח יומי: {int(data.get('orders_fetched') or 0)} מכירות, הכנסות {_fmt_money(float(data.get('total_revenue_tracked') or 0.0), str(data.get('currency') or ''))}",
                        severity="info",
                        kind="sales_daily",
                    ),
                )
                session.commit()
                _mark_sent(session, int(cfg.shop_id), "daily_report", day_key)

        if (
            prefs.notify_sales_monthly
            and now.day == 1
            and now.hour == 9
            and not _already_sent(session, int(cfg.shop_id), "monthly_report", month_key)
        ):
            data = compute_sales_insights(session, int(cfg.shop_id), days=31)
            if data.get("ok"):
                msg = (
                    "דוח חודשי:\n"
                    f"מכירות החודש: {int(data.get('orders_fetched') or 0)}\n"
                    f"הכנסות: {_fmt_money(float(data.get('total_revenue_tracked') or 0.0), str(data.get('currency') or ''))}"
                )
                send_test_text_message(cfg.access_token, cfg.phone_number_id, to_phone, msg[:1900])
                session.add(
                    Alert(
                        shop_id=int(cfg.shop_id),
                        message=f"דוח חודשי: {int(data.get('orders_fetched') or 0)} מכירות, הכנסות {_fmt_money(float(data.get('total_revenue_tracked') or 0.0), str(data.get('currency') or ''))}",
                        severity="info",
                        kind="sales_monthly",
                    ),
                )
                session.commit()
                _mark_sent(session, int(cfg.shop_id), "monthly_report", month_key)
