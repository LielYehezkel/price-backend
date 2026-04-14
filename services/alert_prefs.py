"""סינון התראות לפי העדפות משתמש לחנות."""

from __future__ import annotations

import json

from sqlmodel import Session, select

from backend.models import Alert, UserShopPreferences, utcnow


def get_or_create_user_shop_prefs(session: Session, user_id: int, shop_id: int) -> UserShopPreferences:
    row = session.exec(
        select(UserShopPreferences).where(
            UserShopPreferences.user_id == user_id,
            UserShopPreferences.shop_id == shop_id,
        ),
    ).first()
    if row:
        return row
    row = UserShopPreferences(user_id=user_id, shop_id=shop_id)
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def alert_allowed_by_prefs(alert: Alert, prefs: UserShopPreferences) -> bool:
    k = (getattr(alert, "kind", None) or "general").strip() or "general"
    if k == "general":
        return True
    key_map = {
        "competitor_cheaper": "notify_competitor_cheaper",
        "price_change": "notify_price_change",
        "auto_pricing": "notify_auto_pricing",
        "sanity_failed": "notify_sanity",
        "sales_live": "notify_sale_live",
        "sales_daily": "notify_sales_daily",
        "sales_monthly": "notify_sales_monthly",
    }
    attr = key_map.get(k)
    if not attr:
        return True
    return bool(getattr(prefs, attr, True))


def filter_alerts_for_user(alerts: list[Alert], prefs: UserShopPreferences) -> list[Alert]:
    return [a for a in alerts if alert_allowed_by_prefs(a, prefs)]


def load_dismissed_recommendation_ids(prefs: UserShopPreferences) -> set[str]:
    try:
        data = json.loads(prefs.dismissed_recommendation_ids_json or "[]")
    except json.JSONDecodeError:
        return set()
    if not isinstance(data, list):
        return set()
    return {str(x) for x in data if x}


def save_dismissed_recommendation_ids(session: Session, prefs: UserShopPreferences, ids: set[str]) -> None:
    prefs.dismissed_recommendation_ids_json = json.dumps(sorted(ids), ensure_ascii=False)
    prefs.updated_at = utcnow()
    session.add(prefs)
    session.commit()
