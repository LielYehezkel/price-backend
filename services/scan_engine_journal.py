from __future__ import annotations

import logging
import traceback
from sqlmodel import Session

from backend.db import engine
from backend.models import AdminOperationalLog, SchedulerHeartbeat, Shop, utcnow

log = logging.getLogger(__name__)

# המתזמן אמור לרוץ כל כמה שניות; מעבר לכך — קריטי; בין 45–90 — אזהרת ביצועים.
STALE_HEARTBEAT_AFTER_SECONDS = 90
STALE_WARNING_AFTER_SECONDS = 45

_MAX_ERR_MSG = 600
_MAX_ERR_DETAIL = 12_000
_MAX_TICK_DETAIL = 8000


def compute_scan_engine_health(hb: SchedulerHeartbeat | None) -> tuple[str, str, float | None]:
    """
    מחזיר (סטטוס: ok|warning|error|critical|unknown, הודעה בעברית, שניות מאז דופק אם רלוונטי).
    """
    if hb is None or hb.last_tick_at is None:
        return (
            "unknown",
            "עדיין לא נרשם מחזור מהמתזמן מאז הפעלת השרת (או שהטבלה ריקה).",
            None,
        )
    now = utcnow()
    last = hb.last_tick_at
    if last.tzinfo is None:
        from datetime import timezone as _tz

        last = last.replace(tzinfo=_tz.utc)
    stale = (now - last).total_seconds()
    if not hb.last_tick_ok:
        return (
            "error",
            hb.last_error_message or "מחזור אחרון נכשל — ראו פירוט בשגיאה אחרונה.",
            stale,
        )
    if stale > STALE_HEARTBEAT_AFTER_SECONDS:
        return (
            "critical",
            f"אין דופק מהמתזמן כבר {int(stale)} שניות — ייתכן שהשרת נעצר, שהתהליך תקוע, או שהסריקה נמשכת יותר מדי.",
            stale,
        )
    if stale > STALE_WARNING_AFTER_SECONDS:
        return (
            "warning",
            f"דופק מאוחר ({int(stale)} שניות) — ייתכן עומס, סריקה ארוכה או עיכוב זמני.",
            stale,
        )
    return ("ok", "מנוע הסריקות פעיל והמתזמן מדווח כרגיל.", stale)


def get_or_create_heartbeat(session: Session) -> SchedulerHeartbeat:
    row = session.get(SchedulerHeartbeat, 1)
    if not row:
        row = SchedulerHeartbeat(id=1)
        session.add(row)
        session.commit()
        session.refresh(row)
    return row


def record_tick_success(session: Session, duration_ms: int, scans: int, shops: int) -> None:
    hb = get_or_create_heartbeat(session)
    now = utcnow()
    hb.updated_at = now
    hb.last_tick_at = now
    hb.last_tick_duration_ms = max(0, int(duration_ms))
    hb.last_tick_ok = True
    hb.last_tick_scans = max(0, int(scans))
    hb.last_tick_shops_touched = max(0, int(shops))
    hb.last_error_message = None
    hb.last_error_detail = None
    hb.last_error_at = None
    hb.consecutive_failures = 0
    hb.total_ticks = (hb.total_ticks or 0) + 1
    session.add(hb)
    session.commit()


def record_tick_failure(session: Session, duration_ms: int, exc: BaseException) -> None:
    hb = get_or_create_heartbeat(session)
    now = utcnow()
    msg = str(exc).strip() or type(exc).__name__
    detail = traceback.format_exc()
    if len(detail) > _MAX_TICK_DETAIL:
        detail = detail[: _MAX_TICK_DETAIL] + "\n…(קוצר)"

    hb.updated_at = now
    hb.last_tick_at = now
    hb.last_tick_duration_ms = max(0, int(duration_ms))
    hb.last_tick_ok = False
    hb.last_error_message = msg[:_MAX_ERR_MSG]
    hb.last_error_detail = detail
    hb.last_error_at = now
    hb.consecutive_failures = (hb.consecutive_failures or 0) + 1
    hb.total_ticks = (hb.total_ticks or 0) + 1
    session.add(hb)
    session.commit()

    append_operational_log_safe(
        level="error",
        code="SCHEDULER_TICK_FAILED",
        title="מחזור מתזמן נכשל לחלוטין",
        detail=f"{msg}\n\n{detail}",
        shop_id=None,
        competitor_link_id=None,
    )


def append_operational_log(
    session: Session,
    *,
    level: str,
    code: str,
    title: str,
    detail: str,
    shop_id: int | None,
    competitor_link_id: int | None,
) -> None:
    if len(detail) > _MAX_ERR_DETAIL:
        detail = detail[:_MAX_ERR_DETAIL] + "\n…(קוצר)"
    row = AdminOperationalLog(
        level=level,
        code=code,
        title=title[:500],
        detail=detail,
        shop_id=shop_id,
        competitor_link_id=competitor_link_id,
    )
    session.add(row)
    session.commit()


def append_operational_log_safe(
    *,
    level: str,
    code: str,
    title: str,
    detail: str,
    shop_id: int | None,
    competitor_link_id: int | None,
) -> None:
    """כתיבה בסשן נפרד — אחרי rollback בסריקה המקורית לא נמחק הלוג."""
    try:
        with Session(engine) as session:
            append_operational_log(
                session,
                level=level,
                code=code,
                title=title,
                detail=detail,
                shop_id=shop_id,
                competitor_link_id=competitor_link_id,
            )
    except Exception:
        log.exception("append_operational_log_safe failed")


def classify_competitor_scan_failure(
    exc: BaseException,
    shop: Shop | None,
    competitor_id: int,
) -> tuple[str, str, str]:
    """מחזיר (code, כותרת בעברית, פירוט)."""
    name = type(exc).__name__
    msg = str(exc).strip() or name
    low = msg.lower()

    if "competitor not found" in low or "product not found" in low or "shop not found" in low:
        return (
            "DATA_INTEGRITY",
            "נתונים חסרים בבסיס הנתונים",
            f"מזהה קישור מתחרה: {competitor_id}. השגיאה: {name}: {msg}",
        )
    if "timeout" in low or name in ("Timeout", "ReadTimeout", "ConnectTimeout"):
        return (
            "FETCH_TIMEOUT",
            "תם זמן המתנה לרשת / שרת איטי",
            f"קישור #{competitor_id}: {name}: {msg}",
        )
    if "connection" in low or "resolve" in low or name in ("ConnectionError", "gaierror"):
        return (
            "NETWORK_ERROR",
            "בעיית רשת או DNS",
            f"קישור #{competitor_id}: {name}: {msg}",
        )
    if "ssl" in low or "certificate" in low:
        return (
            "TLS_ERROR",
            "בעיית אבטחה / תעודת SSL",
            f"קישור #{competitor_id}: {name}: {msg}",
        )
    if "403" in msg or "401" in msg or "forbidden" in low:
        return (
            "HTTP_FORBIDDEN",
            "האתר חוסם גישה (403/401)",
            f"קישור #{competitor_id}: {name}: {msg}",
        )
    if "404" in msg or "not found" in low:
        return (
            "HTTP_NOT_FOUND",
            "הדף לא נמצא (404)",
            f"קישור #{competitor_id}: {name}: {msg}",
        )
    if "429" in msg or "too many" in low:
        return (
            "HTTP_RATE_LIMIT",
            "האתר הגביל בקשות (rate limit)",
            f"קישור #{competitor_id}: {name}: {msg}",
        )
    if "http" in name.lower() or msg.startswith("HTTP "):
        return (
            "HTTP_ERROR",
            "שגיאת HTTP בטעינת הדף",
            f"קישור #{competitor_id}: {name}: {msg}",
        )

    shop_bit = f"חנות #{shop.id} " if shop else ""
    return (
        "COMPETITOR_SCAN_FAILED",
        "סריקת מתחרה נכשלה",
        f"{shop_bit}קישור מתחרה #{competitor_id}: {name}: {msg}\n\n{traceback.format_exc()[-4000:]}",
    )
