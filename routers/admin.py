import json
from datetime import timedelta
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, EmailStr
from sqlalchemy import func, text
from sqlmodel import Session, select

from backend.auth_utils import hash_password
from backend.db import get_session
from backend.deps import get_current_admin
from backend.models import (
    AdminOperationalLog,
    CompetitorLink,
    DomainPriceApproval,
    DomainPriceSelector,
    DomainReviewQueueItem,
    ScanLog,
    SchedulerHeartbeat,
    Shop,
    ShopPackageAuditLog,
    ShopScanQuotaDaily,
    User,
    utcnow,
)
from backend.services.price_sanity import get_settings
from backend.services.domain_policy import clear_domain_review_pending_for_live_domain, iter_competitor_ids_for_domain
from backend.services.domain_queue_repair import repair_all_missing_domain_queue_items_global
from backend.services.extract import run_extraction_pipeline, validate_selector_with_fallbacks
from backend.services.fetch_html import (
    FetchHtmlError,
    FetchedHtml,
    fetch_error_api_status,
    fetch_html_sync,
    fetch_html_sync_with_fallback_meta,
    format_fetch_error_hebrew,
)
from backend.services.monitor_checks import run_competitor_check
from backend.services.scan_engine_journal import compute_scan_engine_health, get_or_create_heartbeat
from backend.services.scan_engine_journal import append_operational_log_safe
from backend.services.system_config import get_or_create_system_config


def _safe_fetch_html_for_admin(url: str) -> str:
    """לא מחזיר 500 פנימי על 403 — הודעת שגיאה קריאה לפרונט."""
    try:
        return fetch_html_sync(url)
    except FetchHtmlError as e:
        raise HTTPException(fetch_error_api_status(e), format_fetch_error_hebrew(e)) from e
    except httpx.HTTPStatusError as e:
        code = e.response.status_code if e.response is not None else "?"
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            f"האתר חסם את השרת (HTTP {code}). נסו שוב, או פתחו את הקישור ממחשב — לעיתים WAF חוסם כתובות דאטה-סנטר.",
        ) from e
    except httpx.HTTPError as e:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            f"שגיאת רשת במשיכת הדף: {e!s}",
        ) from e


def _safe_fetch_html_meta_for_admin(url: str) -> FetchedHtml:
    try:
        return fetch_html_sync_with_fallback_meta(url)
    except FetchHtmlError as e:
        raise HTTPException(fetch_error_api_status(e), format_fetch_error_hebrew(e)) from e
    except httpx.HTTPStatusError as e:
        code = e.response.status_code if e.response is not None else "?"
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            f"האתר חסם את השרת (HTTP {code}). נסו שוב, או פתחו את הקישור ממחשב — לעיתים WAF חוסם כתובות דאטה-סנטר.",
        ) from e
    except httpx.HTTPError as e:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            f"שגיאת רשת במשיכת הדף: {e!s}",
        ) from e


from backend.routers.price import ResolveOut, run_price_resolve

router = APIRouter(prefix="/api/admin", tags=["admin"])


PACKAGE_POLICIES: dict[str, dict[str, int]] = {
    "free": {"max_scan_runs_per_day": 10, "max_scans_per_day_window": 1, "min_interval_minutes": 1440},
    "basic": {"max_scan_runs_per_day": 50, "max_scans_per_day_window": 2, "min_interval_minutes": 720},
    "premium": {"max_scan_runs_per_day": 250, "max_scans_per_day_window": 3, "min_interval_minutes": 480},
}


def apply_package_policy(shop: Shop, package_tier: str) -> None:
    tier = (package_tier or "").strip().lower()
    policy = PACKAGE_POLICIES.get(tier)
    if not policy:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "חבילה לא תקינה")
    shop.package_tier = tier
    shop.package_max_scan_runs_per_day = int(policy["max_scan_runs_per_day"])
    shop.package_max_scans_per_day_window = int(policy["max_scans_per_day_window"])
    shop.package_min_interval_minutes = int(policy["min_interval_minutes"])
    # Keep legacy fields aligned with package scheduler policy.
    shop.check_interval_minutes = int(policy["min_interval_minutes"])
    shop.check_interval_hours = max(1, int((shop.check_interval_minutes + 59) // 60))


class UserAdminRow(BaseModel):
    id: int
    email: str
    name: str | None
    is_admin: bool
    created_at: str
    password_note: str


class AdminShopPackageRow(BaseModel):
    shop_id: int
    shop_name: str
    owner_email: str | None
    package_tier: str
    package_max_scan_runs_per_day: int
    package_max_scans_per_day_window: int
    package_min_interval_minutes: int
    package_usage_metric: str
    today_runs_used: int


class AdminShopPackagePatch(BaseModel):
    package_tier: str
    change_note: str | None = None


class AdminShopPackageAuditRow(BaseModel):
    id: int
    shop_id: int
    changed_by_user_id: int
    previous_tier: str
    new_tier: str
    previous_max_scan_runs_per_day: int
    new_max_scan_runs_per_day: int
    previous_max_scans_per_day_window: int
    new_max_scans_per_day_window: int
    previous_min_interval_minutes: int
    new_min_interval_minutes: int
    change_note: str | None
    created_at: str


class UserAdminPatch(BaseModel):
    email: EmailStr | None = None
    name: str | None = None
    is_admin: bool | None = None


class UserPasswordSet(BaseModel):
    new_password: str


@router.get("/users", response_model=list[UserAdminRow])
def list_users(
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    users = session.exec(select(User).order_by(User.id)).all()
    return [
        UserAdminRow(
            id=u.id,
            email=u.email,
            name=u.name,
            is_admin=u.is_admin,
            created_at=u.created_at.isoformat(),
            password_note="מוצפן (bcrypt) — לא ניתן לשחזר את הסיסמה המקורית",
        )
        for u in users
    ]


@router.patch("/users/{user_id}", response_model=UserAdminRow)
def patch_user(
    user_id: int,
    body: UserAdminPatch,
    session: Annotated[Session, Depends(get_session)],
    admin: Annotated[User, Depends(get_current_admin)],
):
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "משתמש לא נמצא")
    if body.email is not None:
        other = session.exec(select(User).where(User.email == body.email)).first()
        if other and other.id != user_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "אימייל תפוס")
        u.email = str(body.email)
    if body.name is not None:
        u.name = body.name
    if body.is_admin is not None:
        if u.id == admin.id and not body.is_admin:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "לא ניתן להסיר מעצמך הרשאת מנהל")
        u.is_admin = body.is_admin
    session.add(u)
    session.commit()
    session.refresh(u)
    return UserAdminRow(
        id=u.id,
        email=u.email,
        name=u.name,
        is_admin=u.is_admin,
        created_at=u.created_at.isoformat(),
        password_note="מוצפן (bcrypt) — לא ניתן לשחזר את הסיסמה המקורית",
    )


@router.post("/users/{user_id}/password")
def set_user_password(
    user_id: int,
    body: UserPasswordSet,
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    if len(body.new_password) < 6:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "סיסמה קצרה מדי")
    u = session.get(User, user_id)
    if not u:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "משתמש לא נמצא")
    u.hashed_password = hash_password(body.new_password)
    session.add(u)
    session.commit()
    return {"ok": True}


@router.get("/shops/packages", response_model=list[AdminShopPackageRow])
def list_shop_packages(
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    today = utcnow().strftime("%Y-%m-%d")
    shops = session.exec(select(Shop).order_by(Shop.id)).all()
    out: list[AdminShopPackageRow] = []
    for s in shops:
        owner = session.get(User, s.owner_id)
        q = session.exec(
            select(ShopScanQuotaDaily).where(
                ShopScanQuotaDaily.shop_id == int(s.id),
                ShopScanQuotaDaily.bucket_date == today,
            ),
        ).first()
        out.append(
            AdminShopPackageRow(
                shop_id=int(s.id or 0),
                shop_name=s.name,
                owner_email=owner.email if owner else None,
                package_tier=(getattr(s, "package_tier", None) or "free"),
                package_max_scan_runs_per_day=int(getattr(s, "package_max_scan_runs_per_day", 10) or 10),
                package_max_scans_per_day_window=int(getattr(s, "package_max_scans_per_day_window", 1) or 1),
                package_min_interval_minutes=int(getattr(s, "package_min_interval_minutes", 1440) or 1440),
                package_usage_metric="shop_scan_cycle_runs_per_day",
                today_runs_used=int(getattr(q, "runs_count", 0) or 0),
            ),
        )
    return out


@router.patch("/shops/{shop_id}/package", response_model=AdminShopPackageRow)
def patch_shop_package(
    shop_id: int,
    body: AdminShopPackagePatch,
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    s = session.get(Shop, shop_id)
    if not s:
        append_operational_log_safe(
            level="warning",
            code="PACKAGE_PATCH_SHOP_NOT_FOUND",
            title="ניסיון שינוי חבילה לחנות לא קיימת",
            detail=f"shop_id={shop_id}",
            shop_id=None,
            competitor_link_id=None,
        )
        raise HTTPException(status.HTTP_404_NOT_FOUND, "חנות לא נמצאה")
    prev = {
        "tier": getattr(s, "package_tier", "free") or "free",
        "max_runs": int(getattr(s, "package_max_scan_runs_per_day", 10) or 10),
        "windows": int(getattr(s, "package_max_scans_per_day_window", 1) or 1),
        "interval": int(getattr(s, "package_min_interval_minutes", 1440) or 1440),
    }
    try:
        apply_package_policy(s, body.package_tier)
    except HTTPException:
        append_operational_log_safe(
            level="warning",
            code="PACKAGE_PATCH_INVALID_TIER",
            title="ניסיון שינוי חבילה עם tier לא תקין",
            detail=f"shop_id={shop_id} requested_tier={(body.package_tier or '').strip().lower()}",
            shop_id=shop_id,
            competitor_link_id=None,
        )
        raise
    s.last_scan_cycle_at = None
    session.add(s)
    changed = (
        s.package_tier != prev["tier"]
        or int(s.package_max_scan_runs_per_day or 0) != prev["max_runs"]
        or int(s.package_max_scans_per_day_window or 0) != prev["windows"]
        or int(s.package_min_interval_minutes or 0) != prev["interval"]
    )
    if changed:
        session.add(
            ShopPackageAuditLog(
                shop_id=int(s.id),
                changed_by_user_id=int(_admin.id),
                previous_tier=prev["tier"],
                new_tier=s.package_tier,
                previous_max_scan_runs_per_day=prev["max_runs"],
                new_max_scan_runs_per_day=int(s.package_max_scan_runs_per_day or 10),
                previous_max_scans_per_day_window=prev["windows"],
                new_max_scans_per_day_window=int(s.package_max_scans_per_day_window or 1),
                previous_min_interval_minutes=prev["interval"],
                new_min_interval_minutes=int(s.package_min_interval_minutes or 1440),
                change_note=(body.change_note or "").strip() or None,
            ),
        )
    session.commit()
    session.refresh(s)
    owner = session.get(User, s.owner_id)
    today = utcnow().strftime("%Y-%m-%d")
    q = session.exec(
        select(ShopScanQuotaDaily).where(
            ShopScanQuotaDaily.shop_id == int(s.id),
            ShopScanQuotaDaily.bucket_date == today,
        ),
    ).first()
    return AdminShopPackageRow(
        shop_id=int(s.id or 0),
        shop_name=s.name,
        owner_email=owner.email if owner else None,
        package_tier=s.package_tier,
        package_max_scan_runs_per_day=int(s.package_max_scan_runs_per_day or 10),
        package_max_scans_per_day_window=int(s.package_max_scans_per_day_window or 1),
        package_min_interval_minutes=int(s.package_min_interval_minutes or 1440),
        package_usage_metric="shop_scan_cycle_runs_per_day",
        today_runs_used=int(getattr(q, "runs_count", 0) or 0),
    )


@router.get("/shops/{shop_id}/package-audit", response_model=list[AdminShopPackageAuditRow])
def list_shop_package_audit(
    shop_id: int,
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
    limit: int = Query(100, ge=1, le=500),
):
    rows = session.exec(
        select(ShopPackageAuditLog)
        .where(ShopPackageAuditLog.shop_id == shop_id)
        .order_by(ShopPackageAuditLog.id.desc())
        .limit(limit),
    ).all()
    return [
        AdminShopPackageAuditRow(
            id=int(r.id or 0),
            shop_id=int(r.shop_id),
            changed_by_user_id=int(r.changed_by_user_id),
            previous_tier=r.previous_tier,
            new_tier=r.new_tier,
            previous_max_scan_runs_per_day=int(r.previous_max_scan_runs_per_day),
            new_max_scan_runs_per_day=int(r.new_max_scan_runs_per_day),
            previous_max_scans_per_day_window=int(r.previous_max_scans_per_day_window),
            new_max_scans_per_day_window=int(r.new_max_scans_per_day_window),
            previous_min_interval_minutes=int(r.previous_min_interval_minutes),
            new_min_interval_minutes=int(r.new_min_interval_minutes),
            change_note=r.change_note,
            created_at=r.created_at.isoformat(),
        )
        for r in rows
    ]


def _parse_candidates_json(raw_json: str | None) -> list[dict[str, Any]]:
    if not raw_json:
        return []
    try:
        raw = json.loads(raw_json)
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)][:40]
    except json.JSONDecodeError:
        pass
    return []


class DomainReviewRow(BaseModel):
    queue_item_id: int | None = None
    domain: str
    shop_id: int | None = None
    product_name: str | None = None
    source: str
    reporter_note: str | None = None
    status: str
    sample_url: str
    pending_price: float | None
    pending_currency: str | None
    suggested_selector: str | None
    candidates: list[dict[str, Any]]
    updated_at: str


class DomainApproveBody(BaseModel):
    domain: str
    css_selector: str
    selector_alternates: list[str] | None = None
    queue_item_id: int | None = None


class RescanCandidatesBody(BaseModel):
    queue_item_id: int | None = None
    domain: str | None = None


def _apply_extraction_to_pending_queue_item(session: Session, qi: DomainReviewQueueItem) -> int:
    """מחזיר מספר מועמדי מחיר לאחר עדכון."""
    comp = session.get(CompetitorLink, qi.competitor_link_id)
    if not comp:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "קישור מתחרה לא נמצא")
    url = (qi.sample_url or comp.url or "").strip()
    if not url:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "אין כתובת לסריקה")
    domain = qi.domain.strip().lower()
    html = _safe_fetch_html_for_admin(url)
    result = run_extraction_pipeline(html)
    price = result.get("price")
    cur = result.get("currency")
    candidates = result.get("candidates") or []
    cand_json = json.dumps(candidates[:40], ensure_ascii=False)
    sug = candidates[0].get("selector") if candidates and isinstance(candidates[0], dict) else None

    qi.pending_price = price
    qi.pending_currency = cur
    qi.candidates_json = cand_json
    qi.suggested_selector = sug
    qi.sample_url = url
    qi.source = "rescan_admin"
    session.add(qi)

    dpa = session.get(DomainPriceApproval, domain)
    if not dpa:
        dpa = DomainPriceApproval(domain=domain)
    dpa.status = "pending"
    dpa.sample_url = url
    dpa.pending_price = price
    dpa.pending_currency = cur
    dpa.candidates_json = cand_json
    dpa.suggested_selector = sug
    dpa.updated_at = utcnow()
    session.add(dpa)
    return len([x for x in candidates if isinstance(x, dict)])


def _resolve_pending_queue_for_domain(session: Session, domain: str) -> None:
    domain = domain.strip().lower()
    now = utcnow()
    rows = session.exec(
        select(DomainReviewQueueItem).where(
            DomainReviewQueueItem.domain == domain,
            DomainReviewQueueItem.status == "pending",
        ),
    ).all()
    for r in rows:
        r.status = "resolved"
        r.resolved_at = now
        session.add(r)


@router.get("/domain-price-reviews", response_model=list[DomainReviewRow])
def list_domain_price_reviews(
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
    status_filter: str = "pending",
    limit: int = Query(500, ge=1, le=500),
):
    # כל קישור ב„בעיבוד“ חייב שורה בתור — תיקון דאטה ישן בלי קריאות רשת
    repair_all_missing_domain_queue_items_global(session, try_fetch=False)
    cleanup_changed = 0

    out: list[DomainReviewRow] = []
    if status_filter == "all":
        q_items = session.exec(
            select(DomainReviewQueueItem).order_by(DomainReviewQueueItem.created_at.desc()).limit(limit),
        ).all()
    else:
        q_items = session.exec(
            select(DomainReviewQueueItem)
            .where(DomainReviewQueueItem.status == "pending")
            .order_by(DomainReviewQueueItem.created_at.desc())
            .limit(limit),
        ).all()

    for it in q_items:
        cleanup_changed += clear_domain_review_pending_for_live_domain(session, it.domain)
        if session.get(DomainPriceSelector, it.domain):
            continue
        cand = _parse_candidates_json(it.candidates_json)
        out.append(
            DomainReviewRow(
                queue_item_id=it.id,
                domain=it.domain,
                shop_id=it.shop_id,
                product_name=it.product_name or None,
                source=it.source,
                reporter_note=it.reporter_note,
                status=it.status,
                sample_url=it.sample_url,
                pending_price=it.pending_price,
                pending_currency=it.pending_currency,
                suggested_selector=it.suggested_selector,
                candidates=cand,
                updated_at=it.created_at.isoformat(),
            ),
        )

    if status_filter != "all" and len(out) < limit:
        seen_domains = {row.domain for row in out}
        dpas = session.exec(select(DomainPriceApproval).where(DomainPriceApproval.status == "pending")).all()
        for dpa in dpas:
            cleanup_changed += clear_domain_review_pending_for_live_domain(session, dpa.domain)
            if session.get(DomainPriceSelector, dpa.domain):
                continue
            if dpa.domain in seen_domains:
                continue
            cand = _parse_candidates_json(dpa.candidates_json)
            out.append(
                DomainReviewRow(
                    queue_item_id=None,
                    domain=dpa.domain,
                    shop_id=None,
                    product_name=None,
                    source="legacy",
                    reporter_note=None,
                    status=dpa.status,
                    sample_url=dpa.sample_url,
                    pending_price=dpa.pending_price,
                    pending_currency=dpa.pending_currency,
                    suggested_selector=dpa.suggested_selector,
                    candidates=cand,
                    updated_at=dpa.updated_at.isoformat(),
                ),
            )
            seen_domains.add(dpa.domain)
            if len(out) >= limit:
                break

    if cleanup_changed:
        session.commit()

    return out


@router.post("/domain-price-reviews/rescan-candidates")
def rescan_domain_price_candidates(
    body: RescanCandidatesBody,
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    """שולף מחדש את דף המתחרה ומריץ זיהוי מועמדי מחיר (ללא אישור דומיין)."""
    if body.queue_item_id is not None:
        qi = session.get(DomainReviewQueueItem, body.queue_item_id)
        if not qi or qi.status != "pending":
            raise HTTPException(status.HTTP_404_NOT_FOUND, "פריט תור לא נמצא או לא ממתין")
        n = _apply_extraction_to_pending_queue_item(session, qi)
        session.commit()
        session.refresh(qi)
        return {
            "ok": True,
            "queue_item_id": qi.id,
            "domain": qi.domain,
            "candidates_count": n,
            "pending_price": qi.pending_price,
            "rows_updated": 1,
        }

    dom = (body.domain or "").strip().lower()
    if not dom:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "שלחו queue_item_id או domain",
        )

    items = session.exec(
        select(DomainReviewQueueItem).where(
            DomainReviewQueueItem.domain == dom,
            DomainReviewQueueItem.status == "pending",
        ),
    ).all()
    if items:
        last_n = 0
        for qi in items:
            last_n = _apply_extraction_to_pending_queue_item(session, qi)
        session.commit()
        return {
            "ok": True,
            "domain": dom,
            "candidates_count": last_n,
            "pending_price": None,
            "rows_updated": len(items),
        }

    dpa = session.get(DomainPriceApproval, dom)
    if not dpa or dpa.status != "pending":
        raise HTTPException(status.HTTP_404_NOT_FOUND, "לא נמצא דומיין ממתין לאישור")
    url = (dpa.sample_url or "").strip()
    if not url:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "אין דף דוגמה לדומיין — הוסיפו קישור מתחרה")
    html = _safe_fetch_html_for_admin(url)
    result = run_extraction_pipeline(html)
    price = result.get("price")
    cur = result.get("currency")
    candidates = result.get("candidates") or []
    cand_json = json.dumps(candidates[:40], ensure_ascii=False)
    sug = candidates[0].get("selector") if candidates and isinstance(candidates[0], dict) else None
    dpa.pending_price = price
    dpa.pending_currency = cur
    dpa.candidates_json = cand_json
    dpa.suggested_selector = sug
    dpa.updated_at = utcnow()
    session.add(dpa)
    session.commit()
    n = len([x for x in candidates if isinstance(x, dict)])
    return {
        "ok": True,
        "domain": dom,
        "candidates_count": n,
        "pending_price": price,
        "rows_updated": 1,
    }


@router.post("/domain-price-reviews/approve")
def approve_domain_price_review(
    body: DomainApproveBody,
    session: Annotated[Session, Depends(get_session)],
    admin: Annotated[User, Depends(get_current_admin)],
):
    if body.queue_item_id is not None:
        qi = session.get(DomainReviewQueueItem, body.queue_item_id)
        if not qi or qi.status != "pending":
            raise HTTPException(status.HTTP_404_NOT_FOUND, "פריט תור לא נמצא או כבר טופל")
        domain = qi.domain.strip().lower()
        sample = (qi.sample_url or "").strip()
        dpa = session.get(DomainPriceApproval, domain)
        if not dpa:
            dpa = DomainPriceApproval(domain=domain)
            session.add(dpa)
    else:
        domain = (body.domain or "").strip().lower()
        if not domain:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "דומיין חסר")
        dpa = session.get(DomainPriceApproval, domain)
        if not dpa:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "אין רשומת ביקורת לדומיין")
        sample = (dpa.sample_url or "").strip()

    if dpa.status == "approved":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "הדומיין כבר אושר")

    if not sample:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "אין כתובת דוגמה — הוסף קישור מתחרה והרץ סריקה")
    fetched = _safe_fetch_html_meta_for_admin(sample)
    html = fetched.html
    strat = fetched.strategy
    alts = body.selector_alternates or []
    price, used = validate_selector_with_fallbacks(html, body.css_selector.strip(), alts)
    if price is None or not used:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "הסלקטור לא מחזיר מחיר תקין בדף הדוגמה")

    alts_json = json.dumps(alts, ensure_ascii=False) if alts else None
    row = session.get(DomainPriceSelector, domain)
    if row:
        row.css_selector = used
        row.alternates_json = alts_json
        row.fetch_strategy = strat
        row.updated_at = utcnow()
        session.add(row)
    else:
        session.add(
            DomainPriceSelector(
                domain=domain,
                css_selector=used,
                alternates_json=alts_json,
                fetch_strategy=strat,
                updated_at=utcnow(),
            ),
        )

    dpa.status = "approved"
    dpa.approved_at = utcnow()
    dpa.approved_by_user_id = admin.id
    session.add(dpa)
    _resolve_pending_queue_for_domain(session, domain)
    session.commit()

    ids = iter_competitor_ids_for_domain(session, domain)
    re_scan_errors: list[str] = []
    for cid in ids:
        try:
            run_competitor_check(session, cid)
        except Exception as ex:
            re_scan_errors.append(f"{cid}: {ex!s}")

    return {
        "ok": True,
        "domain": domain,
        "validated_price": price,
        "re_scanned": len(ids),
        "re_scan_errors": re_scan_errors,
    }


class PriceSanityOut(BaseModel):
    enabled: bool
    abs_min: float
    abs_max: float
    vs_prev_max_multiplier: float
    vs_ours_max_multiplier: float
    updated_at: str


class PriceSanityPatch(BaseModel):
    enabled: bool | None = None
    abs_min: float | None = None
    abs_max: float | None = None
    vs_prev_max_multiplier: float | None = None
    vs_ours_max_multiplier: float | None = None


@router.get("/price-sanity", response_model=PriceSanityOut)
def get_price_sanity_settings(
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    s = get_settings(session)
    return PriceSanityOut(
        enabled=s.enabled,
        abs_min=s.abs_min,
        abs_max=s.abs_max,
        vs_prev_max_multiplier=s.vs_prev_max_multiplier,
        vs_ours_max_multiplier=s.vs_ours_max_multiplier,
        updated_at=s.updated_at.isoformat(),
    )


@router.patch("/price-sanity", response_model=PriceSanityOut)
def patch_price_sanity_settings(
    body: PriceSanityPatch,
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    s = get_settings(session)
    if body.enabled is not None:
        s.enabled = body.enabled
    if body.abs_min is not None:
        s.abs_min = body.abs_min
    if body.abs_max is not None:
        s.abs_max = body.abs_max
    if body.vs_prev_max_multiplier is not None:
        s.vs_prev_max_multiplier = body.vs_prev_max_multiplier
    if body.vs_ours_max_multiplier is not None:
        s.vs_ours_max_multiplier = body.vs_ours_max_multiplier

    if s.abs_min >= s.abs_max:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "abs_min חייב להיות קטן מ-abs_max")
    if s.vs_prev_max_multiplier < 1.01 or s.vs_ours_max_multiplier < 1.01:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "מכפילים חייבים להיות לפחות 1.01")

    s.updated_at = utcnow()
    session.add(s)
    session.commit()
    session.refresh(s)
    return PriceSanityOut(
        enabled=s.enabled,
        abs_min=s.abs_min,
        abs_max=s.abs_max,
        vs_prev_max_multiplier=s.vs_prev_max_multiplier,
        vs_ours_max_multiplier=s.vs_ours_max_multiplier,
        updated_at=s.updated_at.isoformat(),
    )


class AdminPriceResolveIn(BaseModel):
    url: str
    ignore_saved_selector: bool = False


@router.post("/price-resolve", response_model=ResolveOut)
async def admin_price_resolve(
    body: AdminPriceResolveIn,
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    """כמו /api/price/resolve — עם אפשרות להתעלם מסלקטור שמור ולגלות מועמדים מחדש (פאנל אדמין)."""
    return await run_price_resolve(session, body.url, ignore_saved_selector=body.ignore_saved_selector)


# --- מנוע סריקות (לב המערכת) + יומן תפעול ---


class HeartbeatOut(BaseModel):
    last_tick_at: str | None
    last_tick_duration_ms: int
    last_tick_ok: bool
    last_tick_scans: int
    last_tick_shops_touched: int
    last_error_message: str | None
    last_error_detail: str | None
    last_error_at: str | None
    consecutive_failures: int
    total_ticks: int


class ScanEngineHealthOut(BaseModel):
    status: str
    message_he: str
    stale_seconds: float | None


class HourlyBucketOut(BaseModel):
    bucket: str
    count: int


class ScanEngineSummaryOut(BaseModel):
    health: ScanEngineHealthOut
    heartbeat: HeartbeatOut
    scheduler_interval_seconds: int
    scanlog_total: int
    scanlog_last_24h: int
    hourly_scans: list[HourlyBucketOut]
    ops_errors_24h: int
    ops_warnings_24h: int
    users_count: int
    shops_count: int
    competitor_links_count: int
    pending_domain_reviews: int


@router.get("/scan-engine/summary", response_model=ScanEngineSummaryOut)
def scan_engine_summary(
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    """סקירה אחת לדשבורד: דופק מתזמן, בריאות, סטטיסטיקות סריקה ועומס תפעולי."""
    hb = get_or_create_heartbeat(session)
    st, msg, stale = compute_scan_engine_health(hb)

    since24 = utcnow() - timedelta(hours=24)
    scanlog_total = session.exec(select(func.count(ScanLog.id))).first() or 0
    scanlog_last_24h = (
        session.exec(select(func.count(ScanLog.id)).where(ScanLog.created_at >= since24)).first() or 0
    )

    raw_rows = session.execute(
        text("""
            SELECT to_char(date_trunc('hour', created_at), 'YYYY-MM-DD HH24:00') AS bucket, COUNT(*) AS n
            FROM scanlog
            WHERE created_at >= :since
            GROUP BY bucket
            ORDER BY bucket
        """),
        {"since": since24},
    ).mappings().all()
    counts = {str(r["bucket"]): int(r["n"]) for r in raw_rows if r["bucket"] is not None}

    now = utcnow()
    hourly: list[HourlyBucketOut] = []
    for i in range(23, -1, -1):
        t = now - timedelta(hours=i)
        t = t.replace(minute=0, second=0, microsecond=0)
        key = t.strftime("%Y-%m-%d %H:00")
        hourly.append(HourlyBucketOut(bucket=key, count=counts.get(key, 0)))

    ops_errors_24h = (
        session.exec(
            select(func.count(AdminOperationalLog.id)).where(
                AdminOperationalLog.created_at >= since24,
                AdminOperationalLog.level == "error",
            ),
        ).first()
        or 0
    )
    ops_warnings_24h = (
        session.exec(
            select(func.count(AdminOperationalLog.id)).where(
                AdminOperationalLog.created_at >= since24,
                AdminOperationalLog.level == "warning",
            ),
        ).first()
        or 0
    )

    users_count = session.exec(select(func.count(User.id))).first() or 0
    shops_count = session.exec(select(func.count(Shop.id))).first() or 0
    competitor_links_count = session.exec(select(func.count(CompetitorLink.id))).first() or 0
    pending_domain_reviews = (
        session.exec(
            select(func.count(DomainReviewQueueItem.id)).where(DomainReviewQueueItem.status == "pending"),
        ).first()
        or 0
    )

    return ScanEngineSummaryOut(
        health=ScanEngineHealthOut(status=st, message_he=msg, stale_seconds=stale),
        heartbeat=HeartbeatOut(
            last_tick_at=hb.last_tick_at.isoformat() if hb.last_tick_at else None,
            last_tick_duration_ms=hb.last_tick_duration_ms or 0,
            last_tick_ok=bool(hb.last_tick_ok),
            last_tick_scans=hb.last_tick_scans or 0,
            last_tick_shops_touched=hb.last_tick_shops_touched or 0,
            last_error_message=hb.last_error_message,
            last_error_detail=hb.last_error_detail,
            last_error_at=hb.last_error_at.isoformat() if hb.last_error_at else None,
            consecutive_failures=hb.consecutive_failures or 0,
            total_ticks=hb.total_ticks or 0,
        ),
        scheduler_interval_seconds=5,
        scanlog_total=int(scanlog_total),
        scanlog_last_24h=int(scanlog_last_24h),
        hourly_scans=hourly,
        ops_errors_24h=int(ops_errors_24h),
        ops_warnings_24h=int(ops_warnings_24h),
        users_count=int(users_count),
        shops_count=int(shops_count),
        competitor_links_count=int(competitor_links_count),
        pending_domain_reviews=int(pending_domain_reviews),
    )


class OperationalLogRowOut(BaseModel):
    id: int
    created_at: str
    level: str
    code: str
    title: str
    detail: str
    shop_id: int | None
    competitor_link_id: int | None


class OperationalLogPageOut(BaseModel):
    items: list[OperationalLogRowOut]
    total: int
    limit: int
    offset: int


@router.get("/operations-log", response_model=OperationalLogPageOut)
def list_operations_log(
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    level: str | None = Query(None, description="info | warning | error"),
    code_prefix: str | None = Query(None),
):
    base = select(AdminOperationalLog)
    count_stmt = select(func.count(AdminOperationalLog.id))
    if level:
        base = base.where(AdminOperationalLog.level == level)
        count_stmt = count_stmt.where(AdminOperationalLog.level == level)
    if code_prefix:
        base = base.where(AdminOperationalLog.code.startswith(code_prefix))
        count_stmt = count_stmt.where(AdminOperationalLog.code.startswith(code_prefix))

    total = session.exec(count_stmt).first() or 0
    rows = session.exec(base.order_by(AdminOperationalLog.id.desc()).offset(offset).limit(limit)).all()

    return OperationalLogPageOut(
        items=[
            OperationalLogRowOut(
                id=r.id or 0,
                created_at=r.created_at.isoformat(),
                level=r.level,
                code=r.code,
                title=r.title,
                detail=r.detail,
                shop_id=r.shop_id,
                competitor_link_id=r.competitor_link_id,
            )
            for r in rows
        ],
        total=int(total),
        limit=limit,
        offset=offset,
    )


class AdminOverviewOut(BaseModel):
    health: ScanEngineHealthOut
    heartbeat: HeartbeatOut
    scanlog_total: int
    scanlog_last_24h: int
    users_count: int
    shops_count: int
    competitor_links_count: int
    pending_domain_reviews: int
    ops_errors_24h: int
    ops_warnings_24h: int


@router.get("/dashboard/overview", response_model=AdminOverviewOut)
def admin_dashboard_overview(
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    """סקירה קצרה לדף הבית של פאנל הניהול."""
    hb = get_or_create_heartbeat(session)
    st, msg, stale = compute_scan_engine_health(hb)
    since24 = utcnow() - timedelta(hours=24)
    scanlog_total = session.exec(select(func.count(ScanLog.id))).first() or 0
    scanlog_last_24h = (
        session.exec(select(func.count(ScanLog.id)).where(ScanLog.created_at >= since24)).first() or 0
    )
    ops_errors_24h = (
        session.exec(
            select(func.count(AdminOperationalLog.id)).where(
                AdminOperationalLog.created_at >= since24,
                AdminOperationalLog.level == "error",
            ),
        ).first()
        or 0
    )
    ops_warnings_24h = (
        session.exec(
            select(func.count(AdminOperationalLog.id)).where(
                AdminOperationalLog.created_at >= since24,
                AdminOperationalLog.level == "warning",
            ),
        ).first()
        or 0
    )
    users_count = session.exec(select(func.count(User.id))).first() or 0
    shops_count = session.exec(select(func.count(Shop.id))).first() or 0
    competitor_links_count = session.exec(select(func.count(CompetitorLink.id))).first() or 0
    pending_domain_reviews = (
        session.exec(
            select(func.count(DomainReviewQueueItem.id)).where(DomainReviewQueueItem.status == "pending"),
        ).first()
        or 0
    )

    return AdminOverviewOut(
        health=ScanEngineHealthOut(status=st, message_he=msg, stale_seconds=stale),
        heartbeat=HeartbeatOut(
            last_tick_at=hb.last_tick_at.isoformat() if hb.last_tick_at else None,
            last_tick_duration_ms=hb.last_tick_duration_ms or 0,
            last_tick_ok=bool(hb.last_tick_ok),
            last_tick_scans=hb.last_tick_scans or 0,
            last_tick_shops_touched=hb.last_tick_shops_touched or 0,
            last_error_message=hb.last_error_message,
            last_error_detail=hb.last_error_detail,
            last_error_at=hb.last_error_at.isoformat() if hb.last_error_at else None,
            consecutive_failures=hb.consecutive_failures or 0,
            total_ticks=hb.total_ticks or 0,
        ),
        scanlog_total=int(scanlog_total),
        scanlog_last_24h=int(scanlog_last_24h),
        users_count=int(users_count),
        shops_count=int(shops_count),
        competitor_links_count=int(competitor_links_count),
        pending_domain_reviews=int(pending_domain_reviews),
        ops_errors_24h=int(ops_errors_24h),
        ops_warnings_24h=int(ops_warnings_24h),
    )


class AdminSystemConfigOut(BaseModel):
    backend_mode: str
    backend_api_base: str | None
    updated_at: str


class AdminSystemConfigPatch(BaseModel):
    backend_mode: str | None = None  # local | custom
    backend_api_base: str | None = None


@router.get("/system/config", response_model=AdminSystemConfigOut)
def get_system_config(
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    row = get_or_create_system_config(session)
    return AdminSystemConfigOut(
        backend_mode=row.backend_mode,
        backend_api_base=row.backend_api_base,
        updated_at=row.updated_at.isoformat(),
    )


@router.patch("/system/config", response_model=AdminSystemConfigOut)
def patch_system_config(
    body: AdminSystemConfigPatch,
    session: Annotated[Session, Depends(get_session)],
    _admin: Annotated[User, Depends(get_current_admin)],
):
    row = get_or_create_system_config(session)
    if body.backend_mode is not None:
        mode = body.backend_mode.strip().lower()
        if mode not in {"local", "custom"}:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "backend_mode חייב להיות local או custom")
        row.backend_mode = mode
    if body.backend_api_base is not None:
        val = body.backend_api_base.strip()
        if val and not (val.startswith("http://") or val.startswith("https://")):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "backend_api_base חייב להתחיל ב-http:// או https://")
        row.backend_api_base = val or None
    row.updated_at = utcnow()
    session.add(row)
    session.commit()
    session.refresh(row)
    return AdminSystemConfigOut(
        backend_mode=row.backend_mode,
        backend_api_base=row.backend_api_base,
        updated_at=row.updated_at.isoformat(),
    )
