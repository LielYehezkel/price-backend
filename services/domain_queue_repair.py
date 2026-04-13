"""תור ביקורת דומיין: לכל קישור מתחרה בדומיין שלא אושר חייבת להיות שורה בפאנל — גם לפני סריקה מוצלחת."""

from __future__ import annotations

import json
from typing import Literal

from sqlmodel import Session, select

from backend.models import (
    CompetitorLink,
    DomainPriceApproval,
    DomainReviewQueueItem,
    Product,
    utcnow,
)
from backend.services.domain_policy import clear_domain_review_pending_for_live_domain, domain_from_url, domain_is_live
from backend.services.extract import run_extraction_pipeline
from backend.services.fetch_html import fetch_html_sync

EnsureResult = Literal["skipped_live", "already_queued", "created"]


def ensure_domain_review_queue_item_for_competitor(
    session: Session,
    shop_id: int,
    competitor_id: int,
    *,
    try_fetch: bool = True,
) -> EnsureResult:
    """
    מבטיח שיש DomainReviewQueueItem בסטטוס pending לקישור הזה כשהדומיין עדיין לא מאושר.
    אם try_fetch=True — מנסים למשוך HTML ולהעשיר מחיר/מועמדים; אם נכשל — נוצרת רשומת „ממתין לסריקה“ בלי דאטה.
    """
    comp = session.get(CompetitorLink, competitor_id)
    if not comp:
        return "skipped_live"
    product = session.get(Product, comp.product_id)
    if not product or product.shop_id != shop_id:
        return "skipped_live"

    domain = domain_from_url(comp.url)
    if not domain:
        return "skipped_live"

    if domain_is_live(session, domain):
        clear_domain_review_pending_for_live_domain(session, domain)
        return "skipped_live"

    existing = session.exec(
        select(DomainReviewQueueItem).where(
            DomainReviewQueueItem.competitor_link_id == comp.id,
            DomainReviewQueueItem.status == "pending",
        ),
    ).first()
    if existing:
        return "already_queued"

    price: float | None = None
    cur: str | None = None
    candidates: list = []
    sug: str | None = None
    cand_json = "[]"
    fetch_ok = False

    if try_fetch:
        try:
            html = fetch_html_sync(comp.url)
            result = run_extraction_pipeline(html)
            price = result.get("price")
            cur = result.get("currency")
            candidates = result.get("candidates") or []
            sug = candidates[0].get("selector") if candidates and isinstance(candidates[0], dict) else None
            cand_json = json.dumps(candidates[:40], ensure_ascii=False)
            fetch_ok = True
        except Exception:
            fetch_ok = False

    dpa = session.get(DomainPriceApproval, domain)
    if not dpa:
        dpa = DomainPriceApproval(domain=domain)
    dpa.status = "pending"
    if fetch_ok:
        dpa.sample_url = comp.url
        dpa.pending_price = price
        dpa.pending_currency = cur
        dpa.candidates_json = cand_json
        dpa.suggested_selector = sug
    else:
        if not dpa.sample_url:
            dpa.sample_url = comp.url
        if not dpa.candidates_json and not fetch_ok:
            dpa.candidates_json = "[]"
    dpa.updated_at = utcnow()
    session.add(dpa)

    session.add(
        DomainReviewQueueItem(
            domain=domain,
            competitor_link_id=comp.id,
            shop_id=shop_id,
            product_name=product.name or "",
            sample_url=comp.url,
            pending_price=price if fetch_ok else None,
            pending_currency=cur if fetch_ok else None,
            candidates_json=cand_json if fetch_ok else "[]",
            suggested_selector=sug if fetch_ok else None,
            source="enqueue" if not fetch_ok else "enqueue_fetch",
            status="pending",
        ),
    )
    return "created"


def repair_missing_domain_queue_for_shop(session: Session, shop_id: int) -> int:
    """
    לכל קישור מתחרה בחנות: אם הדומיין לא חי ואין פריט pending — יוצרים (עם ניסיון fetch).
    מחזיר כמה פריטים חדשים נוספו.
    """
    comps = session.exec(
        select(CompetitorLink)
        .join(Product, CompetitorLink.product_id == Product.id)
        .where(Product.shop_id == shop_id),
    ).all()
    added = 0
    for comp in comps:
        try:
            r = ensure_domain_review_queue_item_for_competitor(
                session,
                shop_id,
                comp.id,
                try_fetch=True,
            )
            if r == "created":
                session.commit()
                added += 1
            elif r in ("skipped_live", "already_queued"):
                pass
        except Exception:
            session.rollback()
    return added


def repair_all_missing_domain_queue_items_global(session: Session, *, try_fetch: bool = False) -> int:
    """
    תיקון מערכתי: כל קישור בדומיין לא-חי בלי שורת תור pending מקבל אחת.
    try_fetch=False (ברירת מחדל לפאנל אדמין): רק DB, בלי רשת — מהיר ובטוח.
    """
    comps = session.exec(select(CompetitorLink)).all()
    added = 0
    for comp in comps:
        product = session.get(Product, comp.product_id)
        if not product:
            continue
        try:
            r = ensure_domain_review_queue_item_for_competitor(
                session,
                product.shop_id,
                comp.id,
                try_fetch=try_fetch,
            )
            if r == "created":
                session.commit()
                added += 1
        except Exception:
            session.rollback()
    return added
