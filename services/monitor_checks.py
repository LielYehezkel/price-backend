from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx
from sqlmodel import Session, select

from backend.models import (
    Alert,
    CompetitorLink,
    DomainPriceApproval,
    DomainReviewQueueItem,
    DomainPriceSelector,
    PriceSnapshot,
    Product,
    ScanLog,
    Shop,
    ShopScanQuotaDaily,
    utcnow,
)
from backend.services.auto_pricing import maybe_apply_auto_pricing
from backend.services.competitor_lightweight_precheck import try_competitor_lightweight_precheck_skip
from backend.services.domain_policy import domain_from_url, domain_is_live
from backend.services.scan_engine_journal import append_operational_log_safe, classify_competitor_scan_failure
from backend.services.extract import apply_saved_selector, run_extraction_pipeline
from backend.services.fetch_html import (
    FetchHtmlError,
    fetch_html_for_saved_strategy_sync,
    fetch_html_sync,
    normalize_fetch_strategy,
)
from backend.services.price_sanity import validate_competitor_price
from backend.services.woo_sync import effective_wc_price, fetch_wc_product_by_id

logger = logging.getLogger(__name__)


@dataclass
class CompetitorCheckResult:
    """published=False when domain awaits admin approval — do not expose price to shop users."""

    price: float | None
    currency: str | None
    published: bool


def _aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _finalize_after_fetch_error(
    session: Session,
    shop: Shop,
    product: Product,
    comp: CompetitorLink,
    domain: str,
    prev_comp: float | None,
    *,
    published: bool,
    reason: str,
) -> CompetitorCheckResult:
    """לא ניתן למשוך HTML — לא מפילים את הסריקה; מסיימים בצורה מסודרת."""
    comp.last_checked_at = utcnow()
    session.add(comp)
    session.add(
        ScanLog(
            shop_id=shop.id,
            product_id=product.id,
            competitor_link_id=comp.id,
            competitor_domain=domain,
            product_name=product.name or "",
            our_price=product.regular_price,
            competitor_price=None,
            previous_competitor_price=prev_comp,
            price_changed=False,
            comparison="unknown",
        ),
    )
    logger.warning("competitor_fetch_failed link=%s domain=%s %s", comp.id, domain, reason)
    session.commit()
    session.refresh(comp)
    return CompetitorCheckResult(price=None, currency=None, published=published)


def compare_prices(our: float | None, comp: float | None) -> str:
    if our is None or comp is None:
        return "unknown"
    eps = 0.02
    if abs(our - comp) <= eps:
        return "tie"
    return "you_cheaper" if our < comp else "you_expensive"


def _is_our_price_stale(product: Product, *, max_age_seconds: int) -> bool:
    last = getattr(product, "last_price_sync_at", None)
    if last is None:
        return True
    dt = _aware(last)
    if dt is None:
        return True
    return (utcnow() - dt).total_seconds() >= max_age_seconds


def _refresh_our_price_if_stale(session: Session, shop: Shop, product: Product) -> None:
    """
    רענון נקודתי למחיר המוצר מהחנות לפני השוואה מול מתחרה.
    עדכון מחיר נשמר רק אם השתנה בפועל.
    """
    if not product.woo_product_id:
        return
    if not shop.woo_site_url or not shop.woo_consumer_key or not shop.woo_consumer_secret:
        return

    # מוצרים בסריקה צריכים דיוק גבוה, אך בלי להציף את השרת.
    ttl_seconds = 120
    if not _is_our_price_stale(product, max_age_seconds=ttl_seconds):
        return

    try:
        row = fetch_wc_product_by_id(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(product.woo_product_id),
        )
    except Exception:
        # כשל רשת נקודתי לא אמור לעצור את הסריקה; נמשיך עם המחיר הקיים.
        return

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


def run_competitor_check(session: Session, competitor_id: int) -> CompetitorCheckResult:
    comp = session.get(CompetitorLink, competitor_id)
    if not comp:
        raise ValueError("competitor not found")
    product = session.get(Product, comp.product_id)
    if not product:
        raise ValueError("product not found")
    shop = session.get(Shop, product.shop_id)
    if not shop:
        raise ValueError("shop not found")
    _refresh_our_price_if_stale(session, shop, product)

    prev_comp = comp.last_price
    domain = domain_from_url(comp.url)
    live = domain_is_live(session, domain)
    saved = session.get(DomainPriceSelector, domain) if live else None

    lw_hash_for_persist: str | None = None
    if live and saved:
        try:
            early_skip, lw_ht = try_competitor_lightweight_precheck_skip(
                session, shop, product, comp, domain, saved, prev_comp,
            )
            if early_skip is not None:
                return CompetitorCheckResult(
                    price=early_skip.price,
                    currency=early_skip.currency,
                    published=early_skip.published,
                )
            lw_hash_for_persist = lw_ht
        except Exception:
            lw_hash_for_persist = None

    html_fast: str | None = None
    fast_price: float | None = None

    # Fast path: ישירות לפי fetch_strategy שמור; כשל → שרשרת מלאה למטה.
    if saved:
        try:
            strat = normalize_fetch_strategy(getattr(saved, "fetch_strategy", None))
            html_fast = fetch_html_for_saved_strategy_sync(
                comp.url,
                strat,
                timeout_normal=12.0,
                early_stop_css_selector=saved.css_selector,
            )
            fast_price = apply_saved_selector(html_fast, saved.css_selector)
            if fast_price is not None:
                logger.info("competitor_fast_path_hit link=%s domain=%s", comp.id, domain)
        except Exception:
            # Any cheap-path miss/failure falls through to full fetch flow.
            pass

    html: str
    if html_fast is not None and fast_price is not None:
        html = html_fast
    else:
        try:
            html = fetch_html_sync(comp.url)
        except FetchHtmlError as e:
            code = e.status_code if e.status_code is not None else 0
            return _finalize_after_fetch_error(
                session, shop, product, comp, domain, prev_comp, published=live, reason=f"HTTP {code}",
            )
        except httpx.HTTPStatusError as e:
            code = e.response.status_code if e.response is not None else 0
            return _finalize_after_fetch_error(
                session, shop, product, comp, domain, prev_comp, published=live, reason=f"HTTP {code}",
            )
        except httpx.HTTPError as e:
            return _finalize_after_fetch_error(
                session, shop, product, comp, domain, prev_comp, published=live, reason=str(e),
            )
        except Exception as e:
            return _finalize_after_fetch_error(
                session, shop, product, comp, domain, prev_comp, published=live, reason=str(e),
            )

    price: float | None = None
    cur: str | None = None
    candidates: list = []

    if live:
        if saved and fast_price is not None:
            price = fast_price
        elif saved:
            price = apply_saved_selector(html, saved.css_selector)
        if price is None:
            result = run_extraction_pipeline(html)
            price = result.get("price")
            cur = result.get("currency")
            candidates = result.get("candidates") or []
    else:
        result = run_extraction_pipeline(html)
        price = result.get("price")
        cur = result.get("currency")
        candidates = result.get("candidates") or []

        dpa = session.get(DomainPriceApproval, domain)
        if not dpa:
            dpa = DomainPriceApproval(domain=domain)
        dpa.status = "pending"
        dpa.sample_url = comp.url
        dpa.pending_price = price
        dpa.pending_currency = cur
        dpa.candidates_json = json.dumps(candidates[:40], ensure_ascii=False)
        if candidates:
            dpa.suggested_selector = candidates[0].get("selector") if isinstance(candidates[0], dict) else None
        dpa.updated_at = utcnow()
        session.add(dpa)

        cand_json = json.dumps(candidates[:40], ensure_ascii=False)
        sug_sel = candidates[0].get("selector") if candidates and isinstance(candidates[0], dict) else None
        existing_q = session.exec(
            select(DomainReviewQueueItem).where(
                DomainReviewQueueItem.competitor_link_id == comp.id,
                DomainReviewQueueItem.status == "pending",
            ),
        ).first()
        if existing_q:
            existing_q.pending_price = price
            existing_q.pending_currency = cur
            existing_q.candidates_json = cand_json
            existing_q.suggested_selector = sug_sel
            existing_q.sample_url = comp.url
            existing_q.source = "scan"
            session.add(existing_q)
        else:
            session.add(
                DomainReviewQueueItem(
                    domain=domain,
                    competitor_link_id=comp.id,
                    shop_id=shop.id,
                    product_name=product.name or "",
                    sample_url=comp.url,
                    pending_price=price,
                    pending_currency=cur,
                    candidates_json=cand_json,
                    suggested_selector=sug_sel,
                    source="scan",
                    status="pending",
                ),
            )

        log = ScanLog(
            shop_id=shop.id,
            product_id=product.id,
            competitor_link_id=comp.id,
            competitor_domain=domain,
            product_name=product.name,
            our_price=product.regular_price,
            competitor_price=price,
            previous_competitor_price=prev_comp,
            price_changed=False,
            comparison="pending_review",
        )
        session.add(log)

        comp.last_checked_at = utcnow()
        session.add(comp)
        session.commit()
        session.refresh(comp)
        return CompetitorCheckResult(price=price, currency=cur, published=False)

    # --- live domain: full publish path ---
    our = product.regular_price
    sanity_reason: str | None = None
    sanity_rejected_val: float | None = None
    if price is not None:
        ok, sanity_reason = validate_competitor_price(session, price, prev_comp, our)
        if not ok:
            sanity_rejected_val = price
            price = None

    if sanity_rejected_val is not None:
        session.add(
            ScanLog(
                shop_id=shop.id,
                product_id=product.id,
                competitor_link_id=comp.id,
                competitor_domain=domain,
                product_name=product.name,
                our_price=our,
                competitor_price=sanity_rejected_val,
                previous_competitor_price=prev_comp,
                price_changed=False,
                comparison="sanity_failed",
            )
        )
        session.add(
            Alert(
                shop_id=shop.id,
                product_id=product.id,
                message=f"סריקה נדחתה ({sanity_reason or 'אמינות'}) — ערך: {sanity_rejected_val} — {product.name}",
                severity="hot",
                kind="sanity_failed",
            )
        )
        comp.last_checked_at = utcnow()
        session.add(comp)
        session.commit()
        session.refresh(comp)
        return CompetitorCheckResult(price=None, currency=cur, published=True)

    price_changed = False
    if prev_comp is None and price is not None:
        price_changed = True
    elif prev_comp is not None and price is not None and abs(prev_comp - price) > 0.02:
        price_changed = True
    elif prev_comp is not None and price is None:
        price_changed = True

    comparison = compare_prices(our, price)

    session.add(
        PriceSnapshot(
            competitor_link_id=comp.id,
            price=price,
            currency=cur,
        )
    )

    session.add(
        ScanLog(
            shop_id=shop.id,
            product_id=product.id,
            competitor_link_id=comp.id,
            competitor_domain=domain,
            product_name=product.name,
            our_price=our,
            competitor_price=price,
            previous_competitor_price=prev_comp,
            price_changed=price_changed,
            comparison=comparison,
        )
    )

    comp.last_price = price
    comp.last_currency = cur
    comp.last_checked_at = utcnow()
    if lw_hash_for_persist:
        comp.last_light_html_hash = lw_hash_for_persist
    session.add(comp)

    if price is not None and our is not None and price < our - 0.01:
        msg = f"המתחרה זול יותר ב־{domain}: {price} מול {our} ({product.name})"
        session.add(
            Alert(
                shop_id=shop.id,
                product_id=product.id,
                message=msg,
                severity="hot",
                kind="competitor_cheaper",
            )
        )
    elif prev_comp is not None and price is not None and abs(prev_comp - price) > 0.02:
        session.add(
            Alert(
                shop_id=shop.id,
                product_id=product.id,
                message=f"שינוי מחיר אצל {domain}: {price} (קודם {prev_comp}) — {product.name}",
                severity="info",
                kind="price_change",
            )
        )
    session.commit()
    session.refresh(comp)

    try:
        maybe_apply_auto_pricing(session, product.id)
    except Exception:
        pass

    return CompetitorCheckResult(price=price, currency=cur, published=True)


def _shop_interval_minutes(shop: Shop) -> int:
    mins = getattr(shop, "package_min_interval_minutes", None)
    if mins is None or mins < 1:
        mins = getattr(shop, "check_interval_minutes", None)
    if mins is None or mins < 1:
        mins = max(1, (shop.check_interval_hours or 6) * 60)
    return max(1, int(mins))


def _shop_scan_cycle_due(shop: Shop, now: datetime) -> bool:
    """מחזור סריקה מלא לחנות — לפי last_scan_cycle_at ולא לפי כל קישור בנפרד."""
    interval = timedelta(minutes=_shop_interval_minutes(shop))
    last = getattr(shop, "last_scan_cycle_at", None)
    if last is None:
        return True
    lc = _aware(last)
    if lc is None:
        return True
    return (now - lc) >= interval


def _daily_quota_key(now: datetime) -> str:
    n = _aware(now) or now
    return n.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _get_or_create_daily_quota(session: Session, shop_id: int, bucket_date: str) -> ShopScanQuotaDaily:
    row = session.exec(
        select(ShopScanQuotaDaily).where(
            ShopScanQuotaDaily.shop_id == shop_id,
            ShopScanQuotaDaily.bucket_date == bucket_date,
        ),
    ).first()
    if row:
        return row
    row = ShopScanQuotaDaily(shop_id=shop_id, bucket_date=bucket_date, runs_count=0, updated_at=utcnow())
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def _shop_daily_quota_exceeded(session: Session, shop: Shop, now: datetime) -> bool:
    max_runs = int(getattr(shop, "package_max_scan_runs_per_day", 10) or 10)
    max_runs = max(1, max_runs)
    bucket = _daily_quota_key(now)
    row = _get_or_create_daily_quota(session, int(shop.id), bucket)
    return int(row.runs_count or 0) >= max_runs


def _increment_shop_daily_quota(session: Session, shop: Shop, now: datetime) -> None:
    bucket = _daily_quota_key(now)
    row = _get_or_create_daily_quota(session, int(shop.id), bucket)
    row.runs_count = int(row.runs_count or 0) + 1
    row.updated_at = utcnow()
    session.add(row)
    session.commit()


def run_scheduled_checks(session: Session) -> tuple[int, int]:
    """
    לכל חנות שהגיע זמן מחזור: מריצים את **כל** קישורי המתחרה ברצף (מיד אחד אחרי השני).
    המרווח check_interval_minutes חל על מחזור מלא — לא על כל קישור בנפרד.

    מחזיר (מספר סריקות מתחרה שבוצעו, מספר חנויות שעברו מחזור).
    """
    now = utcnow()
    shops = session.exec(select(Shop).order_by(Shop.id)).all()
    total = 0
    shops_touched = 0
    skipped_interval_not_due = 0
    skipped_quota_exceeded = 0
    for shop in shops:
        if not _shop_scan_cycle_due(shop, now):
            skipped_interval_not_due += 1
            continue
        if _shop_daily_quota_exceeded(session, shop, now):
            skipped_quota_exceeded += 1
            append_operational_log_safe(
                level="info",
                code="package_quota_exceeded",
                title="חנות דולגה — מכסת סריקות יומית הושלמה",
                detail=(
                    f"shop_id={shop.id} tier={getattr(shop, 'package_tier', 'free')} "
                    f"max_runs_per_day={getattr(shop, 'package_max_scan_runs_per_day', 10)}"
                ),
                shop_id=shop.id,
                competitor_link_id=None,
            )
            continue

        shops_touched += 1
        links = session.exec(
            select(CompetitorLink)
            .join(Product, CompetitorLink.product_id == Product.id)
            .where(Product.shop_id == shop.id)
            .order_by(Product.id, CompetitorLink.id),
        ).all()

        for comp in links:
            try:
                run_competitor_check(session, comp.id)
                total += 1
            except Exception as ex:
                logger.warning("scheduled scan failed for competitor %s: %s", comp.id, ex, exc_info=True)
                code, title_he, detail = classify_competitor_scan_failure(ex, shop, comp.id)
                append_operational_log_safe(
                    level="warning",
                    code=code,
                    title=title_he,
                    detail=detail,
                    shop_id=shop.id,
                    competitor_link_id=comp.id,
                )
                try:
                    session.rollback()
                    c2 = session.get(CompetitorLink, comp.id)
                    if c2:
                        c2.last_checked_at = utcnow()
                        session.add(c2)
                        session.commit()
                except Exception:
                    try:
                        session.rollback()
                    except Exception:
                        pass

        shop.last_scan_cycle_at = utcnow()
        session.add(shop)
        session.commit()
        session.refresh(shop)
        _increment_shop_daily_quota(session, shop, now)

    if skipped_interval_not_due or skipped_quota_exceeded:
        append_operational_log_safe(
            level="info",
            code="package_cycle_skip_summary",
            title="דילוגי סריקה לפי מדיניות חבילות",
            detail=(
                f"interval_not_due={skipped_interval_not_due} "
                f"quota_exceeded={skipped_quota_exceeded}"
            ),
            shop_id=None,
            competitor_link_id=None,
        )

    return total, shops_touched
