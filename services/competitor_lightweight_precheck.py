"""
בדיקת מחיר קלה לפני סריקה מלאה — GET פשוט + selector שמור, ללא Playwright.
כל כשל או ספק → המפעיל ממשיך לזרימה הקיימת בלי שינוי.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass

import httpx

from backend.models import CompetitorLink, DomainPriceSelector, Product, Shop, ScanLog, utcnow
from backend.services.extract import apply_saved_selector
from backend.services.price_sanity import validate_competitor_price

logger = logging.getLogger(__name__)

# עד 50KB ראשונים ל-hash (כמו בדרישה)
_LIGHT_HTML_PREFIX_LEN = 50 * 1024

_LIGHT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_PRICE_MATCH_EPS = 0.02


@dataclass
class LightweightCheckOutcome:
    """תוצאת lightweight_check — רק אם ok=True ניתן לסמוך על השדות."""

    ok: bool
    status_code: int | None = None
    price: float | None = None
    prefix_hash: str | None = None
    html: str | None = None  # גוף HTML מלא ל-put_cache כשמדלגים על fetch כבד


def _hash_html_prefix(html: str) -> str:
    chunk = (html or "")[:_LIGHT_HTML_PREFIX_LEN].encode("utf-8", errors="ignore")
    return hashlib.sha256(chunk).hexdigest()


def html_prefix_hash_from_html(html: str) -> str:
    """Hash ציבורי ל-50KB ראשונים — לאחר fetch מלא לעדכון מטמון resolve."""
    return _hash_html_prefix(html)


def lightweight_check(url: str, css_selector: str, *, timeout: float = 10.0) -> LightweightCheckOutcome:
    """
    שלב א: GET פשוט (httpx), בלי Playwright.
    שלב ב: חילוץ מחיר עם אותו selector כמו במערכת (apply_saved_selector).
    """
    sel = (css_selector or "").strip()
    if not sel:
        return LightweightCheckOutcome(ok=False)

    headers = {
        "User-Agent": _LIGHT_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=httpx.Timeout(timeout, connect=min(5.0, timeout)),
            trust_env=True,
        ) as client:
            r = client.get(url, headers=headers)
        if r.status_code >= 400:
            return LightweightCheckOutcome(ok=False, status_code=r.status_code)
        html = r.text or ""
        if not html.strip():
            return LightweightCheckOutcome(ok=False, status_code=r.status_code)
        ph = _hash_html_prefix(html)
        price = apply_saved_selector(html, sel)
        return LightweightCheckOutcome(
            ok=True,
            status_code=r.status_code,
            price=price,
            prefix_hash=ph,
            html=html,
        )
    except Exception as ex:
        logger.debug("lightweight_check failed url=%s err=%s", url, ex, exc_info=True)
        return LightweightCheckOutcome(ok=False)


@dataclass(frozen=True)
class LightweightEarlySkip:
    """תוצאה מוכנה ל-CompetitorCheckResult ב-monitor_checks."""

    price: float | None
    currency: str | None
    published: bool


def try_competitor_lightweight_precheck_skip(
    session,
    shop: Shop,
    product: Product,
    comp: CompetitorLink,
    domain: str,
    saved: DomainPriceSelector,
    prev_comp: float | None,
) -> tuple[LightweightEarlySkip | None, str | None]:
    """
    אם אפשר לדלג על סריקה מלאה — מחזיר (תוצאה, None).
    אם צריך סריקה מלאה — מחזיר (None, prefix_hash) כשיש hash לשמירה אחרי סריקה מוצלחת,
    או (None, None) אם נכשל.
    """
    try:
        lw = lightweight_check(comp.url, saved.css_selector)
    except Exception:
        return None, None

    if not lw.ok or not lw.prefix_hash:
        return None, None

    prefix_hash = lw.prefix_hash
    our = product.regular_price

    # --- מחיר זהה למחיר האחרון ---
    if lw.price is not None and comp.last_price is not None:
        if abs(float(lw.price) - float(comp.last_price)) <= _PRICE_MATCH_EPS:
            ok_sanity, _ = validate_competitor_price(session, lw.price, prev_comp, our)
            if not ok_sanity:
                logger.info(
                    "competitor_lightweight_skip_suppressed link=%s reason=sanity",
                    comp.id,
                )
                return None, prefix_hash

            comp.last_checked_at = utcnow()
            comp.last_light_html_hash = prefix_hash
            session.add(comp)
            session.add(
                ScanLog(
                    shop_id=shop.id,
                    product_id=product.id,
                    competitor_link_id=comp.id,
                    competitor_domain=domain,
                    product_name=product.name or "",
                    our_price=our,
                    competitor_price=lw.price,
                    previous_competitor_price=prev_comp,
                    price_changed=False,
                    comparison=_compare_prices_light(our, lw.price),
                ),
            )
            session.commit()
            session.refresh(comp)
            logger.info("competitor_lightweight_skip link=%s reason=price_unchanged", comp.id)
            return LightweightEarlySkip(price=lw.price, currency=comp.last_currency, published=True), None

    # --- ללא מחיר מהבדיקה הקלה: השוואת hash לדף קבוע ---
    stored_hash = (comp.last_light_html_hash or "").strip()
    if lw.price is None and stored_hash and lw.prefix_hash == stored_hash:
        comp.last_checked_at = utcnow()
        session.add(comp)
        session.add(
            ScanLog(
                shop_id=shop.id,
                product_id=product.id,
                competitor_link_id=comp.id,
                competitor_domain=domain,
                product_name=product.name or "",
                our_price=our,
                competitor_price=comp.last_price,
                previous_competitor_price=prev_comp,
                price_changed=False,
                comparison=_compare_prices_light(our, comp.last_price),
            ),
        )
        session.commit()
        session.refresh(comp)
        logger.info("competitor_lightweight_skip link=%s reason=html_prefix_hash_unchanged", comp.id)
        return LightweightEarlySkip(price=comp.last_price, currency=comp.last_currency, published=True), None

    return None, prefix_hash


def _compare_prices_light(our: float | None, comp_price: float | None) -> str:
    if our is None or comp_price is None:
        return "unknown"
    eps = 0.02
    if abs(our - comp_price) <= eps:
        return "tie"
    return "you_cheaper" if our < comp_price else "you_expensive"
