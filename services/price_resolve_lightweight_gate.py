"""
שער החלטה לפני fetch כבד ב-/api/price/resolve כשיש סלקטור שמור.
Lightweight GET + מטמון לפי URL; רק כשאין ביטחון — ממשיכים ל-playwright כמו קודם.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse, urlunsplit

from sqlmodel import Session

from backend.models import PriceResolveUrlCache, utcnow
from backend.services.competitor_lightweight_precheck import LightweightCheckOutcome, html_prefix_hash_from_html
from backend.services.resolve_cache import put_cache

logger = logging.getLogger(__name__)

HASH_SKIP_MAX_AGE_SEC = 24 * 3600
PRICE_MATCH_EPS = 0.02
FETCH_STRATEGY_LIGHTWEIGHT = "http_lightweight"


def normalize_resolve_url(url: str) -> str:
    raw = url.strip()
    if "://" not in raw:
        raw = f"https://{raw}"
    p = urlparse(raw)
    scheme = (p.scheme or "https").lower()
    if not scheme.startswith("http"):
        scheme = "https"
    netloc = (p.netloc or "").lower()
    path = p.path if p.path else "/"
    return urlunsplit((scheme, netloc, path, p.query, ""))


def price_resolve_url_cache_key(canonical_url: str) -> str:
    return hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()


def _age_seconds(since: datetime | None) -> float:
    if since is None:
        return HASH_SKIP_MAX_AGE_SEC + 1.0
    return max(0.0, (utcnow() - since).total_seconds())


def _touch_cache(session: Session, row: PriceResolveUrlCache) -> None:
    row.last_checked_at = utcnow()
    session.add(row)
    session.commit()


def _upsert_cache(
    session: Session,
    *,
    url_key: str,
    canonical: str,
    domain: str,
    last_price: float | None,
    prefix_hash: str | None,
) -> None:
    row = session.get(PriceResolveUrlCache, url_key)
    now = utcnow()
    dom = domain[:512]
    can = canonical[:4096]
    if row is None:
        session.add(
            PriceResolveUrlCache(
                url_key=url_key,
                url_canonical=can,
                domain=dom,
                last_price=last_price,
                last_checked_at=now,
                last_html_prefix_hash=prefix_hash,
            ),
        )
    else:
        row.url_canonical = can
        row.domain = dom
        row.last_price = last_price
        row.last_checked_at = now
        row.last_html_prefix_hash = prefix_hash
        session.add(row)
    session.commit()


def _log_decision(
    url: str,
    *,
    skipped_full_scan_reason: str | None,
    used_cached_price: bool,
    lightweight_price_found: float | None,
    fallback_to_playwright_reason: str | None,
) -> None:
    lp = lightweight_price_found if lightweight_price_found is not None else "-"
    logger.warning(
        "price_resolve_decision url=%s skipped_full_scan_reason=%s used_cached_price=%s "
        "lightweight_price_found=%s fallback_to_playwright_reason=%s",
        url,
        skipped_full_scan_reason or "-",
        used_cached_price,
        lp,
        fallback_to_playwright_reason or "-",
    )


@dataclass
class SavedPathLightweightGateResult:
    """אם resolve_out_dict לא None — להחזיר מיד ResolveOut(**dict). אחרת להמשיך ל-fetch כבד."""

    resolve_out_dict: dict | None
    lw_outcome: LightweightCheckOutcome | None
    fallback_to_playwright_reason: str | None


def apply_price_resolve_lightweight_decision(
    session: Session,
    *,
    url: str,
    domain: str,
    css_selector: str,
    learned_selector_label: str,
    lw: LightweightCheckOutcome,
) -> SavedPathLightweightGateResult:
    """
    החלטה על סמך תוצאת lightweight שכבר רצה (ב-thread נפרד).
    מעדכן מטמון DB; מחזיר תשובת resolve אם אפשר לדלג על playwright.
    """
    sel = (css_selector or "").strip()
    if not sel:
        _log_decision(
            url,
            skipped_full_scan_reason=None,
            used_cached_price=False,
            lightweight_price_found=None,
            fallback_to_playwright_reason="no_css_selector",
        )
        return SavedPathLightweightGateResult(None, None, "no_css_selector")

    canonical = normalize_resolve_url(url)
    url_key = price_resolve_url_cache_key(canonical)
    cache = session.get(PriceResolveUrlCache, url_key)

    if not lw.ok:
        _log_decision(
            url,
            skipped_full_scan_reason=None,
            used_cached_price=False,
            lightweight_price_found=None,
            fallback_to_playwright_reason="lightweight_fetch_failed",
        )
        return SavedPathLightweightGateResult(None, lw, "lightweight_fetch_failed")

    # --- מחיר מה-lightweight ---
    if lw.price is not None:
        if cache is not None and cache.last_price is not None:
            if abs(float(lw.price) - float(cache.last_price)) <= PRICE_MATCH_EPS:
                _touch_cache(session, cache)
                token = put_cache(lw.html or "", url)
                _log_decision(
                    url,
                    skipped_full_scan_reason="lightweight_price_matches_cache",
                    used_cached_price=True,
                    lightweight_price_found=lw.price,
                    fallback_to_playwright_reason=None,
                )
                return SavedPathLightweightGateResult(
                    {
                        "url": url,
                        "domain": domain,
                        "price": lw.price,
                        "currency": None,
                        "source": "learned_selector",
                        "learned_selector": learned_selector_label,
                        "candidates": [],
                        "resolution_token": token,
                        "fetch_strategy_used": FETCH_STRATEGY_LIGHTWEIGHT,
                    },
                    lw,
                    None,
                )
            _log_decision(
                url,
                skipped_full_scan_reason=None,
                used_cached_price=False,
                lightweight_price_found=lw.price,
                fallback_to_playwright_reason="lightweight_price_differs_from_cache",
            )
            return SavedPathLightweightGateResult(None, lw, "lightweight_price_differs_from_cache")

        # אין מטמון מחיר — baseline חדש מה-lightweight (חוסך playwright כשהדף נגיש ב-GET פשוט)
        _upsert_cache(
            session,
            url_key=url_key,
            canonical=canonical,
            domain=domain,
            last_price=float(lw.price),
            prefix_hash=lw.prefix_hash,
        )
        token = put_cache(lw.html or "", url)
        _log_decision(
            url,
            skipped_full_scan_reason="lightweight_new_baseline_no_cache",
            used_cached_price=False,
            lightweight_price_found=lw.price,
            fallback_to_playwright_reason=None,
        )
        return SavedPathLightweightGateResult(
            {
                "url": url,
                "domain": domain,
                "price": lw.price,
                "currency": None,
                "source": "learned_selector",
                "learned_selector": learned_selector_label,
                "candidates": [],
                "resolution_token": token,
                "fetch_strategy_used": FETCH_STRATEGY_LIGHTWEIGHT,
            },
            lw,
            None,
        )

    # --- אין מחיר מה-lightweight: hash + מטמון ---
    if (
        cache is not None
        and cache.last_price is not None
        and (cache.last_html_prefix_hash or "").strip()
        and lw.prefix_hash
        and lw.prefix_hash == cache.last_html_prefix_hash
        and _age_seconds(cache.last_checked_at) <= HASH_SKIP_MAX_AGE_SEC
    ):
        _touch_cache(session, cache)
        token = put_cache(lw.html or "", url)
        _log_decision(
            url,
            skipped_full_scan_reason="html_prefix_hash_unchanged_within_ttl",
            used_cached_price=True,
            lightweight_price_found=None,
            fallback_to_playwright_reason=None,
        )
        return SavedPathLightweightGateResult(
            {
                "url": url,
                "domain": domain,
                "price": cache.last_price,
                "currency": None,
                "source": "learned_selector",
                "learned_selector": learned_selector_label,
                "candidates": [],
                "resolution_token": token,
                "fetch_strategy_used": FETCH_STRATEGY_LIGHTWEIGHT,
            },
            lw,
            None,
        )

    reason = "lightweight_no_price_html_changed_or_no_hash_baseline"
    if cache is None or cache.last_price is None:
        reason = "lightweight_no_price_no_cache_baseline"
    elif not (cache.last_html_prefix_hash or "").strip():
        reason = "lightweight_no_price_missing_stored_hash"
    elif lw.prefix_hash != cache.last_html_prefix_hash:
        reason = "lightweight_no_price_html_prefix_changed"
    elif _age_seconds(cache.last_checked_at) > HASH_SKIP_MAX_AGE_SEC:
        reason = "lightweight_no_price_cache_stale_for_hash_skip"

    _log_decision(
        url,
        skipped_full_scan_reason=None,
        used_cached_price=False,
        lightweight_price_found=None,
        fallback_to_playwright_reason=reason,
    )
    return SavedPathLightweightGateResult(None, lw, reason)


def persist_resolve_url_cache_after_heavy_fetch(
    session: Session,
    *,
    url: str,
    domain: str,
    price: float | None,
    html: str,
    lw_snapshot: LightweightCheckOutcome | None,
) -> None:
    """אחרי fetch כבד מוצלח — לעדכן מטמון לפי URL (מחיר + hash לריצה הבאה)."""
    if price is None:
        return
    canonical = normalize_resolve_url(url)
    url_key = price_resolve_url_cache_key(canonical)
    prefix: str | None = None
    if lw_snapshot is not None and lw_snapshot.ok and lw_snapshot.prefix_hash:
        prefix = lw_snapshot.prefix_hash
    elif html:
        prefix = html_prefix_hash_from_html(html)
    _upsert_cache(
        session,
        url_key=url_key,
        canonical=canonical,
        domain=domain,
        last_price=float(price),
        prefix_hash=prefix,
    )
