import asyncio
import json
import logging
from typing import Annotated, Any
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session

from backend.db import get_session
from backend.models import DomainPriceSelector
from backend.services.extract import (
    apply_saved_selector,
    normalize_domain,
    run_extraction_pipeline,
    validate_selector_with_fallbacks,
)
from backend.services.fetch_html import (
    FetchHtmlError,
    fetch_error_api_status,
    fetch_html_for_saved_strategy,
    fetch_html_with_fallback_meta,
    format_fetch_error_hebrew,
    normalize_fetch_strategy,
)
from backend.services.domain_policy import clear_domain_review_pending_for_live_domain
from backend.services.competitor_lightweight_precheck import LightweightCheckOutcome, lightweight_check
from backend.services.price_resolve_lightweight_gate import (
    apply_price_resolve_lightweight_decision,
    persist_resolve_url_cache_after_heavy_fetch,
)
from backend.services.resolve_cache import get_cache, put_cache

router = APIRouter(prefix="/api/price", tags=["price"])
logger = logging.getLogger(__name__)


class ResolveIn(BaseModel):
    url: str


class ResolveOut(BaseModel):
    url: str
    domain: str
    price: float | None
    currency: str | None
    source: str | None
    learned_selector: str | None
    candidates: list[dict[str, Any]]
    resolution_token: str | None
    # אסטרטגיית המשיכה ששימשה ל-HTML של תשובה זו — לשליחה בחזרה ב-/confirm
    fetch_strategy_used: str | None = None


def _validate_url(raw: str) -> str:
    p = urlparse(raw if "://" in raw else f"https://{raw}")
    if not p.scheme.startswith("http") or not p.netloc:
        raise HTTPException(400, "כתובת לא תקינה")
    return raw if "://" in raw else f"https://{raw}"


async def run_price_resolve(session: Session, url_raw: str, *, ignore_saved_selector: bool) -> ResolveOut:
    """לוגיקת resolve — לשימוש מ־/resolve ומפאנל אדמין (עם התעלמות מסלקטור שמור)."""
    url = _validate_url(url_raw.strip())
    domain = normalize_domain(url)
    learned: str | None = None
    saved = session.get(DomainPriceSelector, domain)
    lw_snapshot = None

    if saved and not ignore_saved_selector:
        try:
            sel_gate = (saved.css_selector or "").strip()
            if not sel_gate:
                lw_snapshot = None
                gate = apply_price_resolve_lightweight_decision(
                    session,
                    url=url,
                    domain=domain,
                    css_selector="",
                    learned_selector_label=saved.css_selector,
                    lw=LightweightCheckOutcome(ok=False),
                )
            else:
                lw_snapshot = await asyncio.to_thread(lightweight_check, url, sel_gate)
                gate = apply_price_resolve_lightweight_decision(
                    session,
                    url=url,
                    domain=domain,
                    css_selector=sel_gate,
                    learned_selector_label=saved.css_selector,
                    lw=lw_snapshot,
                )
            if gate.resolve_out_dict is not None:
                return ResolveOut(**gate.resolve_out_dict)
            if gate.fallback_to_playwright_reason:
                logger.warning(
                    "price_resolve_heavy_fetch_start url=%s prior_gate_reason=%s",
                    url,
                    gate.fallback_to_playwright_reason,
                )
        except Exception:
            lw_snapshot = None
            logger.debug("price_resolve_lightweight_gate_exception url=%s", url, exc_info=True)

    if saved and not ignore_saved_selector:
        strat = normalize_fetch_strategy(getattr(saved, "fetch_strategy", None))
        try:
            html_fast = await fetch_html_for_saved_strategy(
                url,
                strat,
                timeout_normal=12.0,
                early_stop_css_selector=saved.css_selector,
            )
            price_fast = apply_saved_selector(html_fast, saved.css_selector)
            if price_fast:
                learned = saved.css_selector
                token = put_cache(html_fast, url)
                try:
                    persist_resolve_url_cache_after_heavy_fetch(
                        session,
                        url=url,
                        domain=domain,
                        price=price_fast,
                        html=html_fast,
                        lw_snapshot=lw_snapshot,
                    )
                except Exception:
                    logger.debug("persist_resolve_url_cache failed url=%s", url, exc_info=True)
                return ResolveOut(
                    url=url,
                    domain=domain,
                    price=price_fast,
                    currency=None,
                    source="learned_selector",
                    learned_selector=learned,
                    candidates=[],
                    resolution_token=token,
                    fetch_strategy_used=strat,
                )
        except Exception:
            pass

    try:
        fetched = await fetch_html_with_fallback_meta(
            url,
            playwright_early_stop_css_selector=(
                saved.css_selector if saved and not ignore_saved_selector else None
            ),
        )
        html = fetched.html
        strat_used = fetched.strategy
    except FetchHtmlError as e:
        raise HTTPException(fetch_error_api_status(e), format_fetch_error_hebrew(e)) from None

    if saved and not ignore_saved_selector:
        price = apply_saved_selector(html, saved.css_selector)
        if price:
            learned = saved.css_selector
            token = put_cache(html, url)
            try:
                persist_resolve_url_cache_after_heavy_fetch(
                    session,
                    url=url,
                    domain=domain,
                    price=price,
                    html=html,
                    lw_snapshot=lw_snapshot,
                )
            except Exception:
                logger.debug("persist_resolve_url_cache failed url=%s", url, exc_info=True)
            return ResolveOut(
                url=url,
                domain=domain,
                price=price,
                currency=None,
                source="learned_selector",
                learned_selector=learned,
                candidates=[],
                resolution_token=token,
                fetch_strategy_used=strat_used,
            )

    result = run_extraction_pipeline(html)
    token = put_cache(html, url)
    out_price = result.get("price")
    if out_price is not None:
        try:
            persist_resolve_url_cache_after_heavy_fetch(
                session,
                url=url,
                domain=domain,
                price=float(out_price),
                html=html,
                lw_snapshot=lw_snapshot,
            )
        except Exception:
            logger.debug("persist_resolve_url_cache failed url=%s", url, exc_info=True)
    return ResolveOut(
        url=url,
        domain=domain,
        price=out_price,
        currency=result.get("currency"),
        source=result.get("source"),
        learned_selector=None,
        candidates=result.get("candidates") or [],
        resolution_token=token,
        fetch_strategy_used=strat_used,
    )


@router.post("/resolve", response_model=ResolveOut)
async def resolve_price(
    body: ResolveIn,
    session: Annotated[Session, Depends(get_session)],
) -> ResolveOut:
    return await run_price_resolve(session, body.url, ignore_saved_selector=False)


class ConfirmIn(BaseModel):
    url: str
    css_selector: str
    resolution_token: str | None = None
    selector_alternates: list[str] | None = None
    fetch_strategy: str | None = None  # http | playwright_proxy (מ-resolve או ידני)


def _confirm_fetch_strategy_value(body: ConfirmIn, existing: DomainPriceSelector | None) -> str:
    if body.fetch_strategy is not None and str(body.fetch_strategy).strip():
        return normalize_fetch_strategy(body.fetch_strategy)
    if existing is not None and getattr(existing, "fetch_strategy", None):
        return normalize_fetch_strategy(existing.fetch_strategy)
    return normalize_fetch_strategy(None)


@router.post("/confirm")
async def confirm_selector(
    body: ConfirmIn,
    session: Annotated[Session, Depends(get_session)],
) -> dict[str, Any]:
    raw_url = str(body.url)
    url = _validate_url(raw_url.strip())
    domain = normalize_domain(url)

    row_existing = session.get(DomainPriceSelector, domain)
    strat_save = _confirm_fetch_strategy_value(body, row_existing)

    html: str
    if body.resolution_token:
        cached = get_cache(body.resolution_token)
        if not cached or normalize_domain(cached.url) != domain:
            raise HTTPException(400, "מטמון פג תוקף או לא תואם לדומיין — הרץ שוב resolve")
        html = cached.html
    else:
        try:
            fetched = await fetch_html_with_fallback_meta(
                url,
                playwright_early_stop_css_selector=(body.css_selector or "").strip() or None,
            )
            html = fetched.html
            strat_save = fetched.strategy
        except FetchHtmlError as e:
            raise HTTPException(fetch_error_api_status(e), format_fetch_error_hebrew(e)) from None

    alts = body.selector_alternates or []
    price, used = validate_selector_with_fallbacks(html, body.css_selector, alts)
    if price is None:
        raise HTTPException(400, "לא ניתן לאמת את הסלקטור על גרסת הדף האחרונה")

    row = session.get(DomainPriceSelector, domain)
    alts_json = json.dumps(alts) if alts else None
    if row:
        row.css_selector = used or body.css_selector
        row.alternates_json = alts_json
        row.fetch_strategy = strat_save
        session.add(row)
    else:
        session.add(
            DomainPriceSelector(
                domain=domain,
                css_selector=used or body.css_selector,
                alternates_json=alts_json,
                fetch_strategy=strat_save,
            )
        )
    session.flush()
    clear_domain_review_pending_for_live_domain(session, domain)
    session.commit()
    return {"ok": True, "domain": domain, "validated_price": price}
