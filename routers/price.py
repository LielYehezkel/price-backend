import json
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
from backend.services.resolve_cache import get_cache, put_cache

router = APIRouter(prefix="/api/price", tags=["price"])


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
    if saved and not ignore_saved_selector:
        strat = normalize_fetch_strategy(getattr(saved, "fetch_strategy", None))
        try:
            html_fast = await fetch_html_for_saved_strategy(url, strat, timeout_normal=12.0)
            price_fast = apply_saved_selector(html_fast, saved.css_selector)
            if price_fast:
                learned = saved.css_selector
                token = put_cache(html_fast, url)
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
        fetched = await fetch_html_with_fallback_meta(url)
        html = fetched.html
        strat_used = fetched.strategy
    except FetchHtmlError as e:
        raise HTTPException(fetch_error_api_status(e), format_fetch_error_hebrew(e)) from None

    if saved and not ignore_saved_selector:
        price = apply_saved_selector(html, saved.css_selector)
        if price:
            learned = saved.css_selector
            token = put_cache(html, url)
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
    return ResolveOut(
        url=url,
        domain=domain,
        price=result.get("price"),
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
            fetched = await fetch_html_with_fallback_meta(url)
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
    session.commit()
    return {"ok": True, "domain": domain, "validated_price": price}
