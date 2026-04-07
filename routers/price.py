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
from backend.services.fetch_html import FetchHtmlError, fetch_html, format_fetch_error_hebrew
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


def _validate_url(raw: str) -> str:
    p = urlparse(raw if "://" in raw else f"https://{raw}")
    if not p.scheme.startswith("http") or not p.netloc:
        raise HTTPException(400, "כתובת לא תקינה")
    return raw if "://" in raw else f"https://{raw}"


async def run_price_resolve(session: Session, url_raw: str, *, ignore_saved_selector: bool) -> ResolveOut:
    """לוגיקת resolve — לשימוש מ־/resolve ומפאנל אדמין (עם התעלמות מסלקטור שמור)."""
    url = _validate_url(url_raw.strip())
    domain = normalize_domain(url)
    try:
        html = await fetch_html(url)
    except FetchHtmlError as e:
        raise HTTPException(502, format_fetch_error_hebrew(e)) from None

    learned: str | None = None
    saved = session.get(DomainPriceSelector, domain)
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


@router.post("/confirm")
async def confirm_selector(
    body: ConfirmIn,
    session: Annotated[Session, Depends(get_session)],
) -> dict[str, Any]:
    raw_url = str(body.url)
    url = _validate_url(raw_url.strip())
    domain = normalize_domain(url)

    html: str
    if body.resolution_token:
        cached = get_cache(body.resolution_token)
        if not cached or normalize_domain(cached.url) != domain:
            raise HTTPException(400, "מטמון פג תוקף או לא תואם לדומיין — הרץ שוב resolve")
        html = cached.html
    else:
        try:
            html = await fetch_html(url)
        except FetchHtmlError as e:
            raise HTTPException(502, format_fetch_error_hebrew(e)) from None

    alts = body.selector_alternates or []
    price, used = validate_selector_with_fallbacks(html, body.css_selector, alts)
    if price is None:
        raise HTTPException(400, "לא ניתן לאמת את הסלקטור על גרסת הדף האחרונה")

    row = session.get(DomainPriceSelector, domain)
    alts_json = json.dumps(alts) if alts else None
    if row:
        row.css_selector = used or body.css_selector
        row.alternates_json = alts_json
        session.add(row)
    else:
        session.add(
            DomainPriceSelector(
                domain=domain,
                css_selector=used or body.css_selector,
                alternates_json=alts_json,
            )
        )
    session.commit()
    return {"ok": True, "domain": domain, "validated_price": price}
