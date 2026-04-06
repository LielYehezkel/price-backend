from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session

from backend.db import get_session
from backend.deps import get_current_user
from backend.models import Shop, User, WpSetupToken, utcnow
from backend.services.woo_sync import fetch_wc_store_currency

router = APIRouter(prefix="/api/integrations", tags=["integrations"])


class UrlOut(BaseModel):
    url: str


class WordPressConnectIn(BaseModel):
    setup_token: str
    site_url: str
    consumer_key: str
    consumer_secret: str


@router.post("/wordpress/connect")
def wordpress_connect(
    body: WordPressConnectIn,
    session: Annotated[Session, Depends(get_session)],
) -> dict:
    """נקרא מתוך תוסף WordPress — ללא JWT; מאמת טוקן הקמה חד-פעמי."""
    tok = session.get(WpSetupToken, body.setup_token.strip())
    if not tok:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "טוקן לא תקף")
    if tok.used_at is not None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "טוקן כבר נוצל")
    if tok.expires_at < utcnow():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "פג תוקף — הורידו תוסף מחדש")

    shop = session.get(Shop, tok.shop_id)
    if not shop:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "חנות לא נמצאה")

    site = body.site_url.strip().rstrip("/")
    ck = body.consumer_key.strip()
    cs = body.consumer_secret.strip()
    if not site or not ck or not cs:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "חסרים פרטים")

    shop.woo_site_url = site
    shop.woo_consumer_key = ck
    shop.woo_consumer_secret = cs
    cur = fetch_wc_store_currency(site, ck, cs)
    if cur:
        shop.woo_currency = cur

    tok.used_at = utcnow()
    session.add(shop)
    session.add(tok)
    session.commit()
    return {"ok": True, "shop_id": shop.id, "woo_currency": shop.woo_currency}


@router.get("/shopify/oauth/start", response_model=UrlOut)
def shopify_oauth_start(_user: Annotated[User, Depends(get_current_user)]) -> UrlOut:
    return UrlOut(url="https://shopify.com/oauth-placeholder (הגדר מפתחות ב-.env)")


@router.post("/stripe/billing-portal", response_model=UrlOut)
def stripe_portal(_user: Annotated[User, Depends(get_current_user)]) -> UrlOut:
    return UrlOut(url="https://billing.stripe.com/p/login/placeholder")
