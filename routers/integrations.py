from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlmodel import Session

from backend.db import get_session
from backend.deps import get_current_user
from backend.models import Shop, User, WpConnectionToken, WpSetupToken, utcnow
from backend.services.store_connector import store_platform
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
    """נקרא מתוך תוסף WordPress — ללא JWT; תומך בטוקן חד-פעמי ישן או טוקן חיבור קבוע."""
    setup_token = body.setup_token.strip()
    shop: Shop | None = None
    legacy_tok: WpSetupToken | None = session.get(WpSetupToken, setup_token)
    conn_tok: WpConnectionToken | None = None

    if legacy_tok:
        if legacy_tok.used_at is not None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "טוקן כבר נוצל")
        if legacy_tok.expires_at < utcnow():
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "פג תוקף — הורידו תוסף מחדש")
        shop = session.get(Shop, legacy_tok.shop_id)
    else:
        conn_tok = session.get(WpConnectionToken, setup_token)
        if not conn_tok or not conn_tok.active:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "טוקן לא תקף")
        shop = session.get(Shop, conn_tok.shop_id)

    if not shop:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "חנות לא נמצאה")
    if store_platform(shop) == "shopify":
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "חנות זו מוגדרת כ־Shopify — חיבור WordPress/WooCommerce אינו רלוונטי. השתמשו בהגדרות Shopify.",
        )

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

    now = utcnow()
    if legacy_tok:
        legacy_tok.used_at = now
        session.add(legacy_tok)
    if conn_tok:
        conn_tok.last_used_at = now
        session.add(conn_tok)
    session.add(shop)
    session.commit()
    return {"ok": True, "shop_id": shop.id, "woo_currency": shop.woo_currency}


@router.get("/shopify/oauth/start", response_model=UrlOut)
def shopify_oauth_start(_user: Annotated[User, Depends(get_current_user)]) -> UrlOut:
    return UrlOut(url="https://shopify.com/oauth-placeholder (הגדר מפתחות ב-.env)")


@router.post("/stripe/billing-portal", response_model=UrlOut)
def stripe_portal(_user: Annotated[User, Depends(get_current_user)]) -> UrlOut:
    return UrlOut(url="https://billing.stripe.com/p/login/placeholder")
