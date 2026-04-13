from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlmodel import Session, select

from backend.config import settings
from backend.db import get_session
from backend.deps import get_current_user, require_shop_access
from backend.models import Product, User
from backend.services.ai_ops import parse_intent_with_openai, rank_product_candidates
from backend.services.woo_sync import (
    fetch_wc_product_by_id,
    patch_wc_product_out_of_stock,
    patch_wc_product_regular_price,
)

router = APIRouter(prefix="/api/shops", tags=["ai-ops"])


class ChatPlanIn(BaseModel):
    message: str


class ChatCandidateOut(BaseModel):
    product_id: int
    name: str
    score: float
    current_price: float | None


class ChatPlanOut(BaseModel):
    status: Literal["needs_confirmation", "needs_disambiguation", "cannot_plan"]
    action: Literal["reduce_price", "out_of_stock", "unknown"]
    question: str
    product_id: int | None = None
    product_name: str | None = None
    delta_amount: float | None = None
    from_price: float | None = None
    to_price: float | None = None
    currency: str | None = None
    candidates: list[ChatCandidateOut] = []
    confirm_payload: dict[str, Any] | None = None


class ChatConfirmIn(BaseModel):
    approved: bool
    payload: dict[str, Any]


class ChatConfirmOut(BaseModel):
    status: Literal["executed", "cancelled"]
    action: str
    product_id: int | None = None
    product_name: str | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None


def _ensure_chat_enabled() -> None:
    if not settings.ai_chat_enabled:
        raise HTTPException(404, "פיצ'ר צ'אט AI כבוי כרגע")


def _build_confirmation_for_price(name: str, old_price: float, new_price: float) -> str:
    return f'האם להוריד את המחיר של "{name}" מ{old_price:,.2f} ש"ח ל{new_price:,.2f} ש"ח?'


def _build_confirmation_for_stock(name: str) -> str:
    return f'האם להוריד את המוצר "{name}" מהמלאי?'


@router.post("/{shop_id}/ai/chat/plan", response_model=ChatPlanOut)
async def plan_chat_action(
    shop_id: int,
    body: ChatPlanIn,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> ChatPlanOut:
    _ensure_chat_enabled()
    shop = require_shop_access(session, user, shop_id)
    msg = (body.message or "").strip()
    if not msg:
        raise HTTPException(400, "הודעה חסרה")

    intent = await parse_intent_with_openai(msg)
    if intent.action not in ("reduce_price", "out_of_stock"):
        return ChatPlanOut(status="cannot_plan", action="unknown", question="לא זיהיתי פעולה נתמכת בהודעה.")

    products = session.exec(select(Product).where(Product.shop_id == shop.id)).all()
    if not products:
        return ChatPlanOut(status="cannot_plan", action=intent.action, question="לא נמצאו מוצרים בחנות.")

    ranked = rank_product_candidates(intent.product_query or msg, products, top_k=5)
    if not ranked:
        return ChatPlanOut(status="cannot_plan", action=intent.action, question="לא הצלחתי לזהות מוצר מתאים.")

    best = ranked[0]
    second = ranked[1] if len(ranked) > 1 else None
    ambiguous = second is not None and second.score >= best.score - 0.08
    if best.score < 0.45 or ambiguous:
        return ChatPlanOut(
            status="needs_disambiguation",
            action=intent.action,
            question="מצאתי כמה מוצרים דומים. איזה מוצר התכוונת?",
            candidates=[
                ChatCandidateOut(
                    product_id=c.product_id,
                    name=c.name,
                    score=round(c.score, 3),
                    current_price=c.current_price,
                )
                for c in ranked
            ],
        )

    target = session.get(Product, best.product_id)
    if not target:
        return ChatPlanOut(status="cannot_plan", action=intent.action, question="המוצר שנבחר לא נמצא יותר.")

    if intent.action == "reduce_price":
        if intent.delta_amount is None or intent.delta_amount <= 0:
            return ChatPlanOut(
                status="cannot_plan",
                action="reduce_price",
                question="לא זיהיתי בכמה להוריד את המחיר. נסה לכתוב למשל: ב-50 ש\"ח",
            )
        if target.regular_price is None:
            return ChatPlanOut(
                status="cannot_plan",
                action="reduce_price",
                question=f'למוצר "{target.name}" אין מחיר נוכחי במערכת.',
            )
        new_price = max(0.0, float(target.regular_price) - float(intent.delta_amount))
        question = _build_confirmation_for_price(target.name, float(target.regular_price), new_price)
        payload = {
            "action": "reduce_price",
            "product_id": target.id,
            "delta_amount": float(intent.delta_amount),
            "to_price": float(new_price),
        }
        return ChatPlanOut(
            status="needs_confirmation",
            action="reduce_price",
            question=question,
            product_id=target.id,
            product_name=target.name,
            delta_amount=float(intent.delta_amount),
            from_price=float(target.regular_price),
            to_price=float(new_price),
            currency=shop.woo_currency or "ILS",
            confirm_payload=payload,
        )

    payload = {
        "action": "out_of_stock",
        "product_id": target.id,
    }
    return ChatPlanOut(
        status="needs_confirmation",
        action="out_of_stock",
        question=_build_confirmation_for_stock(target.name),
        product_id=target.id,
        product_name=target.name,
        confirm_payload=payload,
    )


@router.post("/{shop_id}/ai/chat/confirm", response_model=ChatConfirmOut)
def confirm_chat_action(
    shop_id: int,
    body: ChatConfirmIn,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> ChatConfirmOut:
    _ensure_chat_enabled()
    shop = require_shop_access(session, user, shop_id)
    payload = body.payload or {}
    action = str(payload.get("action") or "")
    product_id_raw = payload.get("product_id")
    try:
        product_id = int(product_id_raw)
    except (TypeError, ValueError):
        raise HTTPException(400, "payload לא תקין: product_id חסר") from None

    p = session.get(Product, product_id)
    if not p or p.shop_id != shop_id:
        raise HTTPException(404, "מוצר לא נמצא")
    if not body.approved:
        return ChatConfirmOut(status="cancelled", action=action, product_id=p.id, product_name=p.name)
    if not p.woo_product_id:
        raise HTTPException(400, "למוצר אין מזהה WooCommerce ולכן אי אפשר לבצע פעולה זו.")
    if not shop.woo_site_url or not shop.woo_consumer_key or not shop.woo_consumer_secret:
        raise HTTPException(400, "יש לשמור פרטי WooCommerce בהגדרות")

    if action == "reduce_price":
        to_price_raw = payload.get("to_price")
        try:
            to_price = float(to_price_raw)
        except (TypeError, ValueError):
            raise HTTPException(400, "payload לא תקין: to_price חסר") from None
        before = {"regular_price": p.regular_price}
        patch_wc_product_regular_price(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(p.woo_product_id),
            to_price,
        )
        p.regular_price = to_price
        session.add(p)
        session.commit()
        session.refresh(p)
        return ChatConfirmOut(
            status="executed",
            action="reduce_price",
            product_id=p.id,
            product_name=p.name,
            before=before,
            after={"regular_price": p.regular_price},
        )

    if action == "out_of_stock":
        row_before = fetch_wc_product_by_id(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(p.woo_product_id),
        )
        before_status = str(row_before.get("stock_status") or "")
        patch_wc_product_out_of_stock(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(p.woo_product_id),
        )
        return ChatConfirmOut(
            status="executed",
            action="out_of_stock",
            product_id=p.id,
            product_name=p.name,
            before={"stock_status": before_status},
            after={"stock_status": "outofstock"},
        )

    raise HTTPException(400, "פעולה לא נתמכת")

