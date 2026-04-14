from __future__ import annotations

import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlmodel import Session, select

from backend.config import settings
from backend.db import get_session
from backend.deps import get_current_user, require_shop_access
from backend.models import Product, Shop, ShopAiActionLog, ShopWhatsappConfig, ShopWhatsappPendingAction, User, utcnow
from backend.services.ai_ops import parse_intent_with_openai, rank_product_candidates
from backend.services.woo_sync import (
    effective_wc_price,
    force_wc_product_sale_price_via_meta,
    force_wc_variation_sale_price_via_meta,
    fetch_wc_product_by_id,
    fetch_wc_product_variations,
    fetch_wc_product_with_retries,
    parse_price,
    patch_wc_product_in_stock,
    patch_wc_product_out_of_stock,
    patch_wc_product_prices,
    patch_wc_product_regular_price,
    patch_wc_product_sale_price,
    patch_wc_variation_prices,
)
from backend.services.whatsapp_cloud import (
    MetaAuthError,
    send_interactive_confirm_buttons,
    send_test_text_message,
    validate_phone_number_id,
)
from backend.services.sales_notifications import handle_woo_sale_event

router = APIRouter(prefix="/api/shops", tags=["ai-ops"])
log = logging.getLogger(__name__)


def _as_utc_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class ChatPlanIn(BaseModel):
    message: str


class ChatCandidateOut(BaseModel):
    product_id: int
    name: str
    score: float
    current_price: float | None


class ChatPlanOut(BaseModel):
    status: Literal["needs_confirmation", "needs_disambiguation", "cannot_plan"]
    action: Literal["reduce_price", "increase_price", "out_of_stock", "in_stock", "bulk_reduce_price", "unknown"]
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
    action_log_id: int | None = None


def _ensure_chat_enabled() -> None:
    if not settings.ai_chat_enabled:
        raise HTTPException(404, "פיצ'ר צ'אט AI כבוי כרגע")


def _ensure_woo_connected(shop) -> None:
    if not shop.woo_site_url or not shop.woo_consumer_key or not shop.woo_consumer_secret:
        raise HTTPException(400, "יש לשמור פרטי WooCommerce בהגדרות החנות כדי לבצע פעולה זו.")


def _build_confirmation_for_price(name: str, old_price: float, new_price: float) -> str:
    return f'האם להוריד את המחיר של "{name}" מ{old_price:,.2f} ש"ח ל{new_price:,.2f} ש"ח?'


def _build_confirmation_for_price_increase(name: str, old_price: float, new_price: float) -> str:
    return f'האם להעלות את המחיר של "{name}" מ{old_price:,.2f} ש"ח ל{new_price:,.2f} ש"ח?'


def _build_confirmation_for_stock(name: str) -> str:
    return f'האם להוריד את המוצר "{name}" מהמלאי?'


def _build_confirmation_for_restore_stock(name: str) -> str:
    return f'האם להחזיר את המוצר "{name}" למלאי?'


def _build_confirmation_for_bulk_reduce(count: int, delta: float, scope_label: str) -> str:
    return f'האם להוריד {delta:,.2f} ש"ח ל-{count} מוצרים ({scope_label})?'


def _price_almost_equal(a: float | None, b: float | None, eps: float = 0.011) -> bool:
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= eps


def _log_ai_action(
    session: Session,
    *,
    shop_id: int,
    user_id: int,
    action: str,
    payload: dict[str, Any],
    product_id: int | None = None,
) -> ShopAiActionLog:
    now = _as_utc_aware(utcnow()) or utcnow()
    row = ShopAiActionLog(
        shop_id=shop_id,
        user_id=user_id,
        action=action,
        product_id=product_id,
        payload_json=json.dumps(payload, ensure_ascii=False),
        status="executed",
        created_at=now,
        undo_deadline_at=now + timedelta(minutes=5),
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


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
    if intent.action not in ("reduce_price", "increase_price", "out_of_stock", "in_stock", "bulk_reduce_price"):
        return ChatPlanOut(status="cannot_plan", action="unknown", question="לא זיהיתי פעולה נתמכת בהודעה.")

    products = session.exec(select(Product).where(Product.shop_id == shop.id)).all()
    if not products:
        return ChatPlanOut(status="cannot_plan", action=intent.action, question="לא נמצאו מוצרים בחנות.")
    try:
        _ensure_woo_connected(shop)
    except HTTPException as ex:
        return ChatPlanOut(status="cannot_plan", action=intent.action, question=str(ex.detail))

    if intent.action == "bulk_reduce_price":
        if intent.delta_amount is None or intent.delta_amount <= 0:
            return ChatPlanOut(
                status="cannot_plan",
                action="bulk_reduce_price",
                question='לא זיהיתי בכמה להוריד לכל מוצר. נסה לכתוב למשל: "תוריד 50 ש"ח לכל קטגוריית ...".',
            )
        selected: list[Product] = []
        scope_label = "רשימת מוצרים"
        if intent.bulk_scope == "category" and (intent.target_category or "").strip():
            cat_q = (intent.target_category or "").strip().lower()
            selected = [p for p in products if (p.category_name or "").strip().lower() == cat_q]
            if not selected:
                selected = [p for p in products if cat_q in (p.category_name or "").strip().lower()]
            scope_label = f"קטגוריה: {intent.target_category}"
        else:
            qlist = intent.product_queries or [intent.product_query or msg]
            picked: set[int] = set()
            for q in qlist:
                ranked_q = rank_product_candidates(q, products, top_k=3)
                if not ranked_q:
                    continue
                best = ranked_q[0]
                if best.score < 0.45 or best.product_id in picked:
                    continue
                p = session.get(Product, best.product_id)
                if p and p.shop_id == shop.id:
                    selected.append(p)
                    picked.add(p.id or 0)
        if not selected:
            return ChatPlanOut(
                status="cannot_plan",
                action="bulk_reduce_price",
                question="לא הצלחתי להרכיב רשימת מוצרים תקינה לפעולת bulk.",
            )

        ops: list[dict[str, Any]] = []
        sample_names: list[str] = []
        for p in selected:
            if not p.woo_product_id:
                continue
            wc_row = fetch_wc_product_by_id(
                shop.woo_site_url,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                int(p.woo_product_id),
            )
            sale_now = parse_price(wc_row.get("sale_price"))
            regular_now = parse_price(wc_row.get("regular_price")) if wc_row else None
            effective_now = effective_wc_price(wc_row)
            on_sale_now = bool(wc_row.get("on_sale"))
            price_field = "regular_price"
            from_price_val = regular_now if regular_now is not None else p.regular_price
            if on_sale_now or (sale_now is not None and sale_now > 0):
                price_field = "sale_price"
                from_price_val = effective_now if effective_now is not None else sale_now
            if from_price_val is None:
                continue
            to_price_val = max(0.0, float(from_price_val) - float(intent.delta_amount))
            ops.append(
                {
                    "product_id": p.id,
                    "woo_product_id": int(p.woo_product_id),
                    "product_name": p.name,
                    "price_field": price_field,
                    "from_price": float(from_price_val),
                    "to_price": float(to_price_val),
                },
            )
            if len(sample_names) < 4:
                sample_names.append(p.name)
        if not ops:
            return ChatPlanOut(
                status="cannot_plan",
                action="bulk_reduce_price",
                question="לא נמצאו מוצרים עם מחיר פעיל לעדכון.",
            )

        q = _build_confirmation_for_bulk_reduce(len(ops), float(intent.delta_amount), scope_label)
        if sample_names:
            q = f'{q}\nדוגמאות: {", ".join(sample_names)}'
        return ChatPlanOut(
            status="needs_confirmation",
            action="bulk_reduce_price",
            question=q,
            delta_amount=float(intent.delta_amount),
            confirm_payload={
                "action": "bulk_reduce_price",
                "scope_label": scope_label,
                "delta_amount": float(intent.delta_amount),
                "operations": ops,
            },
            candidates=[
                ChatCandidateOut(
                    product_id=int(op["product_id"]),
                    name=str(op["product_name"]),
                    score=1.0,
                    current_price=float(op["from_price"]),
                )
                for op in ops[:10]
            ],
        )

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
                question='לא זיהיתי בכמה להוריד את המחיר. נסה לכתוב למשל: "ב-50 ש"ח".',
            )
        if not target.woo_product_id:
            return ChatPlanOut(
                status="cannot_plan",
                action="reduce_price",
                question=f'למוצר "{target.name}" אין מזהה WooCommerce תקין.',
            )
        wc_row = fetch_wc_product_by_id(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(target.woo_product_id),
        )
        sale_now = parse_price(wc_row.get("sale_price"))
        regular_now = parse_price(wc_row.get("regular_price")) if wc_row else None
        effective_now = effective_wc_price(wc_row)
        on_sale_now = bool(wc_row.get("on_sale"))
        price_field = "regular_price"
        from_price_val = regular_now if regular_now is not None else target.regular_price
        if on_sale_now or (sale_now is not None and sale_now > 0):
            price_field = "sale_price"
            from_price_val = effective_now if effective_now is not None else sale_now
        if from_price_val is None:
            return ChatPlanOut(
                status="cannot_plan",
                action="reduce_price",
                question=f'למוצר "{target.name}" אין מחיר נוכחי במערכת.',
            )
        new_price = max(0.0, float(from_price_val) - float(intent.delta_amount))
        return ChatPlanOut(
            status="needs_confirmation",
            action="reduce_price",
            question=_build_confirmation_for_price(target.name, float(from_price_val), new_price),
            product_id=target.id,
            product_name=target.name,
            delta_amount=float(intent.delta_amount),
            from_price=float(from_price_val),
            to_price=float(new_price),
            currency=shop.woo_currency or "ILS",
            confirm_payload={
                "action": "reduce_price",
                "product_id": target.id,
                "delta_amount": float(intent.delta_amount),
                "to_price": float(new_price),
                "price_field": price_field,
            },
        )

    if intent.action == "increase_price":
        if intent.delta_amount is None or intent.delta_amount <= 0:
            return ChatPlanOut(
                status="cannot_plan",
                action="increase_price",
                question='לא זיהיתי בכמה להעלות את המחיר. נסה לכתוב למשל: "ב-50 ש"ח".',
            )
        if not target.woo_product_id:
            return ChatPlanOut(
                status="cannot_plan",
                action="increase_price",
                question=f'למוצר "{target.name}" אין מזהה WooCommerce תקין.',
            )
        wc_row = fetch_wc_product_by_id(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(target.woo_product_id),
        )
        sale_now = parse_price(wc_row.get("sale_price"))
        regular_now = parse_price(wc_row.get("regular_price")) if wc_row else None
        effective_now = effective_wc_price(wc_row)
        on_sale_now = bool(wc_row.get("on_sale"))
        price_field = "regular_price"
        from_price_val = regular_now if regular_now is not None else target.regular_price
        if on_sale_now or (sale_now is not None and sale_now > 0):
            price_field = "sale_price"
            from_price_val = effective_now if effective_now is not None else sale_now
        if from_price_val is None:
            return ChatPlanOut(
                status="cannot_plan",
                action="increase_price",
                question=f'למוצר "{target.name}" אין מחיר נוכחי במערכת.',
            )
        new_price = max(0.0, float(from_price_val) + float(intent.delta_amount))
        return ChatPlanOut(
            status="needs_confirmation",
            action="increase_price",
            question=_build_confirmation_for_price_increase(target.name, float(from_price_val), new_price),
            product_id=target.id,
            product_name=target.name,
            delta_amount=float(intent.delta_amount),
            from_price=float(from_price_val),
            to_price=float(new_price),
            currency=shop.woo_currency or "ILS",
            confirm_payload={
                "action": "increase_price",
                "product_id": target.id,
                "delta_amount": float(intent.delta_amount),
                "to_price": float(new_price),
                "price_field": price_field,
            },
        )

    if intent.action == "in_stock":
        return ChatPlanOut(
            status="needs_confirmation",
            action="in_stock",
            question=_build_confirmation_for_restore_stock(target.name),
            product_id=target.id,
            product_name=target.name,
            confirm_payload={"action": "in_stock", "product_id": target.id},
        )

    return ChatPlanOut(
        status="needs_confirmation",
        action="out_of_stock",
        question=_build_confirmation_for_stock(target.name),
        product_id=target.id,
        product_name=target.name,
        confirm_payload={"action": "out_of_stock", "product_id": target.id},
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
    if not body.approved:
        return ChatConfirmOut(status="cancelled", action=action)
    _ensure_woo_connected(shop)

    if action == "bulk_reduce_price":
        operations = payload.get("operations")
        if not isinstance(operations, list) or not operations:
            raise HTTPException(400, "payload לא תקין: operations חסר")
        applied: list[dict[str, Any]] = []
        for op in operations:
            if not isinstance(op, dict):
                continue
            woo_id = int(op.get("woo_product_id"))
            pid = int(op.get("product_id"))
            to_price = float(op.get("to_price"))
            price_field = str(op.get("price_field") or "regular_price")
            pname = str(op.get("product_name") or "")
            before_row = fetch_wc_product_by_id(
                shop.woo_site_url,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                woo_id,
            )
            before = {
                "regular_price": parse_price(before_row.get("regular_price")),
                "sale_price": parse_price(before_row.get("sale_price")),
                "price_field": price_field,
            }
            if price_field == "sale_price":
                patch_wc_product_prices(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    woo_id,
                    regular_price=before.get("regular_price"),
                    sale_price=to_price,
                )
            else:
                patch_wc_product_regular_price(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    woo_id,
                    to_price,
                )
                p_row = session.get(Product, pid)
                if p_row and p_row.shop_id == shop_id:
                    p_row.regular_price = to_price
                    session.add(p_row)
            after_row = fetch_wc_product_by_id(
                shop.woo_site_url,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                woo_id,
            )
            applied.append(
                {
                    "product_id": pid,
                    "woo_product_id": woo_id,
                    "product_name": pname,
                    "price_field": price_field,
                    "before": before,
                    "after": {
                        "regular_price": parse_price(after_row.get("regular_price")),
                        "sale_price": parse_price(after_row.get("sale_price")),
                    },
                },
            )
        session.commit()
        log_row = _log_ai_action(
            session,
            shop_id=shop_id,
            user_id=user.id or 0,
            action="bulk_reduce_price",
            payload={
                "scope_label": payload.get("scope_label"),
                "delta_amount": payload.get("delta_amount"),
                "operations": applied,
            },
        )
        return ChatConfirmOut(
            status="executed",
            action="bulk_reduce_price",
            before={"count": len(applied)},
            after={"count": len(applied), "sample": applied[:3]},
            action_log_id=log_row.id,
        )

    product_id_raw = payload.get("product_id")
    try:
        product_id = int(product_id_raw)
    except (TypeError, ValueError):
        raise HTTPException(400, "payload לא תקין: product_id חסר") from None
    p = session.get(Product, product_id)
    if not p or p.shop_id != shop_id:
        raise HTTPException(404, "מוצר לא נמצא")
    if not p.woo_product_id:
        raise HTTPException(400, "למוצר אין מזהה WooCommerce ולכן אי אפשר לבצע פעולה זו.")

    if action in ("reduce_price", "increase_price"):
        to_price_raw = payload.get("to_price")
        try:
            to_price = float(to_price_raw)
        except (TypeError, ValueError):
            raise HTTPException(400, "payload לא תקין: to_price חסר") from None
        delta_amount = float(payload.get("delta_amount") or 0.0)
        price_field = str(payload.get("price_field") or "regular_price")
        row_before = fetch_wc_product_by_id(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(p.woo_product_id),
        )
        before = {
            "regular_price": parse_price(row_before.get("regular_price")),
            "sale_price": parse_price(row_before.get("sale_price")),
            "price_field": price_field,
        }
        on_sale_before = bool(row_before.get("on_sale"))
        # If Woo reports active sale, updating regular_price won't change visible price.
        # Force execution path to sale_price to avoid false "executed" responses.
        sale_before = before.get("sale_price")
        has_sale_price_before = sale_before is not None and float(sale_before) > 0
        if price_field == "regular_price" and (on_sale_before or has_sale_price_before):
            price_field = "sale_price"
            before["price_field"] = "sale_price"
        if price_field == "sale_price":
            if action == "increase_price":
                regular_before = before.get("regular_price")
                sale_before = before.get("sale_price")
                # Woo may refuse/ignore sale updates when regular is missing or <= sale.
                # Keep a small spread so sale remains a valid "discount" price.
                spread = 10.0
                if regular_before is not None and sale_before is not None:
                    spread = max(float(regular_before) - float(sale_before), 10.0)
                desired_regular = float(to_price + spread)
                if regular_before is None or to_price >= float(regular_before):
                    patch_wc_product_prices(
                        shop.woo_site_url,
                        shop.woo_consumer_key,
                        shop.woo_consumer_secret,
                        int(p.woo_product_id),
                        regular_price=desired_regular,
                        sale_price=to_price,
                        clear_sale_schedule=True,
                    )
                else:
                    patch_wc_product_prices(
                        shop.woo_site_url,
                        shop.woo_consumer_key,
                        shop.woo_consumer_secret,
                        int(p.woo_product_id),
                        regular_price=float(regular_before),
                        sale_price=to_price,
                        clear_sale_schedule=True,
                    )
            else:
                patch_wc_product_sale_price(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    int(p.woo_product_id),
                    to_price,
                )
        else:
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
        row_after = fetch_wc_product_with_retries(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(p.woo_product_id),
        )
        after_regular = parse_price(row_after.get("regular_price"))
        after_sale = parse_price(row_after.get("sale_price"))
        observed = after_sale if price_field == "sale_price" else after_regular
        observed_effective = effective_wc_price(row_after)
        if price_field == "sale_price" and action == "increase_price" and not _price_almost_equal(observed, to_price):
            # Last-resort retry: force regular above target, then retry sale update once.
            retry_regular = float(to_price + 10.0)
            patch_wc_product_prices(
                shop.woo_site_url,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                int(p.woo_product_id),
                regular_price=retry_regular,
                sale_price=to_price,
                clear_sale_schedule=True,
            )
            row_after = fetch_wc_product_with_retries(
                shop.woo_site_url,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                int(p.woo_product_id),
            )
            after_regular = parse_price(row_after.get("regular_price"))
            after_sale = parse_price(row_after.get("sale_price"))
            observed = after_sale
            observed_effective = effective_wc_price(row_after)
        if price_field == "sale_price" and action == "increase_price" and not _price_almost_equal(observed, to_price):
            # Some shops derive displayed parent price from variations only.
            product_type = str(row_after.get("type") or row_before.get("type") or "")
            if product_type == "variable":
                vars_rows = fetch_wc_product_variations(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    int(p.woo_product_id),
                )
                for v in vars_rows:
                    vid = v.get("id")
                    if vid is None:
                        continue
                    v_regular = parse_price(v.get("regular_price"))
                    v_sale = parse_price(v.get("sale_price"))
                    spread = 10.0
                    if v_regular is not None and v_sale is not None:
                        spread = max(float(v_regular) - float(v_sale), 10.0)
                    target_regular = float(to_price + spread)
                    patch_wc_variation_prices(
                        shop.woo_site_url,
                        shop.woo_consumer_key,
                        shop.woo_consumer_secret,
                        int(p.woo_product_id),
                        int(vid),
                        regular_price=target_regular,
                        sale_price=to_price,
                        clear_sale_schedule=True,
                    )
                row_after = fetch_wc_product_with_retries(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    int(p.woo_product_id),
                    retries=4,
                    delay_seconds=0.6,
                )
                after_regular = parse_price(row_after.get("regular_price"))
                after_sale = parse_price(row_after.get("sale_price"))
                observed = after_sale
                observed_effective = effective_wc_price(row_after)
        if price_field == "sale_price" and not _price_almost_equal(observed, to_price):
            # Fallback: keep sale mode and enforce sale price.
            product_type = str(row_after.get("type") or row_before.get("type") or "")
            if product_type == "variable":
                vars_rows = fetch_wc_product_variations(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    int(p.woo_product_id),
                )
                for v in vars_rows:
                    vid = v.get("id")
                    if vid is None:
                        continue
                    v_regular = parse_price(v.get("regular_price"))
                    regular_hint = v_regular
                    if regular_hint is None or regular_hint <= float(to_price):
                        regular_hint = float(to_price + 10.0)
                    force_wc_variation_sale_price_via_meta(
                        shop.woo_site_url,
                        shop.woo_consumer_key,
                        shop.woo_consumer_secret,
                        int(p.woo_product_id),
                        int(vid),
                        float(to_price),
                        regular_price_hint=float(regular_hint),
                    )
            else:
                regular_hint = parse_price(row_after.get("regular_price")) or parse_price(row_before.get("regular_price"))
                if regular_hint is None or regular_hint <= float(to_price):
                    regular_hint = float(to_price + 10.0)
                force_wc_product_sale_price_via_meta(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    int(p.woo_product_id),
                    float(to_price),
                    regular_price_hint=float(regular_hint),
                )
            row_after = fetch_wc_product_with_retries(
                shop.woo_site_url,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                int(p.woo_product_id),
                retries=10,
                delay_seconds=1.0,
            )
            after_regular = parse_price(row_after.get("regular_price"))
            after_sale = parse_price(row_after.get("sale_price"))
            observed = after_sale if after_sale is not None else after_regular
            observed_effective = effective_wc_price(row_after)
        if price_field == "sale_price" and not _price_almost_equal(observed, to_price) and not _price_almost_equal(observed_effective, to_price):
            # Plugin-resistant fallback: write core Woo price meta keys too.
            product_type = str(row_after.get("type") or row_before.get("type") or "")
            if product_type == "variable":
                vars_rows = fetch_wc_product_variations(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    int(p.woo_product_id),
                )
                for v in vars_rows:
                    vid = v.get("id")
                    if vid is None:
                        continue
                    v_regular = parse_price(v.get("regular_price"))
                    regular_hint = v_regular
                    if regular_hint is None or regular_hint <= float(to_price):
                        regular_hint = float(to_price + 10.0)
                    force_wc_variation_sale_price_via_meta(
                        shop.woo_site_url,
                        shop.woo_consumer_key,
                        shop.woo_consumer_secret,
                        int(p.woo_product_id),
                        int(vid),
                        float(to_price),
                        regular_price_hint=float(regular_hint),
                    )
            else:
                regular_hint = parse_price(row_after.get("regular_price")) or parse_price(row_before.get("regular_price"))
                if regular_hint is None or regular_hint <= float(to_price):
                    regular_hint = float(to_price + 10.0)
                force_wc_product_sale_price_via_meta(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    int(p.woo_product_id),
                    float(to_price),
                    regular_price_hint=float(regular_hint),
                )
            row_after = fetch_wc_product_with_retries(
                shop.woo_site_url,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                int(p.woo_product_id),
                retries=10,
                delay_seconds=1.0,
            )
            after_regular = parse_price(row_after.get("regular_price"))
            after_sale = parse_price(row_after.get("sale_price"))
            observed = after_sale if after_sale is not None else after_regular
            observed_effective = effective_wc_price(row_after)
        if not _price_almost_equal(observed, to_price) and not _price_almost_equal(observed_effective, to_price):
            raise HTTPException(
                409,
                (
                    "העדכון נשלח ל-WooCommerce אבל המחיר בפועל לא השתנה לערך המבוקש. "
                    f"field={price_field} target={to_price:.2f} observed={observed!s} delta={delta_amount:.2f} "
                    f"observed_effective={observed_effective!s} "
                    f"type={row_after.get('type')!s} on_sale={row_after.get('on_sale')!s}"
                ),
            )
        log_row = _log_ai_action(
            session,
            shop_id=shop_id,
            user_id=user.id or 0,
            action=action,
            product_id=p.id,
            payload={
                "price_field": price_field,
                "product_id": p.id,
                "woo_product_id": int(p.woo_product_id),
                "before": before,
                "after": {
                    "regular_price": after_regular,
                    "sale_price": after_sale,
                },
            },
        )
        return ChatConfirmOut(
            status="executed",
            action=action,
            product_id=p.id,
            product_name=p.name,
            before=before,
            after={
                "regular_price": after_regular,
                "sale_price": after_sale,
                "price_field": price_field,
            },
            action_log_id=log_row.id,
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
        log_row = _log_ai_action(
            session,
            shop_id=shop_id,
            user_id=user.id or 0,
            action="out_of_stock",
            product_id=p.id,
            payload={
                "product_id": p.id,
                "woo_product_id": int(p.woo_product_id),
                "before": {"stock_status": before_status},
                "after": {"stock_status": "outofstock"},
            },
        )
        return ChatConfirmOut(
            status="executed",
            action="out_of_stock",
            product_id=p.id,
            product_name=p.name,
            before={"stock_status": before_status},
            after={"stock_status": "outofstock"},
            action_log_id=log_row.id,
        )

    if action == "in_stock":
        row_before = fetch_wc_product_by_id(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(p.woo_product_id),
        )
        before_status = str(row_before.get("stock_status") or "")
        patch_wc_product_in_stock(
            shop.woo_site_url,
            shop.woo_consumer_key,
            shop.woo_consumer_secret,
            int(p.woo_product_id),
        )
        log_row = _log_ai_action(
            session,
            shop_id=shop_id,
            user_id=user.id or 0,
            action="in_stock",
            product_id=p.id,
            payload={
                "product_id": p.id,
                "woo_product_id": int(p.woo_product_id),
                "before": {"stock_status": before_status},
                "after": {"stock_status": "instock"},
            },
        )
        return ChatConfirmOut(
            status="executed",
            action="in_stock",
            product_id=p.id,
            product_name=p.name,
            before={"stock_status": before_status},
            after={"stock_status": "instock"},
            action_log_id=log_row.id,
        )

    raise HTTPException(400, "פעולה לא נתמכת")


class AiActionLogOut(BaseModel):
    id: int
    action: str
    status: str
    product_id: int | None
    created_at: str
    undo_deadline_at: str | None
    undone_at: str | None
    payload: dict[str, Any]


@router.get("/{shop_id}/ai/actions", response_model=list[AiActionLogOut])
def list_ai_actions(
    shop_id: int,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
    limit: int = Query(50, ge=1, le=200),
) -> list[AiActionLogOut]:
    require_shop_access(session, user, shop_id)
    rows = session.exec(
        select(ShopAiActionLog).where(ShopAiActionLog.shop_id == shop_id).order_by(ShopAiActionLog.id.desc()).limit(limit),
    ).all()
    out: list[AiActionLogOut] = []
    for r in rows:
        try:
            payload = json.loads(r.payload_json or "{}")
        except Exception:
            payload = {}
        out.append(
            AiActionLogOut(
                id=r.id or 0,
                action=r.action,
                status=r.status,
                product_id=r.product_id,
                created_at=r.created_at.isoformat(),
                undo_deadline_at=r.undo_deadline_at.isoformat() if r.undo_deadline_at else None,
                undone_at=r.undone_at.isoformat() if r.undone_at else None,
                payload=payload,
            ),
        )
    return out


class UndoOut(BaseModel):
    ok: bool
    action_id: int
    status: str
    detail: str


@router.post("/{shop_id}/ai/actions/{action_id}/undo", response_model=UndoOut)
def undo_ai_action(
    shop_id: int,
    action_id: int,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> UndoOut:
    shop = require_shop_access(session, user, shop_id)
    row = session.get(ShopAiActionLog, action_id)
    if not row or row.shop_id != shop_id:
        raise HTTPException(404, "פעולה לא נמצאה")
    if row.status != "executed":
        raise HTTPException(400, "הפעולה לא ניתנת לביטול")
    deadline = _as_utc_aware(row.undo_deadline_at)
    if deadline is None or _as_utc_aware(utcnow()) > deadline:
        row.status = "undo_expired"
        row.undo_note = "window expired"
        session.add(row)
        session.commit()
        return UndoOut(ok=False, action_id=action_id, status=row.status, detail="חלון הזמן לביטול פג")
    _ensure_woo_connected(shop)
    try:
        payload = json.loads(row.payload_json or "{}")
    except Exception:
        payload = {}
    try:
        if row.action in ("reduce_price", "increase_price"):
            woo_id = int(payload.get("woo_product_id"))
            price_field = str(payload.get("price_field") or "regular_price")
            before = payload.get("before") or {}
            old_val = before.get("sale_price" if price_field == "sale_price" else "regular_price")
            if old_val is None:
                raise ValueError("missing previous price")
            if price_field == "sale_price":
                patch_wc_product_sale_price(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    woo_id,
                    float(old_val),
                )
            else:
                patch_wc_product_regular_price(
                    shop.woo_site_url,
                    shop.woo_consumer_key,
                    shop.woo_consumer_secret,
                    woo_id,
                    float(old_val),
                )
                pid = payload.get("product_id")
                p = session.get(Product, int(pid)) if pid is not None else None
                if p and p.shop_id == shop_id:
                    p.regular_price = float(old_val)
                    session.add(p)
        elif row.action == "out_of_stock":
            patch_wc_product_in_stock(
                shop.woo_site_url,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                int(payload.get("woo_product_id")),
            )
        elif row.action == "in_stock":
            patch_wc_product_out_of_stock(
                shop.woo_site_url,
                shop.woo_consumer_key,
                shop.woo_consumer_secret,
                int(payload.get("woo_product_id")),
            )
        elif row.action == "bulk_reduce_price":
            ops = payload.get("operations")
            if not isinstance(ops, list):
                raise ValueError("missing bulk operations")
            for op in ops:
                if not isinstance(op, dict):
                    continue
                woo_id = int(op.get("woo_product_id"))
                price_field = str(op.get("price_field") or "regular_price")
                before = op.get("before") or {}
                old_val = before.get("sale_price" if price_field == "sale_price" else "regular_price")
                if old_val is None:
                    continue
                if price_field == "sale_price":
                    patch_wc_product_sale_price(
                        shop.woo_site_url,
                        shop.woo_consumer_key,
                        shop.woo_consumer_secret,
                        woo_id,
                        float(old_val),
                    )
                else:
                    patch_wc_product_regular_price(
                        shop.woo_site_url,
                        shop.woo_consumer_key,
                        shop.woo_consumer_secret,
                        woo_id,
                        float(old_val),
                    )
                    pid = op.get("product_id")
                    p = session.get(Product, int(pid)) if pid is not None else None
                    if p and p.shop_id == shop_id:
                        p.regular_price = float(old_val)
                        session.add(p)
        else:
            raise ValueError("unsupported action for undo")
        row.status = "undone"
        row.undone_at = utcnow()
        row.undone_by_user_id = user.id
        row.undo_note = "ok"
        session.add(row)
        session.commit()
        return UndoOut(ok=True, action_id=action_id, status=row.status, detail="הביטול בוצע בהצלחה")
    except Exception as ex:
        row.status = "undo_failed"
        row.undo_note = str(ex)
        session.add(row)
        session.commit()
        return UndoOut(ok=False, action_id=action_id, status=row.status, detail=f"ביטול נכשל: {ex!s}")


class WhatsappConfigIn(BaseModel):
    enabled: bool
    phone_number_id: str
    business_account_id: str | None = None
    verify_token: str
    access_token: str
    alert_phone_e164: str | None = None


class WhatsappConfigOut(BaseModel):
    enabled: bool
    phone_number_id: str | None
    business_account_id: str | None
    verify_token: str | None
    access_token_masked: str | None
    alert_phone_e164: str | None
    webhook_url: str | None
    webhook_verify_url: str | None
    sales_webhook_url: str | None
    updated_at: str | None


def _mask_token(tok: str | None) -> str | None:
    t = (tok or "").strip()
    if not t:
        return None
    if len(t) <= 8:
        return "*" * len(t)
    return f"{t[:4]}...{t[-4:]}"


def _public_api_base() -> str:
    return (settings.public_api_base or "").strip().rstrip("/")


def _webhook_urls(cfg: ShopWhatsappConfig | None) -> tuple[str | None, str | None, str | None]:
    if cfg is None or not cfg.webhook_path_secret:
        return None, None, None
    base = _public_api_base()
    if not base:
        return None, None, None
    path = f"/api/shops/ai/whatsapp/webhook/{cfg.webhook_path_secret}"
    sales_secret = (cfg.sales_webhook_secret or cfg.webhook_path_secret or "").strip()
    sales_url = f"{base}/api/shops/ai/whatsapp/sales-webhook/{sales_secret}" if sales_secret else None
    return (
        f"{base}{path}",
        f"{base}{path}?hub.mode=subscribe&hub.verify_token=YOUR_VERIFY_TOKEN&hub.challenge=1234",
        sales_url,
    )


@router.get("/{shop_id}/ai/whatsapp/config", response_model=WhatsappConfigOut)
def get_whatsapp_config(
    shop_id: int,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> WhatsappConfigOut:
    require_shop_access(session, user, shop_id)
    cfg = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.shop_id == shop_id)).first()
    wh, wh_verify, sales_wh = _webhook_urls(cfg)
    return WhatsappConfigOut(
        enabled=bool(cfg.enabled) if cfg else False,
        phone_number_id=cfg.phone_number_id if cfg else None,
        business_account_id=cfg.business_account_id if cfg else None,
        verify_token=cfg.verify_token if cfg else None,
        access_token_masked=_mask_token(cfg.access_token if cfg else None),
        alert_phone_e164=cfg.alert_phone_e164 if cfg else None,
        webhook_url=wh,
        webhook_verify_url=wh_verify,
        sales_webhook_url=sales_wh,
        updated_at=cfg.updated_at.isoformat() if cfg and cfg.updated_at else None,
    )


@router.put("/{shop_id}/ai/whatsapp/config", response_model=WhatsappConfigOut)
def upsert_whatsapp_config(
    shop_id: int,
    body: WhatsappConfigIn,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> WhatsappConfigOut:
    require_shop_access(session, user, shop_id)
    cfg = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.shop_id == shop_id)).first()
    if cfg is None:
        cfg = ShopWhatsappConfig(
            shop_id=shop_id,
            created_by_user_id=user.id,
            webhook_path_secret=secrets.token_urlsafe(24),
        )
    cfg.enabled = bool(body.enabled)
    cfg.phone_number_id = (body.phone_number_id or "").strip() or None
    cfg.business_account_id = (body.business_account_id or "").strip() or None
    cfg.verify_token = (body.verify_token or "").strip() or None
    cfg.access_token = (body.access_token or "").strip() or None
    cfg.alert_phone_e164 = (body.alert_phone_e164 or "").strip() or None
    if not (cfg.sales_webhook_secret or "").strip():
        cfg.sales_webhook_secret = secrets.token_urlsafe(24)
    cfg.updated_by_user_id = user.id
    cfg.updated_at = utcnow()
    session.add(cfg)
    session.commit()
    session.refresh(cfg)
    wh, wh_verify, sales_wh = _webhook_urls(cfg)
    return WhatsappConfigOut(
        enabled=cfg.enabled,
        phone_number_id=cfg.phone_number_id,
        business_account_id=cfg.business_account_id,
        verify_token=cfg.verify_token,
        access_token_masked=_mask_token(cfg.access_token),
        alert_phone_e164=cfg.alert_phone_e164,
        webhook_url=wh,
        webhook_verify_url=wh_verify,
        sales_webhook_url=sales_wh,
        updated_at=cfg.updated_at.isoformat() if cfg.updated_at else None,
    )


class WhatsappGuideOut(BaseModel):
    title: str
    steps: list[str]
    webhook_url: str | None
    verify_token: str | None
    notes: list[str]


class WhatsappValidationOut(BaseModel):
    ok: bool
    detail: str
    phone_info: dict[str, Any] | None = None


class WhatsappSendTestIn(BaseModel):
    to_phone_e164: str
    text: str | None = None


class WhatsappSendTestOut(BaseModel):
    ok: bool
    detail: str
    meta_response: dict[str, Any] | None = None


class WizardStepOut(BaseModel):
    key: str
    title: str
    done: bool
    help_text: str


class WhatsappWizardOut(BaseModel):
    completed: bool
    current_step_key: str
    steps: list[WizardStepOut]
    webhook_url: str | None
    sales_webhook_url: str | None
    verify_token: str | None
    blocking_issues: list[str] = []


@router.get("/{shop_id}/ai/whatsapp/guide", response_model=WhatsappGuideOut)
def whatsapp_guide(
    shop_id: int,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> WhatsappGuideOut:
    require_shop_access(session, user, shop_id)
    cfg = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.shop_id == shop_id)).first()
    wh, _, sales_wh = _webhook_urls(cfg)
    verify = cfg.verify_token if cfg else None
    return WhatsappGuideOut(
        title="חיבור בוט החנות ל-WhatsApp Cloud API",
        steps=[
            "1) פתחו Meta Business והקימו WhatsApp Business Account.",
            "2) הוסיפו מספר טלפון וקבלו Phone Number ID ו-Business Account ID.",
            "3) במסך ההגדרות הזינו Phone Number ID, Verify Token, Access Token ולחצו שמירה.",
            "4) העתיקו את Webhook URL שמופיע כאן ל-Meta Developers.",
            "5) הגדירו Verify Token זהה בדיוק, ובצעו Verify and Save.",
            "6) הירשמו ל-webhook fields: messages, message_status.",
            "7) שלחו הודעת בדיקה במספר WhatsApp והפעילו enabled=true.",
        ],
        webhook_url=wh,
        verify_token=verify,
        notes=[
            "לכל חנות חיבור WhatsApp נפרד (credentials נפרדים).",
            "הבוט עובד במודל plan/confirm לפני ביצוע פעולות.",
            "מומלץ לבצע Rotation תקופתי ל-access token.",
        ],
    )


@router.post("/{shop_id}/ai/whatsapp/validate-credentials", response_model=WhatsappValidationOut)
def whatsapp_validate_credentials(
    shop_id: int,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> WhatsappValidationOut:
    require_shop_access(session, user, shop_id)
    cfg = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.shop_id == shop_id)).first()
    if not cfg:
        return WhatsappValidationOut(ok=False, detail="חסר קונפיג WhatsApp. שמור קודם הגדרות.")
    if not cfg.phone_number_id or not cfg.access_token:
        return WhatsappValidationOut(ok=False, detail="חסרים Phone Number ID או Access Token.")
    try:
        info = validate_phone_number_id(cfg.access_token, cfg.phone_number_id)
        return WhatsappValidationOut(ok=True, detail="הקרדנצ'לים תקינים מול Meta.", phone_info=info)
    except MetaAuthError:
        return WhatsappValidationOut(
            ok=False,
            detail="ה-Access Token של WhatsApp פג תוקף או לא תקין. יש לייצר token חדש ולעדכן בהגדרות.",
        )
    except Exception as ex:
        return WhatsappValidationOut(ok=False, detail=f"אימות נכשל: {ex!s}")


@router.post("/{shop_id}/ai/whatsapp/send-test", response_model=WhatsappSendTestOut)
def whatsapp_send_test(
    shop_id: int,
    body: WhatsappSendTestIn,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> WhatsappSendTestOut:
    require_shop_access(session, user, shop_id)
    cfg = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.shop_id == shop_id)).first()
    if not cfg:
        return WhatsappSendTestOut(ok=False, detail="חסר קונפיג WhatsApp. שמור קודם הגדרות.")
    if not cfg.phone_number_id or not cfg.access_token:
        return WhatsappSendTestOut(ok=False, detail="חסרים Phone Number ID או Access Token.")
    txt = (body.text or "בדיקת חיבור מהעוזר האוטומטי - ההתחברות הצליחה").strip()
    try:
        res = send_test_text_message(cfg.access_token, cfg.phone_number_id, body.to_phone_e164, txt)
        return WhatsappSendTestOut(ok=True, detail="הודעת בדיקה נשלחה בהצלחה.", meta_response=res)
    except MetaAuthError:
        return WhatsappSendTestOut(
            ok=False,
            detail="שליחת בדיקה נכשלה: ה-Access Token פג תוקף או לא תקין. עדכן token חדש בהגדרות.",
        )
    except Exception as ex:
        return WhatsappSendTestOut(ok=False, detail=f"שליחת בדיקה נכשלה: {ex!s}")


@router.get("/{shop_id}/ai/whatsapp/wizard", response_model=WhatsappWizardOut)
def whatsapp_wizard(
    shop_id: int,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> WhatsappWizardOut:
    require_shop_access(session, user, shop_id)
    cfg = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.shop_id == shop_id)).first()
    wh, _, sales_wh = _webhook_urls(cfg)
    has_cfg = cfg is not None
    has_ids = has_cfg and bool((cfg.phone_number_id or "").strip()) and bool((cfg.verify_token or "").strip())
    has_token = has_cfg and bool((cfg.access_token or "").strip())

    creds_ok = False
    if has_ids and has_token and cfg:
        try:
            validate_phone_number_id(cfg.access_token or "", cfg.phone_number_id or "")
            creds_ok = True
        except Exception:
            creds_ok = False
    enabled = bool(cfg.enabled) if cfg else False

    blocking_issues: list[str] = []
    if not _public_api_base():
        blocking_issues.append("הגדרת PUBLIC_API_BASE חסרה בשרת, ולכן אי אפשר להפיק Webhook URL ציבורי.")
    if has_cfg and not has_ids:
        blocking_issues.append("חסרים Phone Number ID או Verify Token בהגדרות החנות.")
    if has_cfg and not has_token:
        blocking_issues.append("חסר Access Token בהגדרות החנות.")

    steps = [
        WizardStepOut(
            key="save_config",
            title="שמור פרטי WhatsApp",
            done=bool(has_ids and has_token),
            help_text="הזן Phone Number ID, Verify Token ו-Access Token ולחץ שמירה.",
        ),
        WizardStepOut(
            key="verify_credentials",
            title="בדוק קרדנצ'לים מול Meta",
            done=creds_ok,
            help_text='לחץ "בדוק חיבור מול Meta". אם נכשל, עדכן token/phone id.',
        ),
        WizardStepOut(
            key="set_webhook",
            title="הגדר Webhook ב-Meta",
            done=bool(creds_ok and wh),
            help_text="העתק את כתובת ה-Webhook למסך WhatsApp App ב-Meta ובצע Verify and Save.",
        ),
        WizardStepOut(
            key="enable_bot",
            title="הפעל את הבוט לחנות",
            done=enabled,
            help_text='הפעל "enabled" לאחר שכל השלבים הושלמו.',
        ),
    ]
    current = "done"
    for s in steps:
        if not s.done:
            current = s.key
            break
    return WhatsappWizardOut(
        completed=all(s.done for s in steps),
        current_step_key=current,
        steps=steps,
        webhook_url=wh,
        sales_webhook_url=sales_wh,
        verify_token=cfg.verify_token if cfg else None,
        blocking_issues=blocking_issues,
    )


@router.get("/ai/whatsapp/webhook/{secret}")
def whatsapp_webhook_verify(
    secret: str,
    request: Request,
    session: Annotated[Session, Depends(get_session)],
):
    cfg = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.webhook_path_secret == secret)).first()
    if not cfg:
        raise HTTPException(404, "Webhook not found")
    mode = request.query_params.get("hub.mode")
    verify_token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    if mode == "subscribe" and verify_token and verify_token == (cfg.verify_token or "") and challenge is not None:
        return int(challenge) if challenge.isdigit() else challenge
    raise HTTPException(403, "Webhook verification failed")


@router.post("/ai/whatsapp/webhook/{secret}")
async def whatsapp_webhook_receive(
    secret: str,
    request: Request,
    session: Annotated[Session, Depends(get_session)],
):
    cfg = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.webhook_path_secret == secret)).first()
    if not cfg:
        raise HTTPException(404, "Webhook not found")
    payload = await request.json()
    if not cfg.enabled:
        return {"ok": True, "shop_id": cfg.shop_id, "enabled": cfg.enabled, "skipped": "disabled"}

    shop = session.get(Shop, cfg.shop_id)
    if not shop:
        return {"ok": True, "shop_id": cfg.shop_id, "enabled": cfg.enabled, "skipped": "shop_not_found"}
    actor_user = session.get(User, shop.owner_id)
    if not actor_user:
        return {"ok": True, "shop_id": cfg.shop_id, "enabled": cfg.enabled, "skipped": "owner_not_found"}

    messages = _extract_incoming_whatsapp_messages(payload)
    handled = 0
    for m in messages:
        sender = (m.get("from") or "").strip()
        text = (m.get("text") or "").strip()
        if not sender or not text:
            continue
        try:
            await _process_whatsapp_text_message(
                session=session,
                cfg=cfg,
                shop=shop,
                actor_user=actor_user,
                sender_phone=sender,
                text=text,
            )
            handled += 1
        except MetaAuthError as ex:
            log.error(
                "whatsapp auth failed; disabling config shop_id=%s sender=%s err=%s",
                cfg.shop_id,
                sender,
                ex,
            )
            cfg.enabled = False
            cfg.updated_at = utcnow()
            session.add(cfg)
            session.commit()
            break
        except Exception as ex:
            log.exception("whatsapp webhook message processing failed shop_id=%s sender=%s", cfg.shop_id, sender)
            _send_whatsapp_reply(cfg, sender, f"שגיאה בביצוע הפעולה: {ex!s}")
    return {"ok": True, "shop_id": cfg.shop_id, "enabled": cfg.enabled, "handled_messages": handled}


@router.post("/ai/whatsapp/sales-webhook/{secret}")
async def whatsapp_sales_webhook_receive(
    secret: str,
    request: Request,
    session: Annotated[Session, Depends(get_session)],
):
    cfg = session.exec(select(ShopWhatsappConfig).where(ShopWhatsappConfig.sales_webhook_secret == secret)).first()
    if not cfg:
        raise HTTPException(404, "Webhook not found")
    payload = await request.json()
    ok = False
    try:
        if isinstance(payload, dict):
            # Woo usually posts single order payload.
            ok = handle_woo_sale_event(session, cfg, payload)
    except Exception:
        log.exception("whatsapp sales webhook processing failed shop_id=%s", cfg.shop_id)
    return {"ok": True, "processed": ok}


def _extract_incoming_whatsapp_messages(payload: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not isinstance(payload, dict):
        return out
    entries = payload.get("entry")
    if not isinstance(entries, list):
        return out
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        changes = entry.get("changes")
        if not isinstance(changes, list):
            continue
        for ch in changes:
            if not isinstance(ch, dict):
                continue
            value = ch.get("value")
            if not isinstance(value, dict):
                continue
            msgs = value.get("messages")
            if not isinstance(msgs, list):
                continue
            for msg in msgs:
                if not isinstance(msg, dict):
                    continue
                sender = str(msg.get("from") or "").strip()
                msg_type = str(msg.get("type") or "")
                text_val = ""
                if msg_type == "text":
                    t = msg.get("text")
                    if isinstance(t, dict):
                        text_val = str(t.get("body") or "").strip()
                elif msg_type == "interactive":
                    inter = msg.get("interactive")
                    if isinstance(inter, dict):
                        i_type = str(inter.get("type") or "")
                        if i_type == "button_reply":
                            br = inter.get("button_reply")
                            if isinstance(br, dict):
                                text_val = str(br.get("title") or br.get("id") or "").strip()
                        elif i_type == "list_reply":
                            lr = inter.get("list_reply")
                            if isinstance(lr, dict):
                                text_val = str(lr.get("title") or lr.get("id") or "").strip()
                if sender and text_val:
                    out.append({"from": sender, "text": text_val})
    return out


def _is_whatsapp_yes(text: str) -> bool:
    t = (text or "").strip().lower()
    t = t.replace(",", " ").replace(".", " ")
    return (
        t in {"כן", "אשר", "אישור", "בצע", "תבצע", "yes", "ok", "okay", "confirm_yes"}
        or ("כן" in t and ("בצע" in t or "אשר" in t))
    )


def _is_whatsapp_no(text: str) -> bool:
    t = (text or "").strip().lower()
    t = t.replace(",", " ").replace(".", " ")
    return t in {"לא", "בטל", "ביטול", "no", "cancel", "confirm_no"} or ("לא" in t and "בטל" in t)


async def _process_whatsapp_text_message(
    *,
    session: Session,
    cfg: ShopWhatsappConfig,
    shop: Shop,
    actor_user: User,
    sender_phone: str,
    text: str,
) -> None:
    now = utcnow()
    pending = session.exec(
        select(ShopWhatsappPendingAction).where(
            ShopWhatsappPendingAction.shop_id == (shop.id or 0),
            ShopWhatsappPendingAction.sender_phone == sender_phone,
        ),
    ).first()
    pending_expires_at = _as_utc_aware(pending.expires_at) if pending else None
    if pending and pending_expires_at and pending_expires_at < now:
        session.delete(pending)
        session.commit()
        pending = None

    if pending and (_is_whatsapp_yes(text) or _is_whatsapp_no(text)):
        if _is_whatsapp_no(text):
            session.delete(pending)
            session.commit()
            _send_whatsapp_reply(cfg, sender_phone, "הפעולה בוטלה. אפשר לשלוח משימה חדשה.")
            return
        try:
            payload = json.loads(pending.pending_payload_json or "{}")
        except Exception:
            payload = {}
        try:
            res = confirm_chat_action(
                shop_id=int(shop.id or 0),
                body=ChatConfirmIn(approved=True, payload=payload),
                session=session,
                user=actor_user,
            )
        except Exception as ex:
            log.exception("whatsapp confirm failed shop_id=%s sender=%s", shop.id, sender_phone)
            _send_whatsapp_reply(cfg, sender_phone, f"שגיאה בביצוע הפעולה: {ex!s}")
            return
        session.delete(pending)
        session.commit()
        if res.status == "executed":
            _send_whatsapp_reply(
                cfg,
                sender_phone,
                f'בוצע בהצלחה: {res.action} עבור "{res.product_name or "המוצר"}".',
            )
        else:
            _send_whatsapp_reply(cfg, sender_phone, "הפעולה לא בוצעה.")
        return

    plan = await plan_chat_action(
        shop_id=int(shop.id or 0),
        body=ChatPlanIn(message=text),
        session=session,
        user=actor_user,
    )
    if plan.status == "needs_confirmation" and plan.confirm_payload:
        expires = now + timedelta(minutes=30)
        if pending is None:
            pending = ShopWhatsappPendingAction(
                shop_id=int(shop.id or 0),
                sender_phone=sender_phone,
                pending_payload_json=json.dumps(plan.confirm_payload, ensure_ascii=False),
                pending_question=plan.question,
                created_at=now,
                expires_at=expires,
            )
            session.add(pending)
        else:
            pending.pending_payload_json = json.dumps(plan.confirm_payload, ensure_ascii=False)
            pending.pending_question = plan.question
            pending.expires_at = expires
            session.add(pending)
        session.commit()
        _send_whatsapp_confirmation(cfg, sender_phone, plan.question)
        return

    if pending is not None:
        session.delete(pending)
        session.commit()

    if plan.status == "needs_disambiguation" and plan.candidates:
        opts = ", ".join(c.name for c in plan.candidates[:5])
        _send_whatsapp_reply(cfg, sender_phone, f"{plan.question}\nאפשרויות: {opts}")
        return
    _send_whatsapp_reply(cfg, sender_phone, plan.question)


def _send_whatsapp_reply(cfg: ShopWhatsappConfig, to_phone: str, text: str) -> None:
    if not cfg.access_token or not cfg.phone_number_id:
        return
    try:
        send_test_text_message(cfg.access_token, cfg.phone_number_id, to_phone, text[:1900])
    except MetaAuthError:
        raise
    except Exception:
        log.exception("whatsapp text reply failed shop_id=%s", cfg.shop_id)
        # Keep webhook flow stable on transient send errors.
        return


def _send_whatsapp_confirmation(cfg: ShopWhatsappConfig, to_phone: str, question: str) -> None:
    if not cfg.access_token or not cfg.phone_number_id:
        return
    try:
        send_interactive_confirm_buttons(
            cfg.access_token,
            cfg.phone_number_id,
            to_phone,
            question,
            yes_id="confirm_yes",
            no_id="confirm_no",
        )
        return
    except MetaAuthError:
        raise
    except Exception:
        # Some environments/numbers block interactive messages; fallback to text flow.
        log.exception("whatsapp interactive send failed shop_id=%s; fallback to text", cfg.shop_id)
    _send_whatsapp_reply(cfg, to_phone, f"{question}\n\nלהמשך השב: כן / לא")
