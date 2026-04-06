"""בדיקת אמינות מחיר מתחרה לפני קבלה — מניעת ערכים חריגים."""

from __future__ import annotations

from sqlmodel import Session, select

from backend.models import PriceSanitySettings


def get_settings(session: Session) -> PriceSanitySettings:
    row = session.get(PriceSanitySettings, 1)
    if not row:
        row = PriceSanitySettings(id=1)
        session.add(row)
        session.commit()
        session.refresh(row)
    return row


def validate_competitor_price(
    session: Session,
    price: float | None,
    previous_competitor: float | None,
    our_price: float | None,
) -> tuple[bool, str | None]:
    """
    מחזיר (ok, reason_hebrew).
    אם price הוא None — עובר (אין מה לדחות).
    """
    if price is None:
        return True, None
    cfg = get_settings(session)
    if not cfg.enabled:
        return True, None

    p = float(price)
    if p < cfg.abs_min or p > cfg.abs_max:
        return False, f"מחיר מחוץ לטווח המותר ({cfg.abs_min}–{cfg.abs_max})"

    mult_prev = max(cfg.vs_prev_max_multiplier, 1.001)
    if previous_competitor is not None and previous_competitor > 0:
        lo = previous_competitor / mult_prev
        hi = previous_competitor * mult_prev
        if p < lo or p > hi:
            return False, "קפיצה חריגה מול מחיר המתחרה הקודם"

    mult_our = max(cfg.vs_ours_max_multiplier, 1.001)
    if our_price is not None and our_price > 0:
        lo = our_price / mult_our
        hi = our_price * mult_our
        if p < lo or p > hi:
            return False, "מחיר המתחרה חריג ביחס למחיר המוצר אצלכם"

    return True, None
