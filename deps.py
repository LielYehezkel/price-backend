from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlmodel import Session, select

from backend.auth_utils import decode_token
from backend.db import get_session
from backend.models import Shop, ShopMember, User

security = HTTPBearer(auto_error=False)


def get_current_user(
    session: Annotated[Session, Depends(get_session)],
    cred: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> User:
    if cred is None or cred.scheme.lower() != "bearer":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "לא מחובר")
    try:
        sub = decode_token(cred.credentials)
        uid = int(sub)
    except (ValueError, TypeError):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "טוקן לא תקין") from None
    user = session.get(User, uid)
    if not user:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "משתמש לא נמצא")
    return user


def require_shop_access(
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
    shop_id: int,
) -> Shop:
    shop = session.get(Shop, shop_id)
    if not shop:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "חנות לא נמצאה")
    if shop.owner_id == user.id:
        return shop
    row = session.exec(
        select(ShopMember).where(
            ShopMember.shop_id == shop_id,
            ShopMember.user_id == user.id,
        )
    ).first()
    if not row:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "אין הרשאה לחנות זו")
    return shop


def get_current_admin(user: Annotated[User, Depends(get_current_user)]) -> User:
    if not user.is_admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "נדרשת הרשאת מנהל")
    return user
