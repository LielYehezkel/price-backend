from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr
from sqlmodel import Session, select

from backend.auth_utils import create_access_token, hash_password, verify_password
from backend.db import get_session
from backend.deps import get_current_user
from backend.models import User

router = APIRouter(prefix="/api/auth", tags=["auth"])


class RegisterIn(BaseModel):
    email: EmailStr
    password: str
    name: str | None = None


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserOut(BaseModel):
    id: int
    email: str
    name: str | None
    is_admin: bool = False


class PasswordChangeIn(BaseModel):
    current_password: str
    new_password: str


@router.post("/register", response_model=TokenOut)
def register(body: RegisterIn, session: Annotated[Session, Depends(get_session)]) -> TokenOut:
    exists = session.exec(select(User).where(User.email == body.email)).first()
    if exists:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "אימייל כבר רשום")
    user = User(
        email=body.email,
        hashed_password=hash_password(body.password),
        name=body.name,
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    token = create_access_token(str(user.id))
    return TokenOut(access_token=token)


@router.post("/login", response_model=TokenOut)
def login(body: LoginIn, session: Annotated[Session, Depends(get_session)]) -> TokenOut:
    user = session.exec(select(User).where(User.email == body.email)).first()
    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "אימייל או סיסמה שגויים")
    token = create_access_token(str(user.id))
    return TokenOut(access_token=token)


@router.get("/me", response_model=UserOut)
def me(user: Annotated[User, Depends(get_current_user)]) -> UserOut:
    return UserOut(id=user.id, email=user.email, name=user.name, is_admin=user.is_admin)


@router.post("/me/password")
def change_password(
    body: PasswordChangeIn,
    session: Annotated[Session, Depends(get_session)],
    user: Annotated[User, Depends(get_current_user)],
) -> dict[str, bool]:
    if not verify_password(body.current_password, user.hashed_password):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "סיסמה נוכחית שגויה")
    user.hashed_password = hash_password(body.new_password)
    session.add(user)
    session.commit()
    return {"ok": True}
