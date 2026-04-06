from datetime import datetime, timedelta, timezone

from jose import JWTError, jwt
from passlib.context import CryptContext

from backend.config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(p: str) -> str:
    return pwd_context.hash(p)


def verify_password(p: str, hashed: str) -> bool:
    return pwd_context.verify(p, hashed)


def create_access_token(sub: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(hours=settings.jwt_exp_hours)
    return jwt.encode(
        {"sub": sub, "exp": exp},
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm,
    )


def decode_token(token: str) -> str:
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        sub = payload.get("sub")
        if not isinstance(sub, str):
            raise JWTError("no sub")
        return sub
    except JWTError as e:
        raise ValueError("invalid token") from e
