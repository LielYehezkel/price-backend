import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from backend.models import utcnow


@dataclass
class CachedResolve:
    html: str
    url: str
    expires: datetime


_cache: dict[str, CachedResolve] = {}


def put_cache(html: str, url: str, ttl_minutes: int = 10) -> str:
    token = secrets.token_urlsafe(24)
    _cache[token] = CachedResolve(
        html=html,
        url=url,
        expires=utcnow() + timedelta(minutes=ttl_minutes),
    )
    return token


def get_cache(token: str) -> CachedResolve | None:
    c = _cache.get(token)
    if not c:
        return None
    if utcnow() > c.expires:
        _cache.pop(token, None)
        return None
    return c
