from __future__ import annotations

from fastapi import Request
from sqlmodel import Session

from backend.config import settings
from backend.models import AdminSystemConfig


def get_or_create_system_config(session: Session) -> AdminSystemConfig:
    row = session.get(AdminSystemConfig, 1)
    if row is None:
        row = AdminSystemConfig(id=1, backend_mode="local")
        session.add(row)
        session.commit()
        session.refresh(row)
    return row


def resolve_public_api_base(session: Session, request: Request | None = None) -> str:
    """
    מחזיר base URL ליצירת תוסף:
    - custom: משתמש בכתובת שנשמרה בפאנל ניהול
    - local: התנהגות קיימת (env + fallback לבקשה)
    """
    row = get_or_create_system_config(session)
    configured = settings.public_api_base.rstrip("/")
    mode = (row.backend_mode or "local").strip().lower()
    custom = (row.backend_api_base or "").strip().rstrip("/")

    if mode == "custom" and custom.startswith(("http://", "https://")):
        return custom

    if request is not None:
        host = (request.url.hostname or "").lower()
        if host and host not in {"127.0.0.1", "localhost"} and (
            configured.startswith("http://127.0.0.1") or configured.startswith("http://localhost")
        ):
            return str(request.base_url).rstrip("/")
    return configured

