"""Domain-level price visibility: new domains need admin approval before customers see prices."""

from __future__ import annotations

from urllib.parse import urlparse

from sqlmodel import Session, select

from backend.models import CompetitorLink, DomainPriceApproval, DomainPriceSelector


def domain_from_url(url: str) -> str:
    raw = url if "://" in url else f"https://{url}"
    p = urlparse(raw)
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host or ""


def domain_is_live(session: Session, domain: str) -> bool:
    """
    True if this domain may show prices to end users:
    - learned selector exists (legacy / כלי מחיר), or
    - admin approved DomainPriceApproval.
    """
    if not domain:
        return False
    if session.get(DomainPriceSelector, domain):
        return True
    row = session.get(DomainPriceApproval, domain)
    return row is not None and row.status == "approved"


def iter_competitor_ids_for_domain(session: Session, domain: str) -> list[int]:
    if not domain:
        return []
    out: list[int] = []
    for c in session.exec(select(CompetitorLink)).all():
        if domain_from_url(c.url) == domain:
            out.append(c.id)
    return out
