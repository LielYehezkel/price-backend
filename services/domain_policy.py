"""Domain-level price visibility: new domains need admin approval before customers see prices."""

from __future__ import annotations

from urllib.parse import urlparse

from sqlmodel import Session, select

from backend.models import CompetitorLink, DomainPriceApproval, DomainPriceSelector, DomainReviewQueueItem, utcnow


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


def clear_domain_review_pending_for_live_domain(session: Session, domain: str) -> int:
    """
    כלל מערכת:
    אם לדומיין יש selector שמור, אסור שיישארו לו פריטי pending בתור אישורי דומיין.
    """
    d = (domain or "").strip().lower()
    if not d or session.get(DomainPriceSelector, d) is None:
        return 0

    now = utcnow()
    changed = 0
    rows = session.exec(
        select(DomainReviewQueueItem).where(
            DomainReviewQueueItem.domain == d,
            DomainReviewQueueItem.status == "pending",
        ),
    ).all()
    for r in rows:
        r.status = "resolved"
        r.resolved_at = now
        session.add(r)
        changed += 1

    dpa = session.get(DomainPriceApproval, d)
    if dpa and dpa.status != "approved":
        dpa.status = "approved"
        if dpa.approved_at is None:
            dpa.approved_at = now
        dpa.updated_at = now
        session.add(dpa)
        changed += 1
    return changed
