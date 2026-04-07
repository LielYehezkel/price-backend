from collections.abc import Generator

from sqlalchemy import text
from sqlmodel import Session, SQLModel, create_engine, select

from backend.config import settings

# Normalise the URL: Render supplies postgres:// but SQLAlchemy requires postgresql://
_db_url = settings.database_url
if _db_url.startswith("postgres://"):
    _db_url = _db_url.replace("postgres://", "postgresql://", 1)

engine = create_engine(_db_url)

DEFAULT_ADMIN_EMAIL = "liel@contra-adv.co.il"


def _migrate_user_is_admin() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE user ADD COLUMN is_admin INTEGER DEFAULT 0"))
        except Exception:
            pass


def _ensure_default_admin() -> None:
    from backend.auth_utils import hash_password
    from backend.models import User

    with Session(engine) as session:
        u = session.exec(select(User).where(User.email == DEFAULT_ADMIN_EMAIL)).first()
        if not u:
            session.add(
                User(
                    email=DEFAULT_ADMIN_EMAIL,
                    hashed_password=hash_password("123456"),
                    name="מנהל",
                    is_admin=True,
                )
            )
            session.commit()
        elif not u.is_admin:
            u.is_admin = True
            session.add(u)
            session.commit()


def _migrate_shop_last_scan_cycle_at() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE shop ADD COLUMN last_scan_cycle_at DATETIME"))
        except Exception:
            pass


def _migrate_shop_check_interval_minutes() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE shop ADD COLUMN check_interval_minutes INTEGER DEFAULT 360"))
        except Exception:
            pass
    with Session(engine) as session:
        from backend.models import Shop

        shops = session.exec(select(Shop)).all()
        for s in shops:
            m = getattr(s, "check_interval_minutes", None)
            if m is None or m == 0:
                h = s.check_interval_hours or 6
                s.check_interval_minutes = max(1, int(h * 60))
                session.add(s)
        session.commit()


def _migrate_shop_woo_currency() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE shop ADD COLUMN woo_currency VARCHAR"))
        except Exception:
            pass


def _migrate_product_extended() -> None:
    cols = [
        ("image_url", "VARCHAR"),
        ("category_name", "VARCHAR"),
        ("category_path", "VARCHAR"),
        ("last_price_sync_at", "DATETIME"),
        ("auto_pricing_enabled", "INTEGER DEFAULT 0"),
        ("auto_pricing_min_price", "FLOAT"),
        ("auto_pricing_trigger_kind", "VARCHAR DEFAULT 'percent'"),
        ("auto_pricing_trigger_value", "FLOAT"),
        ("auto_pricing_action_kind", "VARCHAR DEFAULT 'percent'"),
        ("auto_pricing_action_value", "FLOAT"),
    ]
    for name, typ in cols:
        with engine.begin() as conn:
            try:
                conn.execute(text(f"ALTER TABLE product ADD COLUMN {name} {typ}"))
            except Exception:
                pass


def _migrate_shop_setup_and_pricing_strategy() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE shop ADD COLUMN setup_checklist_dismissed INTEGER DEFAULT 0"))
        except Exception:
            pass
    with engine.begin() as conn:
        try:
            conn.execute(
                text("ALTER TABLE product ADD COLUMN auto_pricing_strategy VARCHAR DEFAULT 'reactive_down'"),
            )
        except Exception:
            pass
    with engine.begin() as conn:
        try:
            conn.execute(
                text(
                    "ALTER TABLE competitorlink ADD COLUMN tracked_competitor_id INTEGER REFERENCES trackedcompetitor(id)",
                ),
            )
        except Exception:
            pass


def _backfill_tracked_competitors() -> None:
    from backend.models import CompetitorLink, Product, TrackedCompetitor
    from backend.services.domain_policy import domain_from_url

    with Session(engine) as session:
        links = session.exec(select(CompetitorLink).join(Product).order_by(CompetitorLink.id)).all()
        for c in links:
            dom = domain_from_url(c.url)
            if not dom:
                continue
            prod = session.get(Product, c.product_id)
            if not prod:
                continue
            sid = prod.shop_id
            tc = session.exec(
                select(TrackedCompetitor).where(
                    TrackedCompetitor.shop_id == sid,
                    TrackedCompetitor.domain == dom,
                ),
            ).first()
            if not tc:
                disp = (c.label or "").strip() or dom
                tc = TrackedCompetitor(shop_id=sid, domain=dom, display_name=disp[:240])
                session.add(tc)
                session.flush()
            tid = getattr(c, "tracked_competitor_id", None)
            if tid is None:
                c.tracked_competitor_id = tc.id
                if not (c.label or "").strip():
                    c.label = tc.display_name
                session.add(c)
        session.commit()


def _ensure_price_sanity_defaults() -> None:
    from backend.models import PriceSanitySettings

    with Session(engine) as session:
        row = session.get(PriceSanitySettings, 1)
        if not row:
            session.add(PriceSanitySettings(id=1))
            session.commit()


def _ensure_scheduler_heartbeat_row() -> None:
    from backend.models import SchedulerHeartbeat

    with Session(engine) as session:
        if session.get(SchedulerHeartbeat, 1) is None:
            session.add(SchedulerHeartbeat(id=1))
            session.commit()


def _ensure_admin_system_config_row() -> None:
    from backend.models import AdminSystemConfig

    with Session(engine) as session:
        if session.get(AdminSystemConfig, 1) is None:
            session.add(AdminSystemConfig(id=1, backend_mode="local"))
            session.commit()


def _migrate_alert_kind_column() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE alert ADD COLUMN kind VARCHAR DEFAULT 'general'"))
        except Exception:
            pass
    stmts = [
        "UPDATE alert SET kind = 'auto_pricing' WHERE kind = 'general' AND message LIKE 'תמחור%'",
        "UPDATE alert SET kind = 'sanity_failed' WHERE kind = 'general' AND message LIKE '%סריקה נדחתה%'",
        "UPDATE alert SET kind = 'competitor_cheaper' WHERE kind = 'general' AND message LIKE '%זול יותר%'",
        "UPDATE alert SET kind = 'price_change' WHERE kind = 'general' AND message LIKE '%שינוי מחיר%'",
    ]
    with engine.begin() as conn:
        for s in stmts:
            try:
                conn.execute(text(s))
            except Exception:
                pass


def init_db() -> None:
    # Ensure all SQLModel tables are registered on metadata before create_all
    from backend import models as _models  # noqa: F401

    SQLModel.metadata.create_all(engine)
    _migrate_alert_kind_column()
    _migrate_user_is_admin()
    # חייב לפני כל שאילתת ORM על shop/product/competitorlink עם עמודות חדשות
    _migrate_shop_setup_and_pricing_strategy()
    _migrate_shop_woo_currency()
    _migrate_product_extended()
    _migrate_shop_last_scan_cycle_at()
    _migrate_shop_check_interval_minutes()
    _ensure_default_admin()
    _ensure_price_sanity_defaults()
    _ensure_scheduler_heartbeat_row()
    _ensure_admin_system_config_row()
    _backfill_tracked_competitors()


def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session
