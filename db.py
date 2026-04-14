from collections.abc import Generator
import logging

from sqlalchemy import text
from sqlmodel import Session, SQLModel, create_engine, select

from backend.config import settings

# Normalise the URL: Render supplies postgres:// but SQLAlchemy requires postgresql://
_db_url = settings.database_url
if _db_url.startswith("postgres://"):
    _db_url = _db_url.replace("postgres://", "postgresql://", 1)

engine = create_engine(_db_url)
log = logging.getLogger(__name__)

DEFAULT_ADMIN_EMAIL = "liel@contra-adv.co.il"


def _is_duplicate_column_error(ex: Exception) -> bool:
    msg = str(ex).lower()
    return "already exists" in msg or "duplicate column" in msg


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
            conn.execute(text("ALTER TABLE shop ADD COLUMN last_scan_cycle_at TIMESTAMP"))
        except Exception as ex:
            if not _is_duplicate_column_error(ex):
                log.warning("Migration failed: shop.last_scan_cycle_at: %s", ex)


def _migrate_shop_packages() -> None:
    cols = [
        ("package_tier", "VARCHAR DEFAULT 'free'"),
        ("package_max_scan_runs_per_day", "INTEGER DEFAULT 10"),
        ("package_max_scans_per_day_window", "INTEGER DEFAULT 1"),
        ("package_min_interval_minutes", "INTEGER DEFAULT 1440"),
    ]
    for name, typ in cols:
        with engine.begin() as conn:
            try:
                conn.execute(text(f"ALTER TABLE shop ADD COLUMN {name} {typ}"))
            except Exception as ex:
                if not _is_duplicate_column_error(ex):
                    log.warning("Migration failed: shop.%s: %s", name, ex)
    with engine.begin() as conn:
        try:
            conn.execute(text("UPDATE shop SET package_tier = 'free' WHERE package_tier IS NULL OR package_tier = ''"))
            conn.execute(
                text(
                    "UPDATE shop SET package_max_scan_runs_per_day = 10 "
                    "WHERE package_max_scan_runs_per_day IS NULL OR package_max_scan_runs_per_day < 1",
                ),
            )
            conn.execute(
                text(
                    "UPDATE shop SET package_max_scans_per_day_window = 1 "
                    "WHERE package_max_scans_per_day_window IS NULL OR package_max_scans_per_day_window < 1",
                ),
            )
            conn.execute(
                text(
                    "UPDATE shop SET package_min_interval_minutes = 1440 "
                    "WHERE package_min_interval_minutes IS NULL OR package_min_interval_minutes < 1",
                ),
            )
        except Exception as ex:
            log.warning("Migration failed: shop package backfill: %s", ex)


def _migrate_shop_platform_shopify() -> None:
    shop_cols = [
        ("store_platform", "VARCHAR DEFAULT 'wordpress'"),
        ("shopify_shop_domain", "VARCHAR"),
        ("shopify_admin_access_token", "VARCHAR"),
        ("shopify_api_version", "VARCHAR DEFAULT '2024-10'"),
        ("shopify_webhook_secret", "VARCHAR"),
        ("shopify_client_secret", "VARCHAR"),
    ]
    for name, typ in shop_cols:
        with engine.begin() as conn:
            try:
                conn.execute(text(f"ALTER TABLE shop ADD COLUMN {name} {typ}"))
            except Exception as ex:
                if not _is_duplicate_column_error(ex):
                    log.warning("Migration failed: shop.%s: %s", name, ex)
    with engine.begin() as conn:
        try:
            conn.execute(
                text(
                    "UPDATE shop SET store_platform = 'wordpress' "
                    "WHERE store_platform IS NULL OR store_platform = ''",
                ),
            )
        except Exception as ex:
            log.warning("Migration failed: shop store_platform backfill: %s", ex)
    prod_cols = [
        ("shopify_product_id", "INTEGER"),
        ("shopify_variant_id", "INTEGER"),
        ("shopify_inventory_item_id", "INTEGER"),
    ]
    for name, typ in prod_cols:
        with engine.begin() as conn:
            try:
                conn.execute(text(f"ALTER TABLE product ADD COLUMN {name} {typ}"))
            except Exception as ex:
                if not _is_duplicate_column_error(ex):
                    log.warning("Migration failed: product.%s: %s", name, ex)


def _migrate_shop_scan_quota_daily() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(
                text(
                    "CREATE TABLE shopscanquotadaily ("
                    "id INTEGER PRIMARY KEY, "
                    "shop_id INTEGER REFERENCES shop(id), "
                    "bucket_date VARCHAR(10), "
                    "runs_count INTEGER DEFAULT 0, "
                    "updated_at TIMESTAMP"
                    ")",
                ),
            )
        except Exception as ex:
            msg = str(ex).lower()
            if "already exists" not in msg and "duplicate" not in msg:
                log.warning("Migration failed: create shopscanquotadaily: %s", ex)
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE UNIQUE INDEX uq_shop_scan_quota_daily ON shopscanquotadaily(shop_id, bucket_date)"))
        except Exception:
            pass
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE INDEX ix_shopscanquotadaily_shop_id ON shopscanquotadaily(shop_id)"))
        except Exception:
            pass
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE INDEX ix_shopscanquotadaily_bucket_date ON shopscanquotadaily(bucket_date)"))
        except Exception:
            pass


def _migrate_shop_package_audit_log() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(
                text(
                    "CREATE TABLE shoppackageauditlog ("
                    "id INTEGER PRIMARY KEY, "
                    "shop_id INTEGER REFERENCES shop(id), "
                    'changed_by_user_id INTEGER REFERENCES "user"(id), '
                    "previous_tier VARCHAR DEFAULT 'free', "
                    "new_tier VARCHAR DEFAULT 'free', "
                    "previous_max_scan_runs_per_day INTEGER DEFAULT 10, "
                    "new_max_scan_runs_per_day INTEGER DEFAULT 10, "
                    "previous_max_scans_per_day_window INTEGER DEFAULT 1, "
                    "new_max_scans_per_day_window INTEGER DEFAULT 1, "
                    "previous_min_interval_minutes INTEGER DEFAULT 1440, "
                    "new_min_interval_minutes INTEGER DEFAULT 1440, "
                    "change_note VARCHAR, "
                    "created_at TIMESTAMP"
                    ")",
                ),
            )
        except Exception as ex:
            msg = str(ex).lower()
            if "already exists" not in msg and "duplicate" not in msg:
                log.warning("Migration failed: create shoppackageauditlog: %s", ex)
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE INDEX ix_shoppackageauditlog_shop_id ON shoppackageauditlog(shop_id)"))
        except Exception:
            pass
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE INDEX ix_shoppackageauditlog_changed_by ON shoppackageauditlog(changed_by_user_id)"))
        except Exception:
            pass
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE INDEX ix_shoppackageauditlog_created_at ON shoppackageauditlog(created_at)"))
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
        ("last_price_sync_at", "TIMESTAMP"),
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
            except Exception as ex:
                if not _is_duplicate_column_error(ex):
                    log.warning("Migration failed: product.%s: %s", name, ex)


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


def _migrate_competitor_link_light_html_hash() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE competitorlink ADD COLUMN last_light_html_hash VARCHAR(64)"))
        except Exception as ex:
            if not _is_duplicate_column_error(ex):
                log.warning("Migration failed: competitorlink.last_light_html_hash: %s", ex)


def _migrate_domain_price_selector_fetch_strategy() -> None:
    with engine.begin() as conn:
        try:
            conn.execute(
                text("ALTER TABLE domainpriceselector ADD COLUMN fetch_strategy VARCHAR DEFAULT 'http'"),
            )
        except Exception as ex:
            if not _is_duplicate_column_error(ex):
                log.warning("Migration failed: domainpriceselector.fetch_strategy: %s", ex)


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


def _migrate_shop_ai_action_log() -> None:
    stmts = [
        (
            "shop_id",
            "ALTER TABLE shopaiactionlog ADD COLUMN shop_id INTEGER REFERENCES shop(id)",
        ),
        (
            "user_id",
            'ALTER TABLE shopaiactionlog ADD COLUMN user_id INTEGER REFERENCES "user"(id)',
        ),
        (
            "action",
            "ALTER TABLE shopaiactionlog ADD COLUMN action VARCHAR",
        ),
        (
            "product_id",
            "ALTER TABLE shopaiactionlog ADD COLUMN product_id INTEGER REFERENCES product(id)",
        ),
        (
            "payload_json",
            "ALTER TABLE shopaiactionlog ADD COLUMN payload_json VARCHAR DEFAULT ''",
        ),
        (
            "status",
            "ALTER TABLE shopaiactionlog ADD COLUMN status VARCHAR DEFAULT 'executed'",
        ),
        (
            "created_at",
            "ALTER TABLE shopaiactionlog ADD COLUMN created_at TIMESTAMP",
        ),
        (
            "undo_deadline_at",
            "ALTER TABLE shopaiactionlog ADD COLUMN undo_deadline_at TIMESTAMP",
        ),
        (
            "undone_at",
            "ALTER TABLE shopaiactionlog ADD COLUMN undone_at TIMESTAMP",
        ),
        (
            "undone_by_user_id",
            'ALTER TABLE shopaiactionlog ADD COLUMN undone_by_user_id INTEGER REFERENCES "user"(id)',
        ),
        (
            "undo_note",
            "ALTER TABLE shopaiactionlog ADD COLUMN undo_note VARCHAR",
        ),
    ]
    for _, sql in stmts:
        with engine.begin() as conn:
            try:
                conn.execute(text(sql))
            except Exception as ex:
                if not _is_duplicate_column_error(ex):
                    msg = str(ex).lower()
                    # First deployment where table does not exist yet.
                    if "no such table" not in msg and "does not exist" not in msg:
                        log.warning("Migration failed: shopaiactionlog: %s", ex)


def _migrate_shop_whatsapp_config() -> None:
    stmts = [
        (
            "enabled",
            "ALTER TABLE shopwhatsappconfig ADD COLUMN enabled INTEGER DEFAULT 0",
        ),
        ("phone_number_id", "ALTER TABLE shopwhatsappconfig ADD COLUMN phone_number_id VARCHAR"),
        ("business_account_id", "ALTER TABLE shopwhatsappconfig ADD COLUMN business_account_id VARCHAR"),
        ("verify_token", "ALTER TABLE shopwhatsappconfig ADD COLUMN verify_token VARCHAR"),
        ("access_token", "ALTER TABLE shopwhatsappconfig ADD COLUMN access_token VARCHAR"),
        ("alert_phone_e164", "ALTER TABLE shopwhatsappconfig ADD COLUMN alert_phone_e164 VARCHAR"),
        ("webhook_path_secret", "ALTER TABLE shopwhatsappconfig ADD COLUMN webhook_path_secret VARCHAR"),
        ("sales_webhook_secret", "ALTER TABLE shopwhatsappconfig ADD COLUMN sales_webhook_secret VARCHAR"),
        (
            "created_by_user_id",
            'ALTER TABLE shopwhatsappconfig ADD COLUMN created_by_user_id INTEGER REFERENCES "user"(id)',
        ),
        (
            "updated_by_user_id",
            'ALTER TABLE shopwhatsappconfig ADD COLUMN updated_by_user_id INTEGER REFERENCES "user"(id)',
        ),
        ("updated_at", "ALTER TABLE shopwhatsappconfig ADD COLUMN updated_at TIMESTAMP"),
    ]
    for _, sql in stmts:
        with engine.begin() as conn:
            try:
                conn.execute(text(sql))
            except Exception as ex:
                if not _is_duplicate_column_error(ex):
                    msg = str(ex).lower()
                    if "no such table" not in msg and "does not exist" not in msg:
                        log.warning("Migration failed: shopwhatsappconfig: %s", ex)


def _migrate_user_shop_preferences_sales_notifications() -> None:
    cols = [
        ("notify_sale_live", "INTEGER DEFAULT 0"),
        ("notify_sales_daily", "INTEGER DEFAULT 0"),
        ("notify_sales_monthly", "INTEGER DEFAULT 0"),
    ]
    for name, typ in cols:
        with engine.begin() as conn:
            try:
                conn.execute(text(f"ALTER TABLE usershoppreferences ADD COLUMN {name} {typ}"))
            except Exception as ex:
                if not _is_duplicate_column_error(ex):
                    msg = str(ex).lower()
                    if "no such table" not in msg and "does not exist" not in msg:
                        log.warning("Migration failed: usershoppreferences.%s: %s", name, ex)


def init_db() -> None:
    # Ensure all SQLModel tables are registered on metadata before create_all
    from backend import models as _models  # noqa: F401

    SQLModel.metadata.create_all(engine)
    _migrate_alert_kind_column()
    _migrate_user_is_admin()
    # חייב לפני כל שאילתת ORM על shop/product/competitorlink עם עמודות חדשות
    _migrate_shop_setup_and_pricing_strategy()
    _migrate_shop_woo_currency()
    _migrate_shop_packages()
    _migrate_shop_platform_shopify()
    _migrate_shop_scan_quota_daily()
    _migrate_shop_package_audit_log()
    _migrate_product_extended()
    _migrate_shop_last_scan_cycle_at()
    _migrate_shop_check_interval_minutes()
    _ensure_default_admin()
    _ensure_price_sanity_defaults()
    _ensure_scheduler_heartbeat_row()
    _ensure_admin_system_config_row()
    _migrate_shop_ai_action_log()
    _migrate_shop_whatsapp_config()
    _migrate_user_shop_preferences_sales_notifications()
    _migrate_competitor_link_light_html_hash()
    _migrate_domain_price_selector_fetch_strategy()
    _backfill_tracked_competitors()


def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session
