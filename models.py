from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import UniqueConstraint
from sqlmodel import Field, SQLModel


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TrackedCompetitor(SQLModel, table=True):
    """מתחרה לוגי לפי דומיין — שם תצוגה נרשם בפעם הראשונה שמוסיפים קישור לדומיין."""

    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    domain: str = Field(index=True)
    display_name: str
    created_at: datetime = Field(default_factory=utcnow)

    __table_args__ = (UniqueConstraint("shop_id", "domain", name="uq_tracked_competitor_shop_domain"),)


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(unique=True, index=True)
    hashed_password: str
    name: Optional[str] = None
    is_admin: bool = Field(default=False)
    created_at: datetime = Field(default_factory=utcnow)


class Shop(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    owner_id: int = Field(foreign_key="user.id")
    check_interval_hours: int = Field(default=6)  # legacy; prefer check_interval_minutes
    check_interval_minutes: int = Field(default=360)  # default = 6h; min 1 for testing
    # מחזור סריקה מלא (כל קישורי המתחרה ברצף) — מתעדכן בסיום כל ריצה
    last_scan_cycle_at: Optional[datetime] = None
    woo_site_url: Optional[str] = None
    woo_consumer_key: Optional[str] = None
    woo_consumer_secret: Optional[str] = None
    woo_currency: Optional[str] = None  # ISO code from WooCommerce (e.g. ILS)
    setup_checklist_dismissed: bool = Field(default=False)
    created_at: datetime = Field(default_factory=utcnow)


class ShopMember(SQLModel, table=True):
    shop_id: int = Field(foreign_key="shop.id", primary_key=True)
    user_id: int = Field(foreign_key="user.id", primary_key=True)
    role: str = Field(default="member")  # owner | member


class UserShopPreferences(SQLModel, table=True):
    """העדפות התראות והמלצות דשבורד למשתמש בחנות."""

    user_id: int = Field(foreign_key="user.id", primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", primary_key=True)
    notify_competitor_cheaper: bool = Field(default=True)
    notify_price_change: bool = Field(default=True)
    notify_auto_pricing: bool = Field(default=True)
    notify_sanity: bool = Field(default=True)
    dismissed_recommendation_ids_json: str = Field(default="[]")
    updated_at: datetime = Field(default_factory=utcnow)


class Product(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    woo_product_id: Optional[int] = None
    name: str
    sku: Optional[str] = None
    permalink: Optional[str] = None
    image_url: Optional[str] = None
    category_name: Optional[str] = None
    category_path: Optional[str] = None
    regular_price: Optional[float] = None
    last_price_sync_at: Optional[datetime] = None
    # תמחור אוטומטי מול מתחרים (אופציונלי, פר מוצר)
    auto_pricing_enabled: bool = Field(default=False)
    auto_pricing_min_price: Optional[float] = None
    auto_pricing_trigger_kind: str = Field(default="percent")  # percent | amount
    auto_pricing_trigger_value: Optional[float] = None
    auto_pricing_action_kind: str = Field(default="percent")  # percent | amount
    auto_pricing_action_value: Optional[float] = None
    # reactive_down = רק הורדה כשמתקיים תנאי (התנהגות מקורית)
    # smart_anchor = עוגן מול המחיר הנמוך בשוק — עולה ויורד לפי הכלל (רווחיות)
    auto_pricing_strategy: str = Field(default="reactive_down")


class CompetitorLink(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="product.id", index=True)
    tracked_competitor_id: Optional[int] = Field(default=None, foreign_key="trackedcompetitor.id", index=True)
    url: str
    label: Optional[str] = None
    last_price: Optional[float] = None
    last_currency: Optional[str] = None
    last_checked_at: Optional[datetime] = None
    # hash SHA256 (hex) של 50KB ראשונים מ-GET קל — השוואה כשאין חילוץ מחיר בprecheck
    last_light_html_hash: Optional[str] = Field(default=None, max_length=64)


class PriceSnapshot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    competitor_link_id: int = Field(foreign_key="competitorlink.id", index=True)
    price: Optional[float] = None
    currency: Optional[str] = None
    fetched_at: datetime = Field(default_factory=utcnow)


class ScanLog(SQLModel, table=True):
    """Audit log for each competitor price scan (serial queue)."""

    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    product_id: int = Field(foreign_key="product.id", index=True)
    competitor_link_id: int = Field(foreign_key="competitorlink.id", index=True)
    competitor_domain: str = Field(index=True)
    product_name: str = ""
    our_price: Optional[float] = None
    competitor_price: Optional[float] = None
    previous_competitor_price: Optional[float] = None
    price_changed: bool = Field(default=False)
    # you_cheaper = we are cheaper than competitor (good); you_expensive = competitor cheaper than us
    comparison: str = Field(default="unknown", index=True)
    created_at: datetime = Field(default_factory=utcnow)


class Alert(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    product_id: Optional[int] = Field(default=None, foreign_key="product.id")
    message: str
    severity: str = Field(default="info")
    read: bool = Field(default=False)
    # competitor_cheaper | price_change | auto_pricing | sanity_failed | general
    kind: str = Field(default="general", index=True)
    created_at: datetime = Field(default_factory=utcnow)


class Invite(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id")
    email: str
    token: str = Field(index=True, unique=True)
    role: str = Field(default="member")
    created_at: datetime = Field(default_factory=utcnow)


class ShopOwnershipTransfer(SQLModel, table=True):
    """בקשת העברת בעלות חנות בין משתמשים."""

    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    from_user_id: int = Field(foreign_key="user.id", index=True)
    to_user_id: int = Field(foreign_key="user.id", index=True)
    to_email: str = Field(index=True)
    status: str = Field(default="pending", index=True)  # pending | accepted | declined | canceled | expired
    note: Optional[str] = None
    created_at: datetime = Field(default_factory=utcnow, index=True)
    expires_at: datetime = Field(default_factory=utcnow)
    responded_at: Optional[datetime] = None


class ApiKey(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    name: str
    key_hash: str
    prefix: str
    created_at: datetime = Field(default_factory=utcnow)


class DomainPriceSelector(SQLModel, table=True):
    domain: str = Field(primary_key=True)
    css_selector: str
    alternates_json: Optional[str] = None
    # איך למשוך דפים לדומיין הזה אחרי שלמדנו סלקטור: http | playwright_proxy
    fetch_strategy: str = Field(default="http")
    updated_at: datetime = Field(default_factory=utcnow)


class PriceResolveUrlCache(SQLModel, table=True):
    """מטמון resolve לפי URL — מאפשר לדלג על Playwright כשאין שינוי (lightweight + hash)."""

    url_key: str = Field(primary_key=True, max_length=64)  # sha256 hex של URL מנורמל
    url_canonical: str = Field(max_length=4096)
    domain: str = Field(index=True, max_length=512)
    last_price: Optional[float] = None
    last_checked_at: datetime = Field(default_factory=utcnow)
    last_html_prefix_hash: Optional[str] = Field(default=None, max_length=64)


class DomainReviewQueueItem(SQLModel, table=True):
    """תור ביקורת — פריט לכל מוצר/קישור; מאפשר מאות ממתינים לאותו דומיין בלי דריסה."""

    id: Optional[int] = Field(default=None, primary_key=True)
    domain: str = Field(index=True)
    competitor_link_id: int = Field(foreign_key="competitorlink.id", index=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    product_name: str = ""
    sample_url: str = ""
    pending_price: Optional[float] = None
    pending_currency: Optional[str] = None
    candidates_json: Optional[str] = None
    suggested_selector: Optional[str] = None
    source: str = Field(default="scan", index=True)  # scan | user_report
    reporter_note: Optional[str] = None
    status: str = Field(default="pending", index=True)  # pending | resolved
    created_at: datetime = Field(default_factory=utcnow)
    resolved_at: Optional[datetime] = None


class DomainPriceApproval(SQLModel, table=True):
    """Admin gate: new competitor domains stay 'pending' until selector is approved."""

    domain: str = Field(primary_key=True)
    status: str = Field(default="pending", index=True)  # pending | approved
    sample_url: str = ""
    pending_price: Optional[float] = None
    pending_currency: Optional[str] = None
    candidates_json: Optional[str] = None
    suggested_selector: Optional[str] = None
    updated_at: datetime = Field(default_factory=utcnow)
    approved_at: Optional[datetime] = None
    approved_by_user_id: Optional[int] = Field(default=None, foreign_key="user.id")


class WpSetupToken(SQLModel, table=True):
    """טוקן חד-פעמי להתקנת תוסף WordPress וחיבור מפתחות Woo."""

    token: str = Field(primary_key=True, max_length=96)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    created_by_user_id: int = Field(foreign_key="user.id")
    created_at: datetime = Field(default_factory=utcnow)
    expires_at: datetime
    used_at: Optional[datetime] = None


class WpConnectionToken(SQLModel, table=True):
    """טוקן חיבור קבוע לתוסף WordPress (ללא תפוגה אוטומטית)."""

    token: str = Field(primary_key=True, max_length=128)
    shop_id: int = Field(foreign_key="shop.id", index=True, unique=True)
    created_by_user_id: int = Field(foreign_key="user.id")
    created_at: datetime = Field(default_factory=utcnow)
    active: bool = Field(default=True, index=True)
    last_used_at: Optional[datetime] = None


class AdminSystemConfig(SQLModel, table=True):
    """הגדרות מערכת גלובליות לניהול סביבת שרת/תקשורת."""

    id: int = Field(default=1, primary_key=True)
    backend_mode: str = Field(default="local")  # local | custom
    backend_api_base: Optional[str] = None
    updated_at: datetime = Field(default_factory=utcnow)


class SalesInsightsCache(SQLModel, table=True):
    """תוצאת דוח מכירות מחושבת — להחזרה מהירה ורענון ברקע."""

    __table_args__ = (UniqueConstraint("shop_id", "period_days", name="uq_sales_cache_shop_days"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    period_days: int = Field(index=True)
    payload_json: str = ""
    updated_at: datetime = Field(default_factory=utcnow)


class PriceSanitySettings(SQLModel, table=True):
    """הגדרות גלובליות לסף אמינות מחירים (שורה יחידה id=1)."""

    id: int = Field(default=1, primary_key=True)
    enabled: bool = Field(default=True)
    abs_min: float = Field(default=0.01)
    abs_max: float = Field(default=999_999.0)
    # מכפיל מקסימלי מול מחיר מתחרה קודם (למשל 5 => חייב להיות בין prev/5 ל־prev*5)
    vs_prev_max_multiplier: float = Field(default=5.0)
    # מכפיל מול מחיר החנות שלנו (למשל 15 => בין our/15 ל־our*15)
    vs_ours_max_multiplier: float = Field(default=15.0)
    updated_at: datetime = Field(default_factory=utcnow)


class SchedulerHeartbeat(SQLModel, table=True):
    """דופק המתזמן — שורה יחידה id=1 (מנוע הסריקות)."""

    id: int = Field(default=1, primary_key=True)
    updated_at: datetime = Field(default_factory=utcnow)
    last_tick_at: Optional[datetime] = None
    last_tick_duration_ms: int = 0
    last_tick_ok: bool = True
    last_tick_scans: int = 0
    last_tick_shops_touched: int = 0
    last_error_message: Optional[str] = None
    last_error_detail: Optional[str] = None
    last_error_at: Optional[datetime] = None
    consecutive_failures: int = 0
    total_ticks: int = 0


class AdminOperationalLog(SQLModel, table=True):
    """יומן תפעול למנהלים — כשלים ואירועים במנוע הסריקות ומעלה."""

    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    level: str = Field(default="info", index=True)
    code: str = Field(default="", index=True)
    title: str = ""
    detail: str = ""
    shop_id: Optional[int] = Field(default=None, foreign_key="shop.id", index=True)
    competitor_link_id: Optional[int] = Field(default=None, foreign_key="competitorlink.id", index=True)


class ShopAiActionLog(SQLModel, table=True):
    """Audit log for AI assistant actions with undo support."""

    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    user_id: int = Field(foreign_key="user.id", index=True)
    action: str = Field(index=True)  # reduce_price | out_of_stock | in_stock | bulk_reduce_price
    product_id: Optional[int] = Field(default=None, foreign_key="product.id", index=True)
    payload_json: str = ""  # includes before/after and execution parameters
    status: str = Field(default="executed", index=True)  # executed | undone | undo_expired | undo_failed
    created_at: datetime = Field(default_factory=utcnow, index=True)
    undo_deadline_at: Optional[datetime] = Field(default=None, index=True)
    undone_at: Optional[datetime] = None
    undone_by_user_id: Optional[int] = Field(default=None, foreign_key="user.id")
    undo_note: Optional[str] = None


class ShopWhatsappConfig(SQLModel, table=True):
    """Per-shop WhatsApp Cloud API connector configuration."""

    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True, unique=True)
    enabled: bool = Field(default=False, index=True)
    phone_number_id: Optional[str] = None
    business_account_id: Optional[str] = None
    verify_token: Optional[str] = None
    access_token: Optional[str] = None
    webhook_path_secret: Optional[str] = None
    created_by_user_id: Optional[int] = Field(default=None, foreign_key="user.id")
    updated_by_user_id: Optional[int] = Field(default=None, foreign_key="user.id")
    updated_at: datetime = Field(default_factory=utcnow)


class ShopWhatsappPendingAction(SQLModel, table=True):
    """Pending WhatsApp confirmation per sender within a shop."""

    id: Optional[int] = Field(default=None, primary_key=True)
    shop_id: int = Field(foreign_key="shop.id", index=True)
    sender_phone: str = Field(index=True, max_length=64)
    pending_payload_json: str = ""
    pending_question: str = ""
    created_at: datetime = Field(default_factory=utcnow, index=True)
    expires_at: datetime = Field(default_factory=utcnow, index=True)
