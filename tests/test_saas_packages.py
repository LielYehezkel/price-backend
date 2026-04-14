import unittest
from datetime import datetime, timedelta, timezone

from sqlmodel import Session, SQLModel, create_engine

from backend.models import Shop
from backend.routers.admin import apply_package_policy
from backend.services.monitor_checks import (
    _increment_shop_daily_quota,
    _shop_daily_quota_exceeded,
    _shop_interval_minutes,
    _shop_scan_cycle_due,
)


class TestSaasPackages(unittest.TestCase):
    def test_new_shop_defaults_to_free_policy(self) -> None:
        s = Shop(name="demo", owner_id=1)
        self.assertEqual(s.package_tier, "free")
        self.assertEqual(s.package_max_scan_runs_per_day, 10)
        self.assertEqual(s.package_max_scans_per_day_window, 1)
        self.assertEqual(s.package_min_interval_minutes, 1440)

    def test_interval_gating_uses_package_interval(self) -> None:
        now = datetime.now(timezone.utc)
        s = Shop(name="x", owner_id=1)
        s.package_min_interval_minutes = 720
        s.last_scan_cycle_at = now - timedelta(minutes=100)
        self.assertEqual(_shop_interval_minutes(s), 720)
        self.assertFalse(_shop_scan_cycle_due(s, now))
        s.last_scan_cycle_at = now - timedelta(minutes=721)
        self.assertTrue(_shop_scan_cycle_due(s, now))

    def test_daily_quota_stop_and_date_reset(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(engine)
        with Session(engine) as session:
            s = Shop(name="quota", owner_id=1)
            s.package_max_scan_runs_per_day = 2
            session.add(s)
            session.commit()
            session.refresh(s)
            now = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
            self.assertFalse(_shop_daily_quota_exceeded(session, s, now))
            _increment_shop_daily_quota(session, s, now)
            self.assertFalse(_shop_daily_quota_exceeded(session, s, now))
            _increment_shop_daily_quota(session, s, now)
            self.assertTrue(_shop_daily_quota_exceeded(session, s, now))
            tomorrow = now + timedelta(days=1)
            self.assertFalse(_shop_daily_quota_exceeded(session, s, tomorrow))

    def test_admin_tier_switch_applies_limits_immediately(self) -> None:
        s = Shop(name="tier", owner_id=1)
        apply_package_policy(s, "premium")
        self.assertEqual(s.package_tier, "premium")
        self.assertEqual(s.package_max_scan_runs_per_day, 250)
        self.assertEqual(s.package_max_scans_per_day_window, 3)
        self.assertEqual(s.package_min_interval_minutes, 480)
        self.assertEqual(s.check_interval_minutes, 480)


if __name__ == "__main__":
    unittest.main()
