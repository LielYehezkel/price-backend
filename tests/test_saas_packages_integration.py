import unittest
from datetime import timedelta
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from backend.auth_utils import hash_password
from backend.db import engine
from backend.main import app
from backend.models import Shop, ShopScanQuotaDaily, User, utcnow
from backend.services.monitor_checks import _shop_daily_quota_exceeded, run_scheduled_checks


class TestSaasPackagesIntegration(unittest.TestCase):
    def _ensure_admin_user(self, email: str, password: str) -> None:
        with Session(engine) as session:
            u = session.exec(select(User).where(User.email == email)).first()
            if not u:
                session.add(
                    User(
                        email=email,
                        hashed_password=hash_password(password),
                        name="Integration Admin",
                        is_admin=True,
                    ),
                )
                session.commit()

    def test_login_patch_scheduler_quota_and_reset(self) -> None:
        email = f"admin-{uuid4().hex[:8]}@example.com"
        password = "123456"
        self._ensure_admin_user(email, password)

        with TestClient(app) as client:
            login = client.post("/api/auth/login", json={"email": email, "password": password})
            self.assertEqual(login.status_code, 200)
            token = login.json().get("access_token")
            self.assertTrue(token)
            headers = {"Authorization": f"Bearer {token}"}

            shop_name = f"shop-{uuid4().hex[:6]}"
            create_shop = client.post("/api/shops", headers=headers, json={"name": shop_name})
            self.assertEqual(create_shop.status_code, 200)
            shop_id = int(create_shop.json()["id"])

            patch = client.patch(
                f"/api/admin/shops/{shop_id}/package",
                headers=headers,
                json={"package_tier": "basic", "change_note": "integration test"},
            )
            self.assertEqual(patch.status_code, 200)
            self.assertEqual(patch.json()["package_tier"], "basic")

            audit = client.get(f"/api/admin/shops/{shop_id}/package-audit", headers=headers)
            self.assertEqual(audit.status_code, 200)
            self.assertGreaterEqual(len(audit.json()), 1)
            self.assertEqual(audit.json()[0]["new_tier"], "basic")

            with Session(engine) as session:
                s = session.get(Shop, shop_id)
                self.assertIsNotNone(s)
                # Fast-forward tiny quota for deterministic test speed.
                s.package_max_scan_runs_per_day = 1
                s.last_scan_cycle_at = None
                session.add(s)
                session.commit()
                session.refresh(s)

                _scans_1, shops_1 = run_scheduled_checks(session)
                self.assertGreaterEqual(shops_1, 1)
                self.assertTrue(_shop_daily_quota_exceeded(session, s, utcnow()))
                today = utcnow().strftime("%Y-%m-%d")
                q1 = session.exec(
                    select(ShopScanQuotaDaily).where(
                        ShopScanQuotaDaily.shop_id == shop_id,
                        ShopScanQuotaDaily.bucket_date == today,
                    ),
                ).first()
                self.assertIsNotNone(q1)
                used_after_first = int(q1.runs_count or 0)

                # Should be skipped by quota now.
                _scans_2, shops_2 = run_scheduled_checks(session)
                self.assertGreaterEqual(shops_2, 0)
                q2 = session.exec(
                    select(ShopScanQuotaDaily).where(
                        ShopScanQuotaDaily.shop_id == shop_id,
                        ShopScanQuotaDaily.bucket_date == today,
                    ),
                ).first()
                self.assertIsNotNone(q2)
                self.assertEqual(int(q2.runs_count or 0), used_after_first)

                tomorrow = utcnow() + timedelta(days=1)
                self.assertFalse(_shop_daily_quota_exceeded(session, s, tomorrow))


if __name__ == "__main__":
    unittest.main()
