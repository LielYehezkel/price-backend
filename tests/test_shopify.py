"""Shopify helpers, connector, and API smoke tests (HTTP mocked where needed)."""

from __future__ import annotations

import unittest
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient

from backend.db import engine
from backend.main import app
from backend.models import Shop
from backend.services import shopify_sync
from backend.services import store_connector
from backend.services.sales_notifications import normalize_order_sale_payload
from sqlmodel import Session


class TestShopifyHelpers(unittest.TestCase):
    def test_normalize_shopify_domain(self) -> None:
        self.assertEqual(
            shopify_sync.normalize_shopify_domain("https://My-Shop.myshopify.com/"),
            "my-shop.myshopify.com",
        )

    def test_variant_to_woo_like_row_sale(self) -> None:
        row = shopify_sync.variant_to_woo_like_row(
            {"id": 1, "price": "10.00", "compare_at_price": "20.00", "sku": "x"},
        )
        self.assertTrue(row["on_sale"])
        self.assertAlmostEqual(float(row["regular_price"] or 0), 20.0)
        self.assertAlmostEqual(float(row["sale_price"] or 0), 10.0)

    def test_normalize_order_sale_payload_shopify(self) -> None:
        order = {
            "id": 99,
            "financial_status": "paid",
            "total_price": "100.00",
            "currency": "ILS",
            "line_items": [{"title": "Widget", "quantity": 1, "price": "100.00"}],
        }
        w = normalize_order_sale_payload(order)
        self.assertEqual(w["status"], "processing")
        self.assertEqual(w["line_items"][0].get("name"), "Widget")


class TestShopifyApi(unittest.TestCase):
    def _user_token(self, client: TestClient) -> str:
        email = f"u-{uuid4().hex[:8]}@example.com"
        password = "secret123"
        r = client.post("/api/auth/register", json={"email": email, "password": password, "name": "T"})
        self.assertEqual(r.status_code, 200)
        return r.json()["access_token"]

    def test_create_shopify_shop_and_save_config_mocked(self) -> None:
        with TestClient(app) as client:
            token = self._user_token(client)
            headers = {"Authorization": f"Bearer {token}"}
            name = f"s-{uuid4().hex[:6]}"
            cr = client.post("/api/shops", headers=headers, json={"name": name, "store_platform": "shopify"})
            self.assertEqual(cr.status_code, 200)
            body = cr.json()
            self.assertEqual(body.get("store_platform"), "shopify")
            shop_id = int(body["id"])

            with (
                patch.object(shopify_sync, "verify_shop_credentials", return_value={"currency": "ILS"}),
                patch.object(shopify_sync, "fetch_shop_currency_code", return_value="ILS"),
            ):
                sr = client.post(
                    f"/api/shops/{shop_id}/shopify",
                    headers=headers,
                    json={
                        "shop_domain": "test-shop.myshopify.com",
                        "admin_access_token": "shpat_test_token",
                        "client_secret": "shpss_test_client",
                    },
                )
            self.assertEqual(sr.status_code, 200)
            self.assertIn("shopify_webhook_secret", sr.json())
            with Session(engine) as session:
                s = session.get(Shop, shop_id)
                self.assertIsNotNone(s)
                assert s is not None
                self.assertEqual(s.store_platform, "shopify")
                self.assertEqual(s.shopify_shop_domain, "test-shop.myshopify.com")
                self.assertEqual(s.shopify_client_secret, "shpss_test_client")


class TestShopifyWebhookHmac(unittest.TestCase):
    def test_webhook_hmac_roundtrip(self) -> None:
        secret = "testsecret"
        raw = b'{"id":1,"financial_status":"paid","line_items":[]}'
        import base64
        import hmac
        import hashlib

        digest = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
        hdr = base64.b64encode(digest).decode("utf-8")
        self.assertTrue(shopify_sync.verify_webhook_hmac(raw, hdr, secret))


class TestStoreConnectorDispatch(unittest.TestCase):
    def test_store_platform_default(self) -> None:
        s = Shop(name="x", owner_id=1, store_platform="wordpress")
        self.assertEqual(store_connector.store_platform(s), "wordpress")

    def test_product_has_store_link_shopify(self) -> None:
        s = Shop(name="x", owner_id=1, store_platform="shopify")
        from backend.models import Product

        p = Product(shop_id=1, shopify_variant_id=123)
        self.assertTrue(store_connector.product_has_store_link(s, p))
        p2 = Product(shop_id=1)
        self.assertFalse(store_connector.product_has_store_link(s, p2))
