import unittest

from backend.models import Product
from backend.services.ai_ops import parse_intent_rule_based, rank_product_candidates


class TestAiOps(unittest.TestCase):
    def test_parse_price_reduction_hebrew(self) -> None:
        intent = parse_intent_rule_based('תוריד את המחיר של סנטיאגו ארבעה מושבים ב50 ש"ח')
        self.assertEqual(intent.action, "reduce_price")
        self.assertEqual(intent.delta_amount, 50.0)
        self.assertIn("4", intent.product_query)

    def test_parse_price_reduction_without_preposition(self) -> None:
        intent = parse_intent_rule_based('תוזיל את סנטיאגו 4 מושבים 50 ש"ח')
        self.assertEqual(intent.action, "reduce_price")
        self.assertEqual(intent.delta_amount, 50.0)

    def test_parse_price_increase_hebrew(self) -> None:
        intent = parse_intent_rule_based('תעלה את המחיר של מערכת ישיבה ספרד ב50 ש"ח')
        self.assertEqual(intent.action, "increase_price")
        self.assertEqual(intent.delta_amount, 50.0)

    def test_parse_stock_command(self) -> None:
        intent = parse_intent_rule_based("תוריד את בורדו 4 מושבים מהמלאי")
        self.assertEqual(intent.action, "out_of_stock")
        self.assertIn("בורדו", intent.product_query)

    def test_parse_restore_stock_command(self) -> None:
        intent = parse_intent_rule_based("תחזיר את בורדו 4 מושבים למלאי")
        self.assertEqual(intent.action, "in_stock")
        self.assertIn("בורדו", intent.product_query)

    def test_rank_candidates_prefers_best_match(self) -> None:
        products = [
            Product(id=1, shop_id=1, name="ספת בורדו 4 מושבים", regular_price=3000),
            Product(id=2, shop_id=1, name="ספת בורדו 3 מושבים", regular_price=2500),
            Product(id=3, shop_id=1, name="כיסא גן סנטיאגו", regular_price=500),
        ]
        ranked = rank_product_candidates("בורדו ארבעה מושבים", products)
        self.assertGreater(len(ranked), 0)
        self.assertEqual(ranked[0].product_id, 1)

    def test_rank_candidates_understands_semantic_aliases(self) -> None:
        products = [
            Product(id=10, shop_id=1, name="מערכת ישיבה ספרד 4 מושבים", regular_price=4200),
            Product(id=11, shop_id=1, name="פינת אוכל ספרד", regular_price=2100),
        ]
        ranked = rank_product_candidates("פינת ישיבה ספרד", products)
        self.assertGreater(len(ranked), 0)
        self.assertEqual(ranked[0].product_id, 10)


if __name__ == "__main__":
    unittest.main()

