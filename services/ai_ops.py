from __future__ import annotations

import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

import httpx

from backend.config import settings
from backend.models import Product

_HE_NUM_WORDS = {
    "אחד": "1",
    "אחת": "1",
    "שניים": "2",
    "שתיים": "2",
    "שתי": "2",
    "שלוש": "3",
    "שלושה": "3",
    "ארבע": "4",
    "ארבעה": "4",
    "חמש": "5",
    "חמישה": "5",
    "שש": "6",
    "שישה": "6",
    "שבע": "7",
    "שבעה": "7",
    "שמונה": "8",
    "תשע": "9",
    "תשעה": "9",
    "עשר": "10",
    "עשרה": "10",
}


@dataclass
class ParsedIntent:
    action: str  # reduce_price | out_of_stock | in_stock | bulk_reduce_price | unknown
    product_query: str
    delta_amount: float | None = None
    currency_hint: str | None = None
    confidence: float = 0.0
    bulk_scope: str | None = None  # category | product_list
    target_category: str | None = None
    product_queries: list[str] | None = None


@dataclass
class ProductCandidate:
    product_id: int
    name: str
    score: float
    current_price: float | None
    woo_product_id: int | None


def _normalize_text(s: str) -> str:
    t = (s or "").lower().strip()
    for k, v in _HE_NUM_WORDS.items():
        t = re.sub(rf"\b{k}\b", v, t)
    t = re.sub(r'["\'`׳״]', " ", t)
    t = re.sub(r"[^a-z0-9א-ת\s\-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def parse_intent_rule_based(message: str) -> ParsedIntent:
    txt = _normalize_text(message)
    action = "unknown"
    has_restore_verbs = any(k in txt for k in ("תחזיר", "להחזיר", "החזר", "שים"))
    has_stock_target = any(k in txt for k in ("למלאי", "במלאי", "מלאי"))
    if has_restore_verbs and has_stock_target:
        action = "in_stock"

    has_stock_words = any(k in txt for k in ("מלאי", "אזל", "לא במלאי"))
    has_reduce_words = any(k in txt for k in ("תוריד", "להוריד", "הוצא"))
    if action == "unknown" and has_stock_words and has_reduce_words:
        action = "out_of_stock"
    if action == "unknown" and any(k in txt for k in ("תוריד את המחיר", "הורד מחיר", "להוריד מחיר", "פחות")):
        action = "reduce_price"
    if action == "reduce_price" and any(k in txt for k in ("כל ", "קטגור", "רשימה", "כמה מוצרים", "מוצרים:")):
        action = "bulk_reduce_price"

    delta: float | None = None
    nums = re.findall(r"(\d+(?:[.,]\d+)?)\s*(?:שח|ש\"ח|₪|nis|ils)?", txt)
    if action in ("reduce_price", "bulk_reduce_price") and nums:
        try:
            # Usually the last number in Hebrew command is the requested delta.
            delta = float(nums[-1].replace(",", "."))
        except ValueError:
            delta = None

    product_query = txt
    # Remove obvious command words to leave a cleaner query.
    product_query = re.sub(
        r"\b(תוריד|להוריד|תחזיר|להחזיר|החזר|שים|מחיר|מהמלאי|במלאי|מלאי|של|את|ב|שח|₪|nis|ils)\b",
        " ",
        product_query,
    )
    product_query = re.sub(r"\s+", " ", product_query).strip()
    if not product_query:
        product_query = txt

    conf = 0.4
    if action != "unknown":
        conf = 0.7
    if action == "reduce_price" and delta is not None:
        conf = 0.8
    bulk_scope: str | None = None
    target_category: str | None = None
    product_queries: list[str] | None = None
    if action == "bulk_reduce_price":
        if "קטגור" in txt:
            bulk_scope = "category"
            mcat = re.search(r"קטגור(?:יה|יית)?\s+([a-z0-9א-ת\s\-]+)", txt)
            if mcat:
                target_category = mcat.group(1).strip()
        else:
            bulk_scope = "product_list"
            parts = re.split(r",| ו", txt)
            cleaned: list[str] = []
            for p in parts:
                pp = re.sub(r"\b(תוריד|להוריד|מחיר|ב|שח|₪|nis|ils|מוצרים|רשימה)\b", " ", p).strip()
                pp = re.sub(r"\s+", " ", pp)
                if pp and len(pp) >= 3:
                    cleaned.append(pp)
            product_queries = cleaned or [product_query]
    return ParsedIntent(
        action=action,
        product_query=product_query,
        delta_amount=delta,
        currency_hint="ILS",
        confidence=conf,
        bulk_scope=bulk_scope,
        target_category=target_category,
        product_queries=product_queries,
    )


async def parse_intent_with_openai(message: str) -> ParsedIntent:
    if not settings.openai_api_key:
        return parse_intent_rule_based(message)
    sys = (
        "You parse Hebrew ecommerce admin commands into strict JSON."
        " Return only action/product_query/delta_amount/currency_hint/confidence."
        " action must be one of: reduce_price,out_of_stock,in_stock,bulk_reduce_price,unknown."
    )
    schema = {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["reduce_price", "out_of_stock", "in_stock", "bulk_reduce_price", "unknown"],
            },
            "product_query": {"type": "string"},
            "delta_amount": {"type": ["number", "null"]},
            "currency_hint": {"type": ["string", "null"]},
            "confidence": {"type": "number"},
            "bulk_scope": {"type": ["string", "null"], "enum": ["category", "product_list", None]},
            "target_category": {"type": ["string", "null"]},
            "product_queries": {"type": ["array", "null"], "items": {"type": "string"}},
        },
        "required": [
            "action",
            "product_query",
            "delta_amount",
            "currency_hint",
            "confidence",
            "bulk_scope",
            "target_category",
            "product_queries",
        ],
        "additionalProperties": False,
    }
    body = {
        "model": settings.ai_chat_model,
        "messages": [
            {"role": "system", "content": sys},
            {"role": "user", "content": message},
        ],
        "temperature": 0.1,
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "shop_task_intent", "schema": schema, "strict": True},
        },
    }
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {settings.openai_api_key}", "Content-Type": "application/json"},
                json=body,
            )
            r.raise_for_status()
            data = r.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "{}")
        )
        parsed = json.loads(content)
        return ParsedIntent(
            action=str(parsed.get("action") or "unknown"),
            product_query=str(parsed.get("product_query") or "").strip(),
            delta_amount=float(parsed["delta_amount"]) if parsed.get("delta_amount") is not None else None,
            currency_hint=(str(parsed.get("currency_hint")) if parsed.get("currency_hint") else None),
            confidence=float(parsed.get("confidence") or 0.0),
            bulk_scope=(str(parsed.get("bulk_scope")) if parsed.get("bulk_scope") else None),
            target_category=(str(parsed.get("target_category")) if parsed.get("target_category") else None),
            product_queries=(
                [str(x) for x in parsed.get("product_queries", []) if str(x).strip()]
                if parsed.get("product_queries") is not None
                else None
            ),
        )
    except Exception:
        return parse_intent_rule_based(message)


def rank_product_candidates(query: str, products: list[Product], *, top_k: int = 5) -> list[ProductCandidate]:
    q = _normalize_text(query)
    if not q:
        return []
    out: list[ProductCandidate] = []
    q_tokens = set(q.split())
    for p in products:
        n = _normalize_text(p.name or "")
        if not n:
            continue
        seq = SequenceMatcher(a=q, b=n).ratio()
        token_overlap = 0.0
        n_tokens = set(n.split())
        if q_tokens and n_tokens:
            token_overlap = len(q_tokens & n_tokens) / max(1.0, float(len(q_tokens)))
        contains_bonus = 0.0
        if q in n:
            contains_bonus = 0.2
        score = (0.65 * seq) + (0.25 * token_overlap) + contains_bonus
        out.append(
            ProductCandidate(
                product_id=p.id or 0,
                name=p.name,
                score=score,
                current_price=p.regular_price,
                woo_product_id=p.woo_product_id,
            ),
        )
    out.sort(key=lambda x: x.score, reverse=True)
    return out[:top_k]

