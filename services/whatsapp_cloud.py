from __future__ import annotations

from typing import Any

import httpx


META_GRAPH_BASE = "https://graph.facebook.com/v22.0"


def _auth_headers(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token.strip()}"}


def validate_phone_number_id(access_token: str, phone_number_id: str) -> dict[str, Any]:
    url = f"{META_GRAPH_BASE}/{phone_number_id.strip()}"
    params = {"fields": "id,display_phone_number,verified_name,quality_rating"}
    with httpx.Client(timeout=25.0, follow_redirects=True) as client:
        r = client.get(url, headers=_auth_headers(access_token), params=params)
        if r.status_code >= 400:
            raise RuntimeError(f"Meta validation failed ({r.status_code}): {r.text[:300]}")
        data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("Meta validation returned invalid response")
    return data


def send_test_text_message(
    access_token: str,
    phone_number_id: str,
    to_phone_e164: str,
    text: str,
) -> dict[str, Any]:
    url = f"{META_GRAPH_BASE}/{phone_number_id.strip()}/messages"
    body = {
        "messaging_product": "whatsapp",
        "to": to_phone_e164.strip(),
        "type": "text",
        "text": {"preview_url": False, "body": text.strip()},
    }
    with httpx.Client(timeout=25.0, follow_redirects=True) as client:
        r = client.post(url, headers=_auth_headers(access_token), json=body)
        if r.status_code >= 400:
            raise RuntimeError(f"Meta send failed ({r.status_code}): {r.text[:300]}")
        data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("Meta send returned invalid response")
    return data


def send_interactive_confirm_buttons(
    access_token: str,
    phone_number_id: str,
    to_phone_e164: str,
    body_text: str,
    yes_id: str = "confirm_yes",
    no_id: str = "confirm_no",
) -> dict[str, Any]:
    url = f"{META_GRAPH_BASE}/{phone_number_id.strip()}/messages"
    body = {
        "messaging_product": "whatsapp",
        "to": to_phone_e164.strip(),
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text.strip()[:1024]},
            "action": {
                "buttons": [
                    {"type": "reply", "reply": {"id": yes_id, "title": "כן, לבצע"}},
                    {"type": "reply", "reply": {"id": no_id, "title": "לא, לבטל"}},
                ],
            },
        },
    }
    with httpx.Client(timeout=25.0, follow_redirects=True) as client:
        r = client.post(url, headers=_auth_headers(access_token), json=body)
        if r.status_code >= 400:
            raise RuntimeError(f"Meta interactive send failed ({r.status_code}): {r.text[:300]}")
        data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("Meta interactive send returned invalid response")
    return data
