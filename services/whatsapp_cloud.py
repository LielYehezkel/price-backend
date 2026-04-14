from __future__ import annotations

from typing import Any

import httpx


META_GRAPH_BASE = "https://graph.facebook.com/v22.0"


class MetaAuthError(RuntimeError):
    pass


def _raise_meta_error(prefix: str, response: httpx.Response) -> None:
    body_snippet = response.text[:300]
    msg = f"{prefix} ({response.status_code}): {body_snippet}"
    try:
        data = response.json()
    except Exception:
        data = None
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            code = err.get("code")
            subcode = err.get("error_subcode")
            err_msg = str(err.get("message") or "")
            if response.status_code in (401, 403) or str(code) == "190":
                raise MetaAuthError(
                    f"{prefix} ({response.status_code}) code={code} subcode={subcode}: {err_msg}",
                )
            msg = f"{prefix} ({response.status_code}) code={code} subcode={subcode}: {err_msg}"
    raise RuntimeError(msg)


def _auth_headers(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token.strip()}"}


def validate_phone_number_id(access_token: str, phone_number_id: str) -> dict[str, Any]:
    url = f"{META_GRAPH_BASE}/{phone_number_id.strip()}"
    params = {"fields": "id,display_phone_number,verified_name,quality_rating"}
    with httpx.Client(timeout=25.0, follow_redirects=True) as client:
        r = client.get(url, headers=_auth_headers(access_token), params=params)
        if r.status_code >= 400:
            _raise_meta_error("Meta validation failed", r)
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
            _raise_meta_error("Meta send failed", r)
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
            _raise_meta_error("Meta interactive send failed", r)
        data = r.json()
    if not isinstance(data, dict):
        raise RuntimeError("Meta interactive send returned invalid response")
    return data
