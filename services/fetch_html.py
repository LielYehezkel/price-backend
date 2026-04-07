"""משיכת HTML מדפי חנויות — curl_cffi מחקה TLS/HTTP2 של Chrome (עוקף 403 מרבית ה-WAF)."""

from __future__ import annotations

import asyncio
import logging

import httpx

log = logging.getLogger(__name__)

try:
    from curl_cffi import requests as curl_requests

    _CURL_CFFI = True
except ImportError:
    curl_requests = None  # type: ignore[assignment]
    _CURL_CFFI = False

# Chrome אמיתי — גיבוי כשאין curl_cffi
CHROME_WINDOWS_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def _origin_root(url: str) -> str:
    from urllib.parse import urlparse

    p = urlparse(url)
    if not p.scheme or not p.netloc:
        return url
    return f"{p.scheme}://{p.netloc}/"


def _curl_browser_headers(url: str) -> dict[str, str]:
    """כותרות דמויות דפדפן — חשוב ל־WAF כשמשתמשים ב־curl_cffi."""
    root = _origin_root(url)
    return {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": root,
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
    }


def _httpx_fallback_headers(url: str, *, full_sec_fetch: bool = True) -> dict[str, str]:
    from urllib.parse import urlparse

    p = urlparse(url)
    ref_root = f"{p.scheme}://{p.netloc}/" if p.scheme and p.netloc else url
    h: dict[str, str] = {
        "User-Agent": CHROME_WINDOWS_UA,
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "Referer": ref_root,
    }
    if full_sec_fetch:
        h.update(
            {
                "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
            },
        )
    return h


def _fetch_via_httpx(url: str, timeout: float) -> str:
    from urllib.parse import urlparse

    def origin_root(u: str) -> str | None:
        p = urlparse(u)
        if not p.scheme or not p.netloc:
            return None
        return f"{p.scheme}://{p.netloc}/"

    root = origin_root(url)
    if not root:
        raise ValueError("כתובת לא תקינה")

    def is_not_home(u: str, r: str) -> bool:
        return u.rstrip("/") != r.rstrip("/")

    with httpx.Client(follow_redirects=True, timeout=timeout, trust_env=True) as client:

        def attempt(full_sec: bool) -> httpx.Response:
            return client.get(url, headers=_httpx_fallback_headers(url, full_sec_fetch=full_sec))

        r = attempt(True)
        if r.status_code == 403 and is_not_home(url, root):
            client.get(root, headers=_httpx_fallback_headers(root, full_sec_fetch=True))
            r = attempt(True)

        if r.status_code == 403:
            if is_not_home(url, root):
                client.get(root, headers=_httpx_fallback_headers(root, full_sec_fetch=False))
            r = attempt(False)

        if r.status_code == 403:
            r = attempt(False)

        # ניסיון אחרון: בלי Sec-Fetch-* — חלק מה־WAF דוחים כותרות Client Hints מזויפות
        if r.status_code == 403:
            r = client.get(
                url,
                headers={
                    "User-Agent": CHROME_WINDOWS_UA,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Referer": root,
                    "Cache-Control": "max-age=0",
                },
            )

        r.raise_for_status()
        return r.text


def fetch_html_sync(url: str, timeout: float = 45.0) -> str:
    """
    משיכה סינכרונית. עדיפות: curl_cffi (TLS כמו Chrome) — פותר 403 מול אתרי מסחר רבים.
    """
    if _CURL_CFFI and curl_requests is not None:
        curl_headers = _curl_browser_headers(url)
        # כמה פרופילי TLS — חלק מהאתרים אגרסיביים מול דאטה-סנטר
        for impersonate in ("chrome124", "chrome131", "chrome120", "chrome116", "chrome110", "chrome"):
            try:
                r = curl_requests.get(
                    url,
                    impersonate=impersonate,
                    timeout=timeout,
                    allow_redirects=True,
                    headers=curl_headers,
                )
                r.raise_for_status()
                return r.text
            except Exception as ex:
                log.warning("curl_cffi impersonate=%s failed: %s", impersonate, ex)

    return _fetch_via_httpx(url, timeout)


async def fetch_html(url: str, timeout: float = 45.0) -> str:
    if _CURL_CFFI:
        try:
            from curl_cffi.requests import AsyncSession

            curl_headers = _curl_browser_headers(url)
            for impersonate in ("chrome124", "chrome131", "chrome120", "chrome116", "chrome110", "chrome"):
                try:
                    async with AsyncSession() as session:
                        r = await session.get(
                            url,
                            impersonate=impersonate,
                            timeout=timeout,
                            allow_redirects=True,
                            headers=curl_headers,
                        )
                        r.raise_for_status()
                        return r.text
                except Exception as ex:
                    log.warning("curl_cffi async impersonate=%s failed: %s", impersonate, ex)
        except ImportError:
            pass

    return await asyncio.to_thread(_fetch_via_httpx, url, timeout)
