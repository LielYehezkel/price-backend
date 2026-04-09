"""משיכת HTML מדפי חנויות — curl_cffi מחקה TLS/HTTP2 של Chrome (עוקף 403 מרבית ה-WAF)."""

from __future__ import annotations

import asyncio
import logging
import os

import httpx

log = logging.getLogger(__name__)


class FetchHtmlError(Exception):
    """נכשלה משיכת HTML (חסימת אתר, רשת וכו') — לא לבלבל עם באג שרת."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        api_status_code: int | None = None,
        final_reason: str | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.api_status_code = api_status_code
        self.final_reason = final_reason


def format_fetch_error_hebrew(e: FetchHtmlError) -> str:
    if e.api_status_code and e.api_status_code != 502:
        return (
            "האתר חסם גם את המשיכה הרגילה וגם את ניסיונות ה-fallback (כולל proxy). "
            "זו חסימה בצד אתר היעד. נסו שוב מאוחר יותר או מהדפדפן המקומי."
        )
    """הודעה ללקוח API (502) — לא לזרוק 500 על חסימת WAF."""
    c = e.status_code
    if c == 403:
        return (
            "האתר דחה את הבקשה (חסימת בוטים/WAF מול שרת הענן). "
            "זו לא תקלה פנימית — נסו שוב מאוחר יותר או ודאו שהקישור נפתח בדפדפן."
        )
    if c == 429:
        return "יותר מדי בקשות לאתר היעד — נסו שוב בעוד דקה."
    if c is not None and 500 <= c < 600:
        return f"שרת היעד לא זמין זמנית (קוד {c}). נסו שוב מאוחר יותר."
    if c is not None and 400 <= c < 500:
        return f"לא ניתן להוריד את הדף (קוד {c})."
    return f"לא ניתן להתחבר לאתר: {e!s}"


def fetch_error_api_status(e: FetchHtmlError) -> int:
    return int(e.api_status_code or 502)


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

_BLOCK_MARKERS_STRONG = (
    "access denied",
    "verify you are human",
    "cf-challenge",
    "captcha",
    "attention required",
    "cloudflare ray id",
)

_BLOCK_MARKERS_WEAK = (
    "blocked",
    "forbidden",
    "request blocked",
)

_BLOCK_MARKERS_BROWSERLESS = (
    "access denied",
    "verify you are human",
    "cf-challenge",
    "captcha",
    "attention required",
    "cloudflare ray id",
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

    try:
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

            if r.status_code >= 400:
                raise FetchHtmlError(f"HTTP {r.status_code}", status_code=r.status_code)
            return r.text
    except FetchHtmlError:
        raise
    except httpx.RequestError as e:
        raise FetchHtmlError(str(e) or "network error") from e


def _fetch_html_primary_sync(url: str, timeout: float = 45.0) -> str:
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
                if r.status_code >= 400:
                    raise FetchHtmlError(f"HTTP {r.status_code}", status_code=r.status_code)
                return r.text
            except FetchHtmlError:
                raise
            except Exception as ex:
                log.warning("curl_cffi impersonate=%s failed: %s", impersonate, ex)

    return _fetch_via_httpx(url, timeout)


async def _fetch_html_primary(url: str, timeout: float = 45.0) -> str:
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
                        if r.status_code >= 400:
                            raise FetchHtmlError(f"HTTP {r.status_code}", status_code=r.status_code)
                        return r.text
                except FetchHtmlError:
                    raise
                except Exception as ex:
                    log.warning("curl_cffi async impersonate=%s failed: %s", impersonate, ex)
        except ImportError:
            pass

    return await asyncio.to_thread(_fetch_via_httpx, url, timeout)


def _is_blocked_html(html: str | None) -> bool:
    if not html or not html.strip():
        return True
    low = html.lower()
    if any(m in low for m in _BLOCK_MARKERS_STRONG):
        return True
    weak_hits = sum(1 for m in _BLOCK_MARKERS_WEAK if m in low)
    if weak_hits >= 2:
        return True
    # עמוד קצר מאוד + אינדיקציית חסימה חלשה = סביר שזה דף חסימה
    if len(low) < 300 and (weak_hits >= 1 or "error" in low):
        return True
    return False


def _blocked_markers_found(html: str | None) -> list[str]:
    if not html:
        return ["empty"]
    low = html.lower()
    hit: list[str] = []
    for m in _BLOCK_MARKERS_STRONG:
        if m in low:
            hit.append(m)
    weak = [m for m in _BLOCK_MARKERS_WEAK if m in low]
    if weak:
        hit.extend(weak)
    if len(low) < 300 and ("error" in low):
        hit.append("short_error")
    return hit


def _is_blocked_browserless_html(html: str | None) -> bool:
    """סיווג חסימה לנתיב browserless בלבד — לא להחמיר מדי כדי למנוע false positives."""
    if not html or not html.strip():
        return True
    low = html.lower()
    return any(m in low for m in _BLOCK_MARKERS_BROWSERLESS)


def _safe_html_preview(html: str | None, limit: int = 300) -> str:
    if not html:
        return ""
    compact = " ".join(str(html).split())
    return compact[:limit]


def _classify_browserless_page(html: str | None) -> str:
    if not html or not html.strip():
        return "empty_page"
    low = html.lower()
    if any(x in low for x in ("access denied", "forbidden", "not authorized")):
        return "access_denied_page"
    if any(
        x in low
        for x in (
            "cf-challenge",
            "verify you are human",
            "cloudflare",
            "captcha",
            "attention required",
            "security check",
        )
    ):
        return "challenge_page"
    if any(x in low for x in ("<html", "<body", "woocommerce", "product", "price", "add to cart")):
        return "normal_product_html"
    return "unknown_html"


def _browserless_payload_candidates(url: str) -> list[dict[str, object]]:
    return [
        {"url": url},
        {"url": url, "gotoOptions": {"waitUntil": "domcontentloaded"}},
        {"url": url, "gotoOptions": {"waitUntil": "networkidle2"}},
    ]


def _is_blocking_error(e: Exception) -> bool:
    if isinstance(e, FetchHtmlError):
        if e.status_code in (403, 429):
            return True
        return True  # timeout/network/other fetch errors should trigger fallback
    if isinstance(e, httpx.HTTPError):
        return True
    return True


def _fallback_stage_timeout(timeout: float, *, proxy: bool) -> float:
    """Bound fallback time on blocked sites so worker threads are freed quickly."""
    cap = 10.0 if proxy else 12.0
    return min(float(timeout), cap)


def fetch_html_browserless(url: str, use_proxy: bool = False, timeout: float = 45.0) -> str:
    token = (os.getenv("BROWSERLESS_TOKEN") or "").strip()
    if not token:
        raise FetchHtmlError("Browserless token missing")

    endpoint = f"https://production-sfo.browserless.io/content?token={token}"
    if use_proxy:
        # Browserless expects proxy flags in query params, not JSON body.
        endpoint = f"{endpoint}&proxy=residential"
    stage = "browserless_proxy" if use_proxy else "browserless"
    log.warning(
        "fetch stage=%s starting endpoint_has_proxy=%s timeout=%ss url=%s",
        stage,
        use_proxy,
        timeout,
        url,
    )
    last_err: FetchHtmlError | None = None
    payloads = _browserless_payload_candidates(url)
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True, trust_env=True) as client:
            for i, payload in enumerate(payloads, start=1):
                log.info(
                    "browserless request stage=%s attempt=%s proxy_requested=%s proxy_mode=%s url=%s",
                    stage,
                    i,
                    use_proxy,
                    "residential(query)" if use_proxy else "none",
                    url,
                )
                r = client.post(endpoint, json=payload)
                log.info("fetch stage=%s attempt=%s status=%s url=%s", stage, i, r.status_code, url)
                if r.status_code == 400:
                    body_sample = (r.text or "")[:220].replace("\n", " ").strip()
                    log.warning(
                        "fetch stage=%s attempt=%s bad_request body=%s",
                        stage,
                        i,
                        body_sample,
                    )
                    # Do not retry repeated invalid schema requests.
                    if "validation failed" in body_sample.lower() or "not allowed" in body_sample.lower():
                        raise FetchHtmlError(
                            "Browserless request validation failed",
                            status_code=400,
                            final_reason=body_sample,
                        )
                    last_err = FetchHtmlError("Browserless HTTP 400", status_code=400, final_reason=body_sample)
                    continue
                if r.status_code == 401 and use_proxy:
                    body_sample = (r.text or "")[:220].replace("\n", " ").strip()
                    raise FetchHtmlError(
                        "Browserless proxy may be unsupported by current plan",
                        status_code=401,
                        final_reason=body_sample,
                    )
                if r.status_code >= 400:
                    raise FetchHtmlError(f"Browserless HTTP {r.status_code}", status_code=r.status_code)
                html = r.text or ""
                markers = _blocked_markers_found(html)
                blocked = _is_blocked_browserless_html(html)
                classification = _classify_browserless_page(html)
                preview = _safe_html_preview(html, 300)
                log.info(
                    "fetch stage=%s attempt=%s html_len=%s blocked=%s page_class=%s markers=%s proxy_mode=%s url=%s",
                    stage,
                    i,
                    len(html),
                    blocked,
                    classification,
                    ",".join(markers) if markers else "-",
                    "residential(query)" if use_proxy else "none",
                    url,
                )
                log.info(
                    "fetch stage=%s attempt=%s html_preview=%s",
                    stage,
                    i,
                    preview,
                )
                if blocked:
                    last_err = FetchHtmlError(
                        "Browserless blocked/empty html",
                        status_code=403,
                        final_reason=f"stage={stage};len={len(html)};markers={markers}",
                    )
                    continue
                return html
    except FetchHtmlError:
        raise
    except httpx.RequestError as e:
        raise FetchHtmlError(str(e) or "browserless network error") from e
    if last_err is not None:
        raise last_err
    raise FetchHtmlError("Browserless failed with unknown reason", status_code=500)


def fetch_html_sync_with_fallback(url: str, timeout: float = 45.0) -> str:
    token = (os.getenv("BROWSERLESS_TOKEN") or "").strip()
    proxy_attempted = False
    try:
        log.info("fetch stage=normal url=%s", url)
        html = _fetch_html_primary_sync(url, timeout=timeout)
        markers = _blocked_markers_found(html)
        blocked = _is_blocked_html(html)
        log.info(
            "fetch stage=normal status=200 html_len=%s blocked=%s markers=%s url=%s",
            len(html or ""),
            blocked,
            ",".join(markers) if markers else "-",
            url,
        )
        return html
    except Exception as e:
        if not _is_blocking_error(e):
            raise
        log.warning("fallback triggered stage=normal url=%s err=%s", url, e)
        if not token:
            log.warning("BROWSERLESS_TOKEN missing; skipping browserless fallback url=%s", url)
            if isinstance(e, Exception):
                raise e
        bl_timeout = _fallback_stage_timeout(timeout, proxy=False)
        try:
            return fetch_html_browserless(url, use_proxy=False, timeout=bl_timeout)
        except Exception as e2:
            bl_proxy_timeout = _fallback_stage_timeout(timeout, proxy=True)
            log.warning(
                "fallback triggered stage=browserless url=%s err=%s next_stage=browserless_proxy timeout=%ss",
                url,
                e2,
                bl_proxy_timeout,
            )
            proxy_attempted = True
            try:
                return fetch_html_browserless(url, use_proxy=True, timeout=bl_proxy_timeout)
            except Exception as e3:
                log.error(
                    "fetch failed all stages url=%s proxy_attempted=%s final_reason=%s",
                    url,
                    proxy_attempted,
                    e3,
                )
                raise FetchHtmlError(
                    "all_fetch_stages_blocked",
                    status_code=403,
                    api_status_code=424,
                    final_reason=str(e3),
                ) from e3


async def fetch_html_with_fallback(url: str, timeout: float = 45.0) -> str:
    token = (os.getenv("BROWSERLESS_TOKEN") or "").strip()
    proxy_attempted = False
    try:
        log.info("fetch stage=normal url=%s", url)
        html = await _fetch_html_primary(url, timeout=timeout)
        markers = _blocked_markers_found(html)
        blocked = _is_blocked_html(html)
        log.info(
            "fetch stage=normal status=200 html_len=%s blocked=%s markers=%s url=%s",
            len(html or ""),
            blocked,
            ",".join(markers) if markers else "-",
            url,
        )
        return html
    except Exception as e:
        if not _is_blocking_error(e):
            raise
        log.warning("fallback triggered stage=normal url=%s err=%s", url, e)
        if not token:
            log.warning("BROWSERLESS_TOKEN missing; skipping browserless fallback url=%s", url)
            if isinstance(e, Exception):
                raise e
        bl_timeout = _fallback_stage_timeout(timeout, proxy=False)
        try:
            return await asyncio.to_thread(fetch_html_browserless, url, False, bl_timeout)
        except Exception as e2:
            bl_proxy_timeout = _fallback_stage_timeout(timeout, proxy=True)
            log.warning(
                "fallback triggered stage=browserless url=%s err=%s next_stage=browserless_proxy timeout=%ss",
                url,
                e2,
                bl_proxy_timeout,
            )
            proxy_attempted = True
            try:
                return await asyncio.to_thread(fetch_html_browserless, url, True, bl_proxy_timeout)
            except Exception as e3:
                log.error(
                    "fetch failed all stages url=%s proxy_attempted=%s final_reason=%s",
                    url,
                    proxy_attempted,
                    e3,
                )
                raise FetchHtmlError(
                    "all_fetch_stages_blocked",
                    status_code=403,
                    api_status_code=424,
                    final_reason=str(e3),
                ) from e3


def fetch_html_sync(url: str, timeout: float = 45.0) -> str:
    return fetch_html_sync_with_fallback(url, timeout=timeout)


async def fetch_html(url: str, timeout: float = 45.0) -> str:
    return await fetch_html_with_fallback(url, timeout=timeout)
