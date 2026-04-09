"""משיכת HTML מדפי חנויות — curl_cffi מחקה TLS/HTTP2 של Chrome (עוקף 403 מרבית ה-WAF)."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)

# אסטרטגיית משיכה לדומיין עם סלקטור שמור / מטא־תשובת resolve
FETCH_STRATEGY_HTTP = "http"
FETCH_STRATEGY_PLAYWRIGHT_PROXY = "playwright_proxy"


@dataclass(frozen=True)
class FetchedHtml:
    html: str
    strategy: str


def normalize_fetch_strategy(raw: str | None) -> str:
    s = (raw or "").strip().lower()
    if s == FETCH_STRATEGY_PLAYWRIGHT_PROXY:
        return FETCH_STRATEGY_PLAYWRIGHT_PROXY
    return FETCH_STRATEGY_HTTP


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
            "האתר חסם גם את המשיכה הרגילה וגם את ניסיון ה-proxy (Playwright). "
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
CHROME_WINDOWS_UA_124 = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
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

_PLAYWRIGHT_PROXY_BLOCKED_RESOURCE_TYPES = frozenset(
    {
        "image",
        "media",
        "font",
        "websocket",
        "manifest",
        "eventsource",
        "beacon",
    },
)

_PLAYWRIGHT_PROXY_BLOCKED_DOMAINS = frozenset(
    {
        "google-analytics.com",
        "googletagmanager.com",
        "googletagservices.com",
        "doubleclick.net",
        "google.com/ads",
        "connect.facebook.net",
        "facebook.com/tr",
        "hotjar.com",
        "clarity.ms",
        "segment.com",
        "mixpanel.com",
        "amplitude.com",
        "optimizely.com",
        "vwo.com",
        "convertexperiments.com",
        "intercom.io",
        "drift.com",
        "tawk.to",
        "livechatinc.com",
        "zendesk.com",
        "ads.twitter.com",
        "snap.licdn.com",
        "static.ads-twitter.com",
        "bat.bing.com",
        "cdn.segment.com",
        "cdn.heapanalytics.com",
    },
)

# Process-level circuit breaker: when Playwright browser binary is missing,
# avoid retrying expensive launch attempts on every blocked URL.
_PLAYWRIGHT_PROXY_DISABLED_REASON: str | None = None


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


def fetch_html_sync_no_fallback(url: str, timeout: float = 12.0) -> str:
    """Cheap path: normal HTTP/TLS fetch only (no Playwright fallback)."""
    return _fetch_html_primary_sync(url, timeout=timeout)


async def fetch_html_no_fallback(url: str, timeout: float = 12.0) -> str:
    """Cheap async path: normal fetch only (no Playwright fallback)."""
    return await _fetch_html_primary(url, timeout=timeout)


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


_PLAYWRIGHT_CF_INDICATORS = [
    "cf-browser-verification",
    "cf_chl_opt",
    "__cf_chl_f_tk",
    "ray id",
    "error 1009",
    "access denied",
    "cloudflare to restrict access",
    "checking if the site connection is secure",
]
_PLAYWRIGHT_CAPTCHA_INDICATORS = [
    "g-recaptcha",
    "h-captcha",
    "cf-turnstile",
    "are you a robot",
    "prove you're human",
    "i'm not a robot",
    "verify you are human",
    "challenge-platform",
]
_PLAYWRIGHT_GEO_INDICATORS = [
    "not available in your region",
    "not available in your country",
    "geo-restricted",
    "geographically restricted",
    "country is not supported",
]


def _detect_playwright_block(html: str | None) -> str | None:
    if not html:
        return "empty"
    low = html.lower()
    if any(ind in low for ind in _PLAYWRIGHT_CF_INDICATORS):
        return "cloudflare"
    # Captcha is treated as block only for explicit challenge markers.
    if any(ind in low for ind in _PLAYWRIGHT_CAPTCHA_INDICATORS):
        return "captcha"
    if any(ind in low for ind in _PLAYWRIGHT_GEO_INDICATORS):
        return "geo"
    return None


def _is_blocked_url_for_playwright_proxy(url: str) -> bool:
    low = (url or "").lower()
    return any(d in low for d in _PLAYWRIGHT_PROXY_BLOCKED_DOMAINS)


async def _playwright_proxy_route_interceptor(route) -> None:
    req = route.request
    if req.resource_type in _PLAYWRIGHT_PROXY_BLOCKED_RESOURCE_TYPES:
        await route.abort()
        return
    if _is_blocked_url_for_playwright_proxy(req.url):
        await route.abort()
        return
    await route.continue_()


def _root_host(host: str | None) -> str:
    h = (host or "").lower().strip()
    if h.startswith("www."):
        h = h[4:]
    return h


def _is_same_site(host: str | None, target_root: str) -> bool:
    h = _root_host(host)
    if not h or not target_root:
        return False
    return h == target_root or h.endswith("." + target_root)


_HEAVY_ASSET_RE = re.compile(
    r"\.(?:png|jpe?g|webp|gif|svg|ico|mp4|webm|mp3|woff2?|ttf|otf|map)(?:[\?#]|$)",
    re.I,
)


def _build_playwright_proxy_route_interceptor(target_url: str):
    from urllib.parse import urlparse

    target_root = _root_host(urlparse(target_url).hostname)

    async def _handler(route) -> None:
        req = route.request
        req_url = req.url or ""
        req_low = req_url.lower()
        req_host = urlparse(req_url).hostname

        if req.resource_type in _PLAYWRIGHT_PROXY_BLOCKED_RESOURCE_TYPES:
            await route.abort()
            return
        if _HEAVY_ASSET_RE.search(req_low):
            await route.abort()
            return
        if _is_blocked_url_for_playwright_proxy(req_url):
            await route.abort()
            return

        # Aggressive bandwidth saving on proxy path:
        # block third-party domains and keep same-site traffic only.
        if target_root and req_host and not _is_same_site(req_host, target_root):
            await route.abort()
            return

        await route.continue_()

    return _handler


def _is_blocking_error(e: Exception) -> bool:
    if isinstance(e, FetchHtmlError):
        if e.status_code in (403, 429):
            return True
        return True  # timeout/network/other fetch errors should trigger fallback
    if isinstance(e, httpx.HTTPError):
        return True
    return True


def _fallback_stage_timeout(timeout: float, *, proxy: bool) -> float:
    """Bound fallback time on blocked sites; proxy stage needs more headroom for slow CF pages."""
    cap = 22.0 if proxy else 12.0
    return min(float(timeout), cap)


# If goto hits TimeoutError but the document already has substantial HTML, return it anyway
# (domcontentloaded can be late while the tree is already parseable for selectors).
_PLAYWRIGHT_TIMEOUT_MIN_HTML_LEN = 25_000


def _playwright_proxy_settings_from_env() -> dict[str, str] | None:
    server = (os.getenv("PROXY_SERVER") or "").strip()
    username = (os.getenv("PROXY_USERNAME") or "").strip()
    password = (os.getenv("PROXY_PASSWORD") or "").strip()
    if not server:
        return None
    if not server.startswith("http://") and not server.startswith("https://"):
        server = f"http://{server}"
    out = {"server": server}
    if username:
        out["username"] = username
    if password:
        out["password"] = password
    return out


def _is_playwright_binary_missing_error(msg: str) -> bool:
    low = (msg or "").lower()
    return "executable doesn't exist" in low or "playwright install" in low


def _playwright_proxy_is_disabled() -> bool:
    return _PLAYWRIGHT_PROXY_DISABLED_REASON is not None


async def fetch_html_playwright_proxy(url: str, timeout: float = 10.0) -> str:
    """משיכת HTML דרך Playwright + פרוקסי חיצוני (PROXY_SERVER)."""
    global _PLAYWRIGHT_PROXY_DISABLED_REASON
    if _playwright_proxy_is_disabled():
        log.warning(
            "playwright_proxy skipped disabled_reason=%s url=%s",
            _PLAYWRIGHT_PROXY_DISABLED_REASON,
            url,
        )
        raise FetchHtmlError(
            "Playwright proxy disabled on this instance",
            final_reason=_PLAYWRIGHT_PROXY_DISABLED_REASON,
        )

    proxy_settings = _playwright_proxy_settings_from_env()
    if not proxy_settings:
        raise FetchHtmlError("PROXY_SERVER missing for playwright proxy fallback")

    stage = "playwright_proxy"
    log.warning(
        "fetch stage=%s starting proxy_server=%s timeout=%ss url=%s",
        stage,
        proxy_settings.get("server", "-"),
        timeout,
        url,
    )
    try:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright
    except Exception as ex:
        log.error("playwright_proxy import_failed url=%s err=%s", url, ex)
        if _is_playwright_binary_missing_error(str(ex)):
            _PLAYWRIGHT_PROXY_DISABLED_REASON = str(ex)
        raise FetchHtmlError(
            "Playwright is not available; install playwright and browser binaries",
            final_reason=str(ex),
        ) from ex

    html = ""
    browser = None
    context = None
    page = None
    try:
        async with async_playwright() as p:
            log.warning("playwright_proxy launching_chromium url=%s", url)
            browser = await p.chromium.launch(
                headless=True,
                proxy=proxy_settings,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-sync",
                    "--disable-translate",
                    "--disable-default-apps",
                    "--mute-audio",
                    "--no-first-run",
                ],
            )
            log.warning("playwright_proxy browser_launched url=%s", url)
            context = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=CHROME_WINDOWS_UA_124,
                locale="he-IL",
                timezone_id="Asia/Jerusalem",
                java_script_enabled=True,
            )
            log.warning("playwright_proxy context_ready url=%s", url)
            await context.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['he-IL','he','en-US','en'] });
                """,
            )
            page = await context.new_page()
            log.warning("playwright_proxy page_opened url=%s", url)
            await page.route("**/*", _build_playwright_proxy_route_interceptor(url))
            log.warning("playwright_proxy route_interceptor_enabled url=%s", url)
            nav_timeout_ms = max(int(timeout * 1000), 20_000)
            log.warning(
                "playwright_proxy goto_start url=%s wait_until=%s timeout_ms=%s",
                url,
                "domcontentloaded",
                nav_timeout_ms,
            )
            resp = None
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            except PlaywrightTimeoutError as ex:
                # Often the DOM is already large enough while Playwright waits on domcontentloaded.
                html_timeout = await page.content()
                timeout_preview = _safe_html_preview(html_timeout, 300)
                timeout_block_type = _detect_playwright_block(html_timeout)
                timeout_page_class = _classify_browserless_page(html_timeout)
                log.warning(
                    "proxy_response stage=%s goto_timeout url=%s html_len=%s page_class=%s block_type=%s preview=%s",
                    stage,
                    url,
                    len(html_timeout or ""),
                    timeout_page_class,
                    timeout_block_type or "-",
                    timeout_preview,
                )
                if len(html_timeout or "") >= _PLAYWRIGHT_TIMEOUT_MIN_HTML_LEN:
                    log.warning(
                        "playwright_proxy goto_timeout_recoverable url=%s html_len=%s (returning partial DOM)",
                        url,
                        len(html_timeout),
                    )
                    html = html_timeout
                    preview = timeout_preview
                    markers = _blocked_markers_found(html)
                    block_type = timeout_block_type
                    blocked = bool(block_type)
                    page_class = timeout_page_class
                    log.warning(
                        "proxy_response stage=%s status=%s html_len=%s page_class=%s blocked=%s block_type=%s markers=%s preview=%s",
                        stage,
                        None,
                        len(html or ""),
                        page_class,
                        blocked,
                        block_type or "-",
                        ",".join(markers) if markers else "-",
                        preview,
                    )
                    if blocked:
                        log.warning(
                            "playwright_proxy soft_block_after_timeout_returning_html url=%s block_type=%s",
                            url,
                            block_type or "-",
                        )
                    return html
                raise FetchHtmlError(
                    f"Playwright proxy navigation timeout after {nav_timeout_ms}ms",
                    status_code=408,
                ) from ex
            log.warning(
                "playwright_proxy goto_done url=%s status=%s",
                url,
                resp.status if resp else None,
            )
            status = resp.status if resp else None
            html = await page.content()
            preview = _safe_html_preview(html, 300)
            markers = _blocked_markers_found(html)
            block_type = _detect_playwright_block(html)
            blocked = bool(block_type)
            page_class = _classify_browserless_page(html)
            log.warning(
                "proxy_response stage=%s status=%s html_len=%s page_class=%s blocked=%s block_type=%s markers=%s preview=%s",
                stage,
                status,
                len(html or ""),
                page_class,
                blocked,
                block_type or "-",
                ",".join(markers) if markers else "-",
                preview,
            )
            if status in (403, 429, 503):
                raise FetchHtmlError(f"Playwright proxy HTTP {status}", status_code=status)
            # Soft-block signals on HTTP 200 are not fatal: return HTML and let extraction decide.
            if blocked and status != 200:
                raise FetchHtmlError("Playwright proxy blocked/empty html", status_code=403)
            if blocked and status == 200:
                log.warning(
                    "playwright_proxy soft_block_signal_on_200 returning_html url=%s block_type=%s",
                    url,
                    block_type or "-",
                )
            return html
    except FetchHtmlError:
        raise
    except Exception as ex:
        log.error("playwright_proxy failed url=%s err=%s", url, ex, exc_info=True)
        if _is_playwright_binary_missing_error(str(ex)):
            _PLAYWRIGHT_PROXY_DISABLED_REASON = str(ex)
            log.error("playwright_proxy disabled for process reason=%s", _PLAYWRIGHT_PROXY_DISABLED_REASON)
        raise FetchHtmlError(str(ex) or "playwright proxy error", status_code=408) from ex
    finally:
        # Always cleanup to avoid lingering traffic and memory.
        try:
            if page is not None:
                await page.close()
        except Exception:
            pass
        try:
            if context is not None:
                await context.close()
        except Exception:
            pass
        try:
            if browser is not None:
                await browser.close()
        except Exception:
            pass


def fetch_html_sync_with_fallback_meta(url: str, timeout: float = 45.0) -> FetchedHtml:
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
        return FetchedHtml(html=html, strategy=FETCH_STRATEGY_HTTP)
    except Exception as e:
        if not _is_blocking_error(e):
            raise
        log.warning("fallback triggered stage=normal url=%s err=%s", url, e)
        if not _playwright_proxy_settings_from_env():
            log.warning("playwright_proxy skipped PROXY_SERVER missing url=%s", url)
            if isinstance(e, Exception):
                raise e
        bl_proxy_timeout = _fallback_stage_timeout(timeout, proxy=True)
        log.warning(
            "fallback next_stage=playwright_proxy timeout=%ss url=%s",
            bl_proxy_timeout,
            url,
        )
        try:
            html = asyncio.run(fetch_html_playwright_proxy(url, timeout=bl_proxy_timeout))
            return FetchedHtml(html=html, strategy=FETCH_STRATEGY_PLAYWRIGHT_PROXY)
        except Exception as e3:
            log.error(
                "fetch failed all stages url=%s proxy_attempted=%s final_reason=%s",
                url,
                True,
                e3,
            )
            raise FetchHtmlError(
                "all_fetch_stages_blocked",
                status_code=403,
                api_status_code=424,
                final_reason=str(e3),
            ) from e3


async def fetch_html_with_fallback_meta(url: str, timeout: float = 45.0) -> FetchedHtml:
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
        return FetchedHtml(html=html, strategy=FETCH_STRATEGY_HTTP)
    except Exception as e:
        if not _is_blocking_error(e):
            raise
        log.warning("fallback triggered stage=normal url=%s err=%s", url, e)
        if not _playwright_proxy_settings_from_env():
            log.warning("playwright_proxy skipped PROXY_SERVER missing url=%s", url)
            if isinstance(e, Exception):
                raise e
        bl_proxy_timeout = _fallback_stage_timeout(timeout, proxy=True)
        log.warning(
            "fallback next_stage=playwright_proxy timeout=%ss url=%s",
            bl_proxy_timeout,
            url,
        )
        try:
            html = await fetch_html_playwright_proxy(url, timeout=bl_proxy_timeout)
            return FetchedHtml(html=html, strategy=FETCH_STRATEGY_PLAYWRIGHT_PROXY)
        except Exception as e3:
            log.error(
                "fetch failed all stages url=%s proxy_attempted=%s final_reason=%s",
                url,
                True,
                e3,
            )
            raise FetchHtmlError(
                "all_fetch_stages_blocked",
                status_code=403,
                api_status_code=424,
                final_reason=str(e3),
            ) from e3


def fetch_html_for_saved_strategy_sync(
    url: str,
    strategy: str | None,
    *,
    timeout_normal: float = 12.0,
) -> str:
    """נתיב ישיר לפי אסטרטגיה שמורה לדומיין (ללא fallback עד שמפילים החוצה)."""
    s = normalize_fetch_strategy(strategy)
    if s == FETCH_STRATEGY_HTTP:
        return fetch_html_sync_no_fallback(url, timeout=timeout_normal)
    t = _fallback_stage_timeout(45.0, proxy=True)
    return asyncio.run(fetch_html_playwright_proxy(url, timeout=t))


async def fetch_html_for_saved_strategy(
    url: str,
    strategy: str | None,
    *,
    timeout_normal: float = 12.0,
) -> str:
    s = normalize_fetch_strategy(strategy)
    if s == FETCH_STRATEGY_HTTP:
        return await fetch_html_no_fallback(url, timeout=timeout_normal)
    t = _fallback_stage_timeout(45.0, proxy=True)
    return await fetch_html_playwright_proxy(url, timeout=t)


def fetch_html_sync_with_fallback(url: str, timeout: float = 45.0) -> str:
    return fetch_html_sync_with_fallback_meta(url, timeout=timeout).html


async def fetch_html_with_fallback(url: str, timeout: float = 45.0) -> str:
    return (await fetch_html_with_fallback_meta(url, timeout=timeout)).html


def fetch_html_sync(url: str, timeout: float = 45.0) -> str:
    return fetch_html_sync_with_fallback(url, timeout=timeout)


async def fetch_html(url: str, timeout: float = 45.0) -> str:
    return await fetch_html_with_fallback(url, timeout=timeout)
