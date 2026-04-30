from __future__ import annotations

import asyncio
import inspect
import random
import re
import time
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import httpx

from app.core.logging import get_logger
from app.scraping.html_adapter import parse_html_document

if TYPE_CHECKING:
    from app.core.config import Settings, SiteConfig
    from app.scraping.site_profiles import SiteProfile

try:
    from playwright.async_api import (
        Browser,
        BrowserContext,
        Playwright,
        TimeoutError as PlaywrightTimeoutError,
        async_playwright,
    )
except ImportError:  # pragma: no cover - optional dependency path for local imports
    Browser = None
    BrowserContext = None
    Playwright = None
    PlaywrightTimeoutError = TimeoutError
    async_playwright = None

PageKind = Literal["list", "detail"]
Mode = Literal["http", "browser", "ai"]
ModeAlias = Literal["http", "browser", "ai", "dynamic", "stealth"]

MODE_ALIASES: dict[str, Mode] = {
    "http": "http",
    "browser": "browser",
    "ai": "ai",
    "dynamic": "browser",
    "stealth": "ai",
}

logger = get_logger("scraping_fetchers")

DETAIL_REVEAL_TEXT_RE = re.compile(
    r"(show|contact|contacts|phone|call|email|e-mail|"
    r"показ|контакт|контакти|контакты|телефон|телефоны|"
    r"подзвон|дзвон|зателефон|email|пошта|почта)",
    flags=re.I,
)
GENERIC_DETAIL_CLICK_SELECTORS: tuple[str, ...] = (
    "button",
    "[role='button']",
    "[data-testid*='phone']",
    "[data-testid*='contact']",
    "[class*='phone'] button",
    "[class*='contact'] button",
    "button[class*='phone']",
    "button[class*='contact']",
    "button[class*='show']",
)


@dataclass(slots=True)
class FetchResult:
    page: Any | None
    url: str
    status_code: int | None = None
    error_class: str | None = None


class StrategyBlockedError(RuntimeError):
    def __init__(
        self,
        *,
        site_name: str,
        mode: str,
        url: str,
        reason: str,
        classification: str = "blocked",
    ) -> None:
        super().__init__(f"{site_name}:{mode}:{reason} ({url})")
        self.site_name = site_name
        self.mode = mode
        self.url = url
        self.reason = reason
        self.classification = classification


class SessionClient(AbstractAsyncContextManager["SessionClient"]):
    mode: Mode = "http"

    def __init__(
        self,
        *,
        settings: Settings,
        site_config: SiteConfig,
        site_profile: SiteProfile,
    ) -> None:
        self.settings = settings
        self.site_config = site_config
        self.site_profile = site_profile
        self._rate_limit_lock = asyncio.Lock()
        self._last_request_at = 0.0
        self._proxy_cursor = 0
        self._proxy_failures: dict[str, int] = {}
        self._rng = random.Random(site_config.name)

    async def __aenter__(self) -> "SessionClient":
        await self._open()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool | None:
        await self._close()
        return None

    async def fetch(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str = "",
    ) -> FetchResult:
        attempts = max(1, int(getattr(self.settings, "scrape_retry_count", 1)))
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            await self._respect_rate_limit()
            proxy = self._select_proxy()

            try:
                result = await self._execute_request(
                    url,
                    page_kind=page_kind,
                    wait_selector=wait_selector,
                    proxy=proxy,
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                classification = self._classify_exception(exc)
                if proxy is not None:
                    self._record_proxy_failure(proxy)

                logger.warning(
                    "Scrape request failed",
                    site=self.site_config.name,
                    mode=self.mode,
                    page_kind=page_kind,
                    url=url,
                    attempt=attempt,
                    attempts=attempts,
                    proxy=proxy,
                    error=str(exc),
                    classification=classification,
                )
                if attempt >= attempts or classification not in {"timeout", "network", "transient"}:
                    raise
                await asyncio.sleep(self._backoff_delay(attempt))
                continue

            if proxy is not None:
                self._record_proxy_success(proxy)

            result.error_class = result.error_class or self._classify_status(result.status_code)
            if attempt < attempts and result.status_code in self._retryable_status_codes():
                logger.warning(
                    "Retrying scrape request after retryable status",
                    site=self.site_config.name,
                    mode=self.mode,
                    page_kind=page_kind,
                    url=result.url,
                    status_code=result.status_code,
                    attempt=attempt,
                    attempts=attempts,
                    proxy=proxy,
                    classification=result.error_class,
                )
                if proxy is not None:
                    self._record_proxy_failure(proxy)
                await asyncio.sleep(self._backoff_delay(attempt))
                continue

            return result

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"{self.site_config.name}:{self.mode}:request_failed_without_error")

    async def _open(self) -> None:
        return None

    async def _close(self) -> None:
        return None

    async def _execute_request(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str,
        proxy: str | None,
    ) -> FetchResult:
        raise NotImplementedError

    async def _respect_rate_limit(self) -> None:
        min_delay = float(getattr(self.settings, "scrape_delay_min_seconds", 0.0))
        max_delay = float(getattr(self.settings, "scrape_delay_max_seconds", min_delay))
        delay_floor = max(0.0, min(min_delay, max_delay))
        delay_ceiling = max(delay_floor, max_delay)
        target_delay = self._rng.uniform(delay_floor, delay_ceiling) if delay_ceiling > 0 else 0.0

        async with self._rate_limit_lock:
            elapsed = time.monotonic() - self._last_request_at
            remaining = target_delay - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
            self._last_request_at = time.monotonic()

    def _timeout_seconds(self) -> float:
        return float(self.site_config.timeout or getattr(self.settings, "scrape_timeout_seconds", 30.0))

    def _verify_ssl(self) -> bool:
        global_verify = bool(getattr(self.settings, "scrape_verify_ssl", True))
        return global_verify and bool(self.site_config.verify_ssl)

    def _backoff_delay(self, attempt: int) -> float:
        base = float(getattr(self.settings, "scrape_backoff_base_seconds", 1.5))
        cap = float(getattr(self.settings, "scrape_backoff_cap_seconds", 12.0))
        return min(cap, base * (2 ** max(0, attempt - 1)))

    def _retryable_status_codes(self) -> set[int]:
        return {408, 425, 429, 500, 502, 503, 504}

    def _classify_status(self, status_code: int | None) -> str | None:
        if status_code is None:
            return None
        if status_code in {401, 403, 407, 429}:
            return "ban"
        if status_code in {408, 425, 500, 502, 503, 504}:
            return "timeout"
        if 400 <= status_code < 500:
            return "client_error"
        if status_code >= 500:
            return "server_error"
        return None

    def _classify_exception(self, exc: Exception) -> str:
        if isinstance(exc, (httpx.TimeoutException, PlaywrightTimeoutError, TimeoutError)):
            return "timeout"
        if isinstance(exc, (httpx.NetworkError, httpx.ProtocolError, httpx.TransportError)):
            return "network"
        return "transient" if isinstance(exc, RuntimeError) and "temporar" in str(exc).lower() else "fatal"

    def _headers(self) -> dict[str, str]:
        user_agents = list(getattr(self.settings, "user_agents", []) or [])
        if not user_agents:
            fallback = getattr(self.settings, "user_agent", "")
            user_agents = [fallback] if fallback else []
        selected_user_agent = self._rng.choice(user_agents) if user_agents else "Mozilla/5.0"

        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,uk;q=0.8,bg;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": selected_user_agent,
        }

    def _select_proxy(self) -> str | None:
        if not getattr(self.settings, "proxy_enabled", False):
            return None

        pool = list(getattr(self.settings, "proxy_pool", []))
        if not pool:
            return None

        strategy = getattr(self.settings, "proxy_rotation_strategy", "random")
        if strategy == "round_robin":
            proxy = pool[self._proxy_cursor % len(pool)]
            self._proxy_cursor += 1
            return proxy
        if strategy == "failover":
            return min(pool, key=lambda value: self._proxy_failures.get(value, 0))
        return self._rng.choice(pool)

    def _record_proxy_failure(self, proxy: str) -> None:
        self._proxy_failures[proxy] = self._proxy_failures.get(proxy, 0) + 1

    def _record_proxy_success(self, proxy: str) -> None:
        if proxy in self._proxy_failures:
            self._proxy_failures[proxy] = max(0, self._proxy_failures[proxy] - 1)


class HttpxSessionClient(SessionClient):
    mode: Mode = "http"

    def __init__(
        self,
        *,
        settings: Settings,
        site_config: SiteConfig,
        site_profile: SiteProfile,
    ) -> None:
        super().__init__(settings=settings, site_config=site_config, site_profile=site_profile)
        self._clients: dict[str, httpx.AsyncClient] = {}

    async def _close(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()

    async def _execute_request(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str,
        proxy: str | None,
    ) -> FetchResult:
        del page_kind, wait_selector
        client = self._client_for(proxy)
        response = await client.get(url, headers=self._headers())
        page = parse_html_document(
            response.text or "",
            url=str(response.url),
            status_code=response.status_code,
        )
        return FetchResult(
            page=page,
            url=str(response.url),
            status_code=response.status_code,
        )

    def _client_for(self, proxy: str | None) -> httpx.AsyncClient:
        key = proxy or "__direct__"
        if key in self._clients:
            return self._clients[key]

        timeout = httpx.Timeout(self._timeout_seconds())
        limits = httpx.Limits(
            max_connections=max(1, int(getattr(self.settings, "http_max_connections", 30))),
            max_keepalive_connections=max(1, int(getattr(self.settings, "http_max_keepalive_connections", 10))),
        )
        client = httpx.AsyncClient(
            follow_redirects=bool(getattr(self.settings, "scrape_follow_redirects", True)),
            headers=self._headers(),
            limits=limits,
            proxy=proxy,
            timeout=timeout,
            verify=self._verify_ssl(),
        )
        self._clients[key] = client
        return client


class PlaywrightSessionClient(SessionClient):
    mode: Mode = "browser"

    def __init__(
        self,
        *,
        settings: Settings,
        site_config: SiteConfig,
        site_profile: SiteProfile,
    ) -> None:
        super().__init__(settings=settings, site_config=site_config, site_profile=site_profile)
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None

    async def _open(self) -> None:
        if async_playwright is None:
            raise RuntimeError("Playwright is not installed. Browser strategy is unavailable.")

        self._playwright = await async_playwright().start()
        browser_name = getattr(self.settings, "playwright_browser", "chromium")
        browser_factory = getattr(self._playwright, browser_name, None)
        if browser_factory is None:
            raise RuntimeError(f"Unsupported Playwright browser '{browser_name}'.")

        proxy = self._select_proxy()
        launch_kwargs: dict[str, object] = {
            "headless": bool(getattr(self.settings, "browser_headless", True)),
        }
        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}
        if bool(getattr(self.settings, "browser_stealth", True)):
            launch_kwargs["args"] = ["--disable-blink-features=AutomationControlled"]

        self._browser = await browser_factory.launch(**launch_kwargs)
        self._context = await self._browser.new_context(
            extra_http_headers={"Accept-Language": self._headers()["Accept-Language"]},
            ignore_https_errors=not self._verify_ssl(),
            locale="en-US",
            user_agent=self._headers()["User-Agent"],
        )

        if bool(getattr(self.settings, "browser_stealth", True)):
            await self._context.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = window.chrome || { runtime: {} };
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
                """
            )

    async def _close(self) -> None:
        if self._context is not None:
            await self._context.close()
            self._context = None
        if self._browser is not None:
            await self._browser.close()
            self._browser = None
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

    async def _execute_request(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str,
        proxy: str | None,
    ) -> FetchResult:
        del proxy
        if self._context is None:
            raise RuntimeError("Playwright browser context is not initialized.")

        page = await self._context.new_page()
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=self._browser_timeout_ms())
            if wait_selector:
                try:
                    await page.wait_for_selector(
                        wait_selector,
                        state="attached",
                        timeout=self._browser_timeout_ms(),
                    )
                except PlaywrightTimeoutError:
                    logger.info(
                        "Browser wait selector timed out; returning current DOM snapshot",
                        site=self.site_config.name,
                        mode=self.mode,
                        url=url,
                        wait_selector=wait_selector,
                    )
            if page_kind == "detail":
                await self._prepare_detail_page(page)
            markup = await page.content()
            final_url = page.url or url
            status_code = response.status if response is not None else None
            return FetchResult(
                page=parse_html_document(markup, url=final_url, status_code=status_code),
                url=final_url,
                status_code=status_code,
            )
        finally:
            await page.close()

    def _browser_timeout_ms(self) -> int:
        return max(1000, int(self._timeout_seconds() * 1000))

    async def _prepare_detail_page(self, page: Any) -> None:
        clicked = await self._click_detail_reveal_targets(page)
        if clicked:
            try:
                await page.wait_for_load_state("networkidle", timeout=min(3000, self._browser_timeout_ms()))
            except PlaywrightTimeoutError:
                logger.info(
                    "Detail page did not reach network idle after contact reveal click",
                    site=self.site_config.name,
                    mode=self.mode,
                    clicks=clicked,
                )

    async def _click_detail_reveal_targets(self, page: Any) -> int:
        selectors = tuple(
            dict.fromkeys(
                [
                    *self.site_profile.detail_click_selectors,
                    *GENERIC_DETAIL_CLICK_SELECTORS,
                ]
            )
        )
        clicks = 0
        for selector in selectors:
            if clicks >= 4:
                return clicks
            try:
                locator = page.locator(selector)
                count = await locator.count()
            except Exception:
                continue
            for index in range(min(count, 2)):
                if await self._click_locator(locator.nth(index)):
                    clicks += 1

        try:
            text_locator = page.locator("button, a, [role='button']").filter(has_text=DETAIL_REVEAL_TEXT_RE)
            count = await text_locator.count()
        except Exception:
            return clicks

        for index in range(min(count, 3)):
            if clicks >= 4:
                return clicks
            if await self._click_locator(text_locator.nth(index)):
                clicks += 1
        return clicks

    async def _click_locator(self, locator: Any) -> bool:
        try:
            href = await locator.get_attribute("href")
        except Exception:
            href = None

        if href and not href.startswith(("#", "javascript:")):
            return False

        try:
            text_content = await locator.text_content() or ""
        except Exception:
            text_content = ""
        metadata_parts = [text_content]
        for attr_name in ("aria-label", "data-testid", "class", "title"):
            try:
                value = await locator.get_attribute(attr_name)
            except Exception:
                value = None
            if value:
                metadata_parts.append(value)
        if not DETAIL_REVEAL_TEXT_RE.search(" ".join(metadata_parts)):
            return False

        try:
            if not await locator.is_visible():
                return False
        except Exception:
            return False

        try:
            await locator.scroll_into_view_if_needed(timeout=1000)
        except Exception:
            return False

        try:
            await locator.click(timeout=2000)
            await asyncio.sleep(0.35)
            return True
        except Exception:
            return False


AISessionHandler = Callable[..., FetchResult | dict[str, Any] | Awaitable[FetchResult | dict[str, Any]]]


class AISessionClient(SessionClient):
    mode: Mode = "ai"

    async def _execute_request(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str,
        proxy: str | None,
    ) -> FetchResult:
        del proxy
        handler = getattr(self.settings, "scrapling_ai_handler", None)
        if handler is None:
            raise RuntimeError(
                "AI strategy requires settings.scrapling_ai_handler and should be used only as an optional fallback."
            )

        payload = handler(
            url=url,
            page_kind=page_kind,
            wait_selector=wait_selector,
            site_config=self.site_config,
            site_profile=self.site_profile,
        )
        resolved = await self._maybe_await(payload)
        if isinstance(resolved, FetchResult):
            return resolved
        if not isinstance(resolved, dict):
            raise RuntimeError("AI strategy handler must return FetchResult or a dict payload.")

        final_url = str(resolved.get("url", url))
        status_code = resolved.get("status_code")
        markup = str(resolved.get("html", ""))
        return FetchResult(
            page=parse_html_document(markup, url=final_url, status_code=status_code),
            url=final_url,
            status_code=status_code if isinstance(status_code, int) else None,
            error_class="ai_fallback",
        )

    async def _maybe_await(self, result: object) -> object:
        if inspect.isawaitable(result):
            return await result
        return result


def normalize_mode(mode: ModeAlias | str) -> Mode:
    normalized = MODE_ALIASES.get(mode)
    if normalized is None:
        raise KeyError(f"Unsupported scraping mode '{mode}'.")
    return normalized


def build_session_client(
    mode: ModeAlias | str,
    *,
    settings: Settings,
    site_config: SiteConfig,
    site_profile: SiteProfile,
) -> SessionClient:
    normalized_mode = normalize_mode(mode)
    session_classes: dict[Mode, type[SessionClient]] = {
        "http": HttpxSessionClient,
        "browser": PlaywrightSessionClient,
        "ai": AISessionClient,
    }
    session_class = session_classes[normalized_mode]
    return session_class(settings=settings, site_config=site_config, site_profile=site_profile)


HttpScraplingSession = HttpxSessionClient
DynamicScraplingSession = PlaywrightSessionClient
StealthScraplingSession = AISessionClient
ScraplingSessionClient = SessionClient


__all__ = [
    "AISessionClient",
    "DynamicScraplingSession",
    "FetchResult",
    "HttpScraplingSession",
    "HttpxSessionClient",
    "PlaywrightSessionClient",
    "ScraplingSessionClient",
    "SessionClient",
    "StealthScraplingSession",
    "StrategyBlockedError",
    "build_session_client",
    "normalize_mode",
]
