from __future__ import annotations

import inspect
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from app.core.config import Settings, SiteConfig
    from app.scraping.site_profiles import SiteProfile

try:
    from app.scraping.storage import MySQLAdaptiveStorage
except Exception:  # pragma: no cover - optional dependency path for local imports
    MySQLAdaptiveStorage = None

try:
    from scrapling.fetchers import AsyncDynamicSession, AsyncStealthySession, FetcherSession, ProxyRotator
except ImportError:  # pragma: no cover - optional dependency path for local imports
    AsyncDynamicSession = None
    AsyncStealthySession = None
    FetcherSession = None
    ProxyRotator = None

PageKind = Literal["list", "detail"]
Mode = Literal["http", "dynamic", "stealth"]


@dataclass(slots=True)
class FetchResult:
    page: Any | None
    url: str
    status_code: int | None = None


class StrategyBlockedError(RuntimeError):
    def __init__(
        self,
        *,
        site_name: str,
        mode: str,
        url: str,
        reason: str,
    ) -> None:
        super().__init__(f"{site_name}:{mode}:{reason} ({url})")
        self.site_name = site_name
        self.mode = mode
        self.url = url
        self.reason = reason


class ScraplingSessionClient(AbstractAsyncContextManager["ScraplingSessionClient"]):
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
        self._session: Any | None = None

    async def __aenter__(self) -> "ScraplingSessionClient":
        self._session = self._build_session()
        enter = getattr(self._session, "__aenter__", None)
        if callable(enter):
            await self._maybe_await(enter())
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool | None:
        if self._session is None:
            return None
        exit_method = getattr(self._session, "__aexit__", None)
        if callable(exit_method):
            return await self._maybe_await(exit_method(exc_type, exc, tb))
        close = getattr(self._session, "close", None)
        if callable(close):
            await self._maybe_await(close())
        return None

    async def fetch(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str = "",
    ) -> FetchResult:
        if self._session is None:
            raise RuntimeError("Scrapling session is not initialized.")
        page = await self._execute_request(url, page_kind=page_kind, wait_selector=wait_selector)
        return FetchResult(
            page=page,
            url=self._page_url(page, fallback=url),
            status_code=self._status_code(page),
        )

    def _build_session(self) -> Any:
        raise NotImplementedError

    async def _execute_request(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str,
    ) -> Any | None:
        raise NotImplementedError

    def _storage_args(self, target_url: str) -> dict[str, object]:
        return {
            "url": target_url,
            "version": self.site_profile.selector_version,
            "table_name": self.settings.scrapling_storage_table,
        }

    def _adaptive_request_kwargs(self, target_url: str) -> dict[str, object]:
        kwargs: dict[str, object] = {"adaptive": True}
        if MySQLAdaptiveStorage is not None:
            kwargs["storage"] = MySQLAdaptiveStorage
            kwargs["storage_args"] = self._storage_args(target_url)
        return kwargs

    def _browser_timeout_ms(self) -> int:
        timeout_seconds = self.site_config.timeout or self.settings.scrape_timeout_seconds
        return max(
            1000,
            self.settings.scrapling_wait_selector_timeout_ms,
            int(timeout_seconds * 1000),
        )

    def _proxy_rotator(self) -> Any | None:
        if not self.settings.proxy_enabled or not self.settings.proxy_pool:
            return None
        if ProxyRotator is None:
            raise RuntimeError("Scrapling ProxyRotator is unavailable but proxies are enabled.")
        return ProxyRotator(list(self.settings.proxy_pool))

    def _page_url(self, page: Any, *, fallback: str) -> str:
        for attr_name in ("url",):
            value = getattr(page, attr_name, None)
            if isinstance(value, str) and value:
                return value
        return fallback

    def _status_code(self, page: Any) -> int | None:
        for attr_name in ("status", "status_code"):
            value = getattr(page, attr_name, None)
            if isinstance(value, int):
                return value
        return None

    async def _maybe_await(self, result: Any) -> Any:
        if inspect.isawaitable(result):
            return await result
        return result


class HttpScraplingSession(ScraplingSessionClient):
    mode: Mode = "http"

    def _build_session(self) -> Any:
        if FetcherSession is None:
            raise RuntimeError("Scrapling FetcherSession is not installed.")

        return FetcherSession(
            impersonate=self.settings.scrapling_http_impersonate,
            http3=self.settings.scrapling_http3,
            stealthy_headers=True,
            follow_redirects=self.settings.scrape_follow_redirects,
            retries=self.settings.scrape_retry_count,
            retry_delay=self.settings.scrape_backoff_base_seconds,
            timeout=self.site_config.timeout or self.settings.scrape_timeout_seconds,
            proxy_rotator=self._proxy_rotator(),
        )

    async def _execute_request(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str,
    ) -> Any | None:
        if self._session is None:
            return None
        request = getattr(self._session, "get")
        result = request(
            url,
            **self._adaptive_request_kwargs(url),
        )
        return await self._maybe_await(result)


class DynamicScraplingSession(ScraplingSessionClient):
    mode: Mode = "dynamic"

    def _build_session(self) -> Any:
        if AsyncDynamicSession is None:
            raise RuntimeError("Scrapling AsyncDynamicSession is not installed.")

        return AsyncDynamicSession(
            headless=True,
            disable_resources=self.settings.scrapling_disable_resources,
            disable_ads=self.settings.scrapling_disable_ads,
            block_webrtc=self.settings.scrapling_block_webrtc,
            network_idle=self.settings.scrapling_network_idle,
            timeout=self._browser_timeout_ms(),
            proxy=self.settings.proxy_pool[0] if self.settings.proxy_enabled and self.settings.proxy_pool else None,
        )

    async def _execute_request(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str,
    ) -> Any | None:
        if self._session is None:
            return None
        result = self._session.fetch(
            url,
            wait_selector=wait_selector or None,
            wait_selector_state="attached",
            timeout=self._browser_timeout_ms(),
            **self._adaptive_request_kwargs(url),
        )
        return await self._maybe_await(result)


class StealthScraplingSession(ScraplingSessionClient):
    mode: Mode = "stealth"

    def _build_session(self) -> Any:
        if AsyncStealthySession is None:
            raise RuntimeError("Scrapling AsyncStealthySession is not installed.")

        return AsyncStealthySession(
            headless=True,
            disable_resources=self.settings.scrapling_disable_resources,
            disable_ads=self.settings.scrapling_disable_ads,
            block_webrtc=self.settings.scrapling_block_webrtc,
            network_idle=self.settings.scrapling_network_idle,
            humanize=self.settings.scrapling_stealth_humanize,
            solve_cloudflare=self.settings.scrapling_solve_cloudflare,
            timeout=self._browser_timeout_ms(),
            proxy=self.settings.proxy_pool[0] if self.settings.proxy_enabled and self.settings.proxy_pool else None,
        )

    async def _execute_request(
        self,
        url: str,
        *,
        page_kind: PageKind,
        wait_selector: str,
    ) -> Any | None:
        if self._session is None:
            return None
        result = self._session.fetch(
            url,
            wait_selector=wait_selector or None,
            wait_selector_state="attached",
            timeout=self._browser_timeout_ms(),
            **self._adaptive_request_kwargs(url),
        )
        return await self._maybe_await(result)


def build_session_client(
    mode: Mode,
    *,
    settings: Settings,
    site_config: SiteConfig,
    site_profile: SiteProfile,
) -> ScraplingSessionClient:
    session_classes: dict[Mode, type[ScraplingSessionClient]] = {
        "http": HttpScraplingSession,
        "dynamic": DynamicScraplingSession,
        "stealth": StealthScraplingSession,
    }
    session_class = session_classes[mode]
    return session_class(settings=settings, site_config=site_config, site_profile=site_profile)


__all__ = [
    "FetchResult",
    "HttpScraplingSession",
    "DynamicScraplingSession",
    "StealthScraplingSession",
    "ScraplingSessionClient",
    "StrategyBlockedError",
    "build_session_client",
]
