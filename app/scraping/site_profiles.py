from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from app.core.config import Settings, SiteConfig

ScrapeMode = Literal["http", "dynamic", "stealth"]


@dataclass(slots=True, frozen=True)
class SiteProfile:
    name: str
    list_wait_selector: str = ""
    detail_wait_selector: str = ""
    blocked_markers: tuple[str, ...] = ()
    requires_js_on: tuple[str, ...] = ()
    detail_requires_browser: bool = False
    mode_order: tuple[ScrapeMode, ...] = ("http", "dynamic", "stealth")
    selector_version: str = "v1"


_DEFAULT_PROFILES: dict[str, SiteProfile] = {
    "imoti.bg": SiteProfile(
        name="imoti.bg",
        list_wait_selector="article.product-classic, a[href*='/наеми/']",
        detail_wait_selector="main, article, body",
        blocked_markers=(
            "just a moment",
            "cloudflare",
            "turnstile",
            "verify you are human",
        ),
        requires_js_on=("enable javascript", "please enable javascript"),
        detail_requires_browser=False,
        mode_order=("http", "dynamic", "stealth"),
        selector_version="v2",
    ),
    "alo.bg": SiteProfile(
        name="alo.bg",
        list_wait_selector=".list, .listvip, .list-obj, article, a[href*='/obiava/']",
        detail_wait_selector="main, article, body",
        blocked_markers=(
            "access denied",
            "captcha",
            "verify you are human",
        ),
        requires_js_on=("enable javascript",),
        detail_requires_browser=False,
        mode_order=("http", "dynamic", "stealth"),
        selector_version="v2",
    ),
    "dom.ria.com": SiteProfile(
        name="dom.ria.com",
        list_wait_selector="article, [data-testid], a[href*='/arenda-kvartir/']",
        detail_wait_selector="main, article, body",
        blocked_markers=(
            "access denied",
            "captcha",
            "unusual traffic",
        ),
        requires_js_on=("javascript", "hydration", "__next"),
        detail_requires_browser=True,
        mode_order=("http", "dynamic", "stealth"),
        selector_version="v2",
    ),
    "olx.ua": SiteProfile(
        name="olx.ua",
        list_wait_selector="article, li, a[href*='/obyavlenie/'], a[href*='/d/uk/obyavlenie/']",
        detail_wait_selector="main, article, body",
        blocked_markers=(
            "access denied",
            "captcha",
            "verify you are a human",
        ),
        requires_js_on=("enable javascript", "noscript"),
        detail_requires_browser=True,
        mode_order=("http", "dynamic", "stealth"),
        selector_version="v2",
    ),
    "lun.ua": SiteProfile(
        name="lun.ua",
        list_wait_selector="article, section, a[href*='/rent/kyiv/flats/']",
        detail_wait_selector="main, article, body",
        blocked_markers=(
            "access denied",
            "captcha",
            "verify you are human",
        ),
        requires_js_on=("javascript", "__next", "hydration"),
        detail_requires_browser=True,
        mode_order=("http", "dynamic", "stealth"),
        selector_version="v2",
    ),
}


def get_site_profile(site_config: SiteConfig, settings: Settings | None = None) -> SiteProfile:
    base_profile = _DEFAULT_PROFILES.get(site_config.name, SiteProfile(name=site_config.name))
    blocked_markers = base_profile.blocked_markers
    if settings is not None and settings.scrapling_blocked_markers:
        blocked_markers = tuple(
            dict.fromkeys(
                [
                    *blocked_markers,
                    *(marker.strip().lower() for marker in settings.scrapling_blocked_markers if marker.strip()),
                ]
            )
        )

    return replace(
        base_profile,
        blocked_markers=blocked_markers,
        mode_order=tuple(site_config.mode_order) if site_config.mode_order else base_profile.mode_order,
        selector_version=site_config.selector_version or base_profile.selector_version,
    )


__all__ = ["SiteProfile", "ScrapeMode", "get_site_profile"]
