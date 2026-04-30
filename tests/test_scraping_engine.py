from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

from app.core.config import SiteConfig
from app.scraping.engine import ScrapingEngine
from app.scraping.fetchers import StrategyBlockedError
from app.scraping.models import ScrapedListing


class _FakeStrategy:
    def __init__(self, *, name: str, mode: str, payload: dict[str, list[ScrapedListing]]) -> None:
        self.name = name
        self.mode = mode
        self._payload = payload

    def supports(self, site_config: SiteConfig) -> bool:
        return site_config.name in self._payload

    async def scrape_site(self, site_config: SiteConfig) -> list[ScrapedListing]:
        return list(self._payload.get(site_config.name, []))


class _BlockingStrategy:
    def __init__(
        self, *, name: str, mode: str, blocked_sites: set[str], reason: str = "captcha"
    ) -> None:
        self.name = name
        self.mode = mode
        self._blocked_sites = blocked_sites
        self._reason = reason

    def supports(self, site_config: SiteConfig) -> bool:
        return site_config.name in self._blocked_sites

    async def scrape_site(self, site_config: SiteConfig) -> list[ScrapedListing]:
        raise StrategyBlockedError(
            site_name=site_config.name,
            mode=self.mode,
            url=site_config.base_url,
            reason=self._reason,
        )


def _listing(
    *,
    ad_id: str,
    source_site: str,
    title: str = "Spacious apartment for rent",
    price: str = "1200 EUR",
    location: str = "Kyiv, Podil",
    size: str = "54 м²",
    link: str | None = None,
) -> ScrapedListing:
    listing_link = link or f"https://{source_site}/listing/{ad_id}"
    return ScrapedListing(
        ad_id=ad_id,
        title=title,
        price=price,
        location=location,
        size=size,
        link=listing_link,
        source_site=source_site,
    )


class ScrapingEngineTests(unittest.TestCase):
    def test_engine_rejects_hard_invalid_listing(self) -> None:
        site = SiteConfig(name="example.com", base_url="https://example.com")
        valid = _listing(ad_id="A-1", source_site=site.name)
        invalid = _listing(ad_id="", source_site=site.name, link="not-a-url")
        engine = ScrapingEngine(
            settings=SimpleNamespace(sites=[site]),
            strategies=[
                _FakeStrategy(
                    name="legacy_http_bs4",
                    mode="http",
                    payload={site.name: [valid, invalid]},
                )
            ],
        )

        async def run_test() -> None:
            result = await engine.scrape_all_sites()
            self.assertEqual(result.accepted_count, 1)
            self.assertEqual(result.rejected_count, 1)
            self.assertEqual(result.listings[0].ad_id, "A-1")

        asyncio.run(run_test())

    def test_engine_retries_next_strategy_when_first_one_fails_validation(self) -> None:
        site = SiteConfig(name="retry.example", base_url="https://retry.example")
        invalid = _listing(ad_id="", source_site=site.name, link="broken-link")
        valid = _listing(ad_id="B-2", source_site=site.name)
        engine = ScrapingEngine(
            settings=SimpleNamespace(sites=[site]),
            strategies=[
                _FakeStrategy(
                    name="http_first_pass",
                    mode="http",
                    payload={site.name: [invalid]},
                ),
                _FakeStrategy(
                    name="browser_second_pass",
                    mode="browser",
                    payload={site.name: [valid]},
                ),
            ],
        )

        async def run_test() -> None:
            result = await engine.scrape_all_sites()
            self.assertEqual(result.accepted_count, 1)
            self.assertEqual(result.rejected_count, 0)
            self.assertEqual(result.site_results[0].strategy_name, "browser_second_pass")

        asyncio.run(run_test())

    def test_engine_deduplicates_per_site_and_ad_id(self) -> None:
        site_a = SiteConfig(name="site-a.example", base_url="https://site-a.example")
        site_b = SiteConfig(name="site-b.example", base_url="https://site-b.example")
        listing_a = _listing(ad_id="shared-id", source_site=site_a.name)
        listing_b = _listing(ad_id="shared-id", source_site=site_b.name)
        engine = ScrapingEngine(
            settings=SimpleNamespace(sites=[site_a, site_b]),
            strategies=[
                _FakeStrategy(
                    name="legacy_http_bs4",
                    mode="http",
                    payload={
                        site_a.name: [listing_a],
                        site_b.name: [listing_b],
                    },
                )
            ],
        )

        async def run_test() -> None:
            result = await engine.scrape_all_sites()
            self.assertEqual(result.accepted_count, 2)
            self.assertEqual(
                {row.source_site for row in result.listings}, {site_a.name, site_b.name}
            )

        asyncio.run(run_test())

    def test_engine_retries_next_strategy_when_first_one_is_blocked(self) -> None:
        site = SiteConfig(name="blocked.example", base_url="https://blocked.example")
        valid = _listing(ad_id="C-3", source_site=site.name)
        engine = ScrapingEngine(
            settings=SimpleNamespace(sites=[site]),
            strategies=[
                _BlockingStrategy(
                    name="http_strategy",
                    mode="http",
                    blocked_sites={site.name},
                    reason="blocked_marker:captcha",
                ),
                _FakeStrategy(
                    name="browser_strategy",
                    mode="browser",
                    payload={site.name: [valid]},
                ),
            ],
        )

        async def run_test() -> None:
            result = await engine.scrape_all_sites()
            self.assertEqual(result.accepted_count, 1)
            self.assertEqual(result.site_results[0].strategy_name, "browser_strategy")
            self.assertIn(
                "http_strategy blocked: blocked_marker:captcha", result.site_results[0].errors
            )

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
