from __future__ import annotations

import unittest
from types import SimpleNamespace

from app.core.config import SiteConfig
from app.scraping.site_profiles import get_site_profile


class SiteProfilesTests(unittest.TestCase):
    def test_profile_merges_global_blocked_markers(self) -> None:
        site = SiteConfig(name="imoti.bg", base_url="https://imoti.bg/наеми/page:{page}")
        settings = SimpleNamespace(scrapling_blocked_markers=["custom blocker", "captcha"])

        profile = get_site_profile(site, settings)

        self.assertIn("custom blocker", profile.blocked_markers)
        self.assertIn("captcha", profile.blocked_markers)
        self.assertEqual(profile.selector_version, site.selector_version)

    def test_profile_preserves_site_mode_order_override(self) -> None:
        site = SiteConfig(
            name="olx.ua",
            base_url="https://www.olx.ua/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/",
            mode_order=["dynamic", "stealth"],
        )

        profile = get_site_profile(site)

        self.assertEqual(profile.mode_order, ("browser", "ai"))


if __name__ == "__main__":
    unittest.main()
