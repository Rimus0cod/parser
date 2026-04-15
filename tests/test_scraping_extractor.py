from __future__ import annotations

import re
import unittest

from app.core.config import SiteConfig
from app.scraping.extractor import ListingExtractor
from app.scraping.site_profiles import SiteProfile


class _FakeNode:
    def __init__(
        self,
        text_value: str = "",
        *,
        attrs: dict[str, str] | None = None,
        selectors: dict[str, list["_FakeNode"]] | None = None,
    ) -> None:
        self._text_value = text_value
        self._attrs = attrs or {}
        self._selectors = selectors or {}
        self.attrib = self._attrs

    def css(self, selector: str) -> list["_FakeNode"]:
        return list(self._selectors.get(selector, []))

    def css_first(self, selector: str, **_: object) -> "_FakeNode | None":
        matches = self.css(selector)
        return matches[0] if matches else None

    def find_similar(self) -> list["_FakeNode"]:
        return []

    def text(self) -> str:
        return self._text_value

    def get(self, attr_name: str) -> str:
        return self._attrs.get(attr_name, "")

    def find_by_regex(self, pattern: str, first_match: bool = True) -> "_FakeNode | None":
        match = re.search(pattern, self._text_value, flags=re.I)
        if match is None:
            return None
        if first_match:
            return _FakeNode(match.group(0))
        return _FakeNode(match.group(0))

    def save(self, *_: object, **__: object) -> None:
        return None

    def __str__(self) -> str:
        return self._text_value


class ListingExtractorTests(unittest.TestCase):
    def test_extractor_parses_listing_and_enriches_detail(self) -> None:
        site = SiteConfig(
            name="example.com",
            base_url="https://example.com/catalog",
            selectors={
                "card": "article",
                "title": "a[href]",
                "link": "a[href]",
                "seller": ".seller",
            },
            listing_path_keywords=["/listing/"],
            allowed_domains=["example.com"],
        )
        profile = SiteProfile(
            name="example.com",
            list_wait_selector="article",
            detail_wait_selector="body",
            blocked_markers=("captcha",),
        )
        extractor = ListingExtractor(site, profile)

        link = _FakeNode("Sunny apartment for rent", attrs={"href": "/listing/12345"})
        seller = _FakeNode("Agency Alpha")
        card = _FakeNode(
            "Sunny apartment for rent 1200 EUR Sofia, Center 65 м²",
            selectors={
                "a[href]": [link],
                ".seller": [seller],
            },
        )
        page = _FakeNode(
            "catalog",
            selectors={
                "article": [card],
                "a[href]": [link],
            },
        )

        listings = extractor.extract_listings(page, base_url="https://example.com/catalog")

        self.assertEqual(len(listings), 1)
        self.assertEqual(listings[0].ad_id, "12345")
        self.assertEqual(listings[0].price, "1200 EUR")
        self.assertEqual(listings[0].location, "Sofia, Center")
        self.assertEqual(listings[0].ad_type, "agency")

        detail_page = _FakeNode(
            "Contact Ivan Ivanov +359 88 123 4567 owner@example.com",
            selectors={"body": [_FakeNode("body")]},
        )
        enriched = extractor.enrich_listing(detail_page, listings[0])
        self.assertEqual(enriched.phone, "+359881234567")
        self.assertEqual(enriched.contact_email, "owner@example.com")

    def test_extractor_detects_blocked_and_js_required_pages(self) -> None:
        site = SiteConfig(name="olx.ua", base_url="https://www.olx.ua/")
        profile = SiteProfile(
            name="olx.ua",
            list_wait_selector="article",
            blocked_markers=("captcha",),
            requires_js_on=("enable javascript",),
            detail_wait_selector="body",
            detail_requires_browser=True,
        )
        extractor = ListingExtractor(site, profile)

        blocked_page = _FakeNode("Please solve CAPTCHA to continue")
        self.assertEqual(extractor.detect_list_page_issue(blocked_page), "blocked_marker:captcha")

        js_page = _FakeNode("Please enable javascript to continue")
        self.assertEqual(extractor.detect_list_page_issue(js_page), "js_required:enable javascript")

        detail_page = _FakeNode("detail text")
        self.assertEqual(extractor.detect_detail_page_issue(detail_page), "detail_requires_browser:body")


if __name__ == "__main__":
    unittest.main()
