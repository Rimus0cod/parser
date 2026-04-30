from __future__ import annotations

import re
import unittest

from app.core.config import SiteConfig
from app.scraping.extractor import ListingExtractor
from app.scraping.html_adapter import parse_html_document
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
            "Sunny apartment for rent 1200 EUR Sofia, Center 65 m2",
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
        site = SiteConfig(name="alo.bg", base_url="https://www.alo.bg/")
        profile = SiteProfile(
            name="alo.bg",
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
        self.assertEqual(
            extractor.detect_detail_page_issue(detail_page), "detail_requires_browser:body"
        )

    def test_extractor_prefers_contact_block_links_over_global_page_number(self) -> None:
        site = SiteConfig(
            name="alo.bg",
            base_url="https://www.alo.bg/obiavi/imoti-naemi/",
            selectors={
                "card": "article",
                "title": "a[href]",
                "link": "a[href]",
            },
            listing_path_keywords=["/obiava/"],
            allowed_domains=["www.alo.bg", "alo.bg"],
        )
        profile = SiteProfile(
            name="alo.bg",
            detail_wait_selector="body",
            detail_contact_selectors=("[class*='contact']",),
        )
        extractor = ListingExtractor(site, profile)

        listing = extractor._parse_card(
            card=_FakeNode(
                "Apartment for rent 1200 BGN Sofia, Center 50 m2",
                selectors={
                    "a[href]": [
                        _FakeNode(
                            "Apartment", attrs={"href": "/obiava/apartment-sofia-12345"}
                        )
                    ],
                },
            ),
            base_url="https://www.alo.bg/obiavi/imoti-naemi/",
            position=0,
        )
        assert listing is not None

        tel_link = _FakeNode("+359 (0)88 123 4567", attrs={"href": "tel:00359 88 123 4567"})
        mail_link = _FakeNode("owner@example.com", attrs={"href": "mailto:owner@example.com"})
        contact_block = _FakeNode(
            "Contact Ivan Petrov",
            selectors={"a[href]": [tel_link, mail_link]},
        )
        detail_page = _FakeNode(
            "019607843 header number Roboto ArialFallBack icon:https://www.alo.bg",
            selectors={
                "[class*='contact']": [contact_block],
                "a[href]": [],
            },
        )

        enriched = extractor.enrich_listing(detail_page, listing)

        self.assertEqual(enriched.phone, "+359881234567")
        self.assertEqual(enriched.contact_email, "owner@example.com")
        self.assertEqual(enriched.contact_name, "Ivan Petrov")

    def test_extractor_rejects_non_bulgarian_phone_numbers(self) -> None:
        site = SiteConfig(name="alo.bg", base_url="https://www.alo.bg/")
        extractor = ListingExtractor(site)

        self.assertEqual(extractor._extract_phone_from_text("+380 67 123 45 67"), "")
        self.assertEqual(extractor._extract_phone_from_text("019607843"), "")
        self.assertEqual(extractor._extract_phone_from_text("0888 123 456"), "0888123456")
        self.assertEqual(extractor._extract_phone_from_text("00359 88 123 4567"), "+359881234567")

    def test_html_adapter_ignores_script_and_style_text(self) -> None:
        page = parse_html_document(
            """
            <html>
              <head>
                <style>.x{font-family:Roboto,ArialFallBack}</style>
                <script>window.icon='https://example.com';</script>
              </head>
              <body>
                <main>Contact Ivan Petrov</main>
              </body>
            </html>
            """,
            url="https://example.com/listing/1",
        )

        text = page.text_content()

        self.assertIn("Contact Ivan Petrov", text)
        self.assertNotIn("Roboto", text)
        self.assertNotIn("icon", text)


if __name__ == "__main__":
    unittest.main()
