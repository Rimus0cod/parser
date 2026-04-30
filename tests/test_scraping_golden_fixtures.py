from __future__ import annotations

import unittest
from pathlib import Path

from app.core.config import SiteConfig
from app.scraping.extractor import ListingExtractor
from app.scraping.html_adapter import parse_html_document

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "scraping"


class ScrapingGoldenFixtureTests(unittest.TestCase):
    def test_imoti_fixture_uses_field_selectors_and_normalized_values(self) -> None:
        site = SiteConfig(
            name="imoti.bg",
            base_url="https://imoti.bg/наеми/page:1",
            selectors={
                "card": "article",
                "title": "a[href]",
                "link": "a[href]",
                "price": ".price",
                "location": ".location",
                "size": ".size",
                "seller": ".seller",
            },
            listing_path_keywords=["/наеми/"],
            allowed_domains=["imoti.bg"],
        )
        page = parse_html_document(
            (FIXTURE_DIR / "imoti_listing.html").read_text(encoding="utf-8"),
            url=site.base_url,
        )

        listings = ListingExtractor(site).extract_listings(page, base_url=site.base_url)

        self.assertEqual(len(listings), 1)
        listing = listings[0]
        self.assertEqual(listing.identity.storage_key, "imoti.bg:12345")
        self.assertEqual(listing.price, "1 200 BGN")
        self.assertEqual(str(listing.price_amount), "1200")
        self.assertEqual(listing.currency, "BGN")
        self.assertEqual(listing.size, "65 м²")
        self.assertEqual(str(listing.area_m2), "65")

    def test_alo_fixture_accepts_prefix_currency_and_m2_area(self) -> None:
        site = SiteConfig(
            name="alo.bg",
            base_url="https://www.alo.bg/obiavi/imoti-naemi/",
            selectors={
                "card": "article",
                "title": "a[href]",
                "link": "a[href]",
                "price": ".price",
                "location": ".location",
                "size": ".area",
            },
            listing_path_keywords=["/obiava/"],
            allowed_domains=["www.alo.bg", "alo.bg"],
        )
        page = parse_html_document(
            (FIXTURE_DIR / "alo_listing.html").read_text(encoding="utf-8"),
            url=site.base_url,
        )

        listings = ListingExtractor(site).extract_listings(page, base_url=site.base_url)

        self.assertEqual(len(listings), 1)
        listing = listings[0]
        self.assertEqual(listing.identity.storage_key, "alo.bg:67890")
        self.assertEqual(listing.price, "700 EUR")
        self.assertEqual(str(listing.price_amount), "700")
        self.assertEqual(listing.currency, "EUR")
        self.assertEqual(listing.size, "50 м²")
        self.assertEqual(str(listing.area_m2), "50")


if __name__ == "__main__":
    unittest.main()
