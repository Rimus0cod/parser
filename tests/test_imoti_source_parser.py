from __future__ import annotations

import unittest

from bs4 import BeautifulSoup

from app.core.config import Settings, SiteConfig
from app.services.async_scraper import BaseScraper


class ImotiSourceParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.site = SiteConfig(
            name="imoti.bg",
            base_url="https://imoti.bg/наеми/page:{page}",
            max_pages=1,
            selectors={
                "link": "h4 a[href*='/наеми/'], a[href*='/наеми/']",
                "seller": "[class*='agency'], [class*='seller']",
            },
            allowed_domains=["imoti.bg"],
        )
        self.settings = Settings(
            sites=[self.site],
            city_filter=None,
            scrape_detail_pages=False,
            _env_file=None,
        )
        self.scraper = BaseScraper(self.site, self.settings)

    def test_exact_card_parsing_is_delegated_to_imoti_parser(self) -> None:
        html = """
        <html><body>
          <article class="product-classic">
            <h4 class="product-classic-title">
              <a href="/наеми/двустаен-апартамент-123456.htm">
                Двустаен апартамент под наем
              </a>
            </h4>
            <div class="product-classic-price">
              900 EUR
              <span>Допълнителен текст</span>
            </div>
            <div class="btext">София, Лозенец</div>
            <ul class="product-classic-list">
              <li>72 кв.м.</li>
            </ul>
            <div class="product-classic-agency">Example Agency</div>
            <a href="tel:+359888123456">+359 888 123 456</a>
            <img src="/images/123456.jpg">
          </article>

          <article class="product-classic">
            <h4 class="product-classic-title">
              <a href="/наеми/двустаен-апартамент-123456.htm">
                Двустаен апартамент под наем
              </a>
            </h4>
            <div class="product-classic-price">900 EUR</div>
            <div class="btext">София, Лозенец</div>
          </article>
        </body></html>
        """

        listings = self.scraper._parse_listing_page(
            html,
            base_url="https://imoti.bg/наеми/",
        )

        self.assertEqual(len(listings), 1)
        listing = listings[0]
        self.assertEqual(listing.ad_id, "123456")
        self.assertEqual(listing.title, "Двустаен апартамент под наем")
        self.assertEqual(listing.price, "900 EUR")
        self.assertEqual(listing.location, "София, Лозенец")
        self.assertEqual(listing.size, "72 кв.м.")
        self.assertEqual(listing.seller_name, "Example Agency")
        self.assertEqual(listing.ad_type, "agency")
        self.assertTrue(listing.phone)
        self.assertEqual(listing.source_site, "imoti.bg")
        self.assertEqual(
            listing.link,
            "https://imoti.bg/наеми/двустаен-апартамент-123456.htm",
        )

    def test_detail_enrichment_remains_source_specific(self) -> None:
        listing = self.scraper._parse_listing_page(
            """
            <article class="product-classic">
              <h4 class="product-classic-title">
                <a href="/наеми/апартамент-654321.htm">
                  Апартамент под наем
                </a>
              </h4>
              <div class="product-classic-price">800 EUR</div>
              <div class="btext">София</div>
            </article>
            """,
            base_url="https://imoti.bg/наеми/",
        )[0]

        soup = BeautifulSoup(
            """
            <html><body>
              <div class="block-person-link">
                <span class="icon mdi-account"></span>
                Example Agency
              </div>
              <div class="block-person-link">
                <span class="icon mdi-phone"></span>
                <a href="tel:+359887654321">+359 887 654 321</a>
              </div>
              <div class="block-person-link">
                <span class="icon mdi-email"></span>
                <a href="mailto:agent@example.com">agent@example.com</a>
              </div>
            </body></html>
            """,
            "lxml",
        )

        self.scraper._imoti_parser.enrich_detail(soup, listing)

        self.assertEqual(listing.seller_name, "Example Agency")
        self.assertEqual(listing.contact_name, "Example Agency")
        self.assertTrue(listing.phone)
        self.assertEqual(listing.contact_email, "agent@example.com")
        self.assertEqual(listing.ad_type, "agency")


if __name__ == "__main__":
    unittest.main()
