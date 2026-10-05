from __future__ import annotations

import unittest

from bs4 import BeautifulSoup

from app.core.config import Settings, SiteConfig
from app.services.async_scraper import BaseScraper


class AloSourceParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.site = SiteConfig(
            name="alo.bg",
            base_url="https://www.alo.bg/obiavi/imoti-naemi/?page={page}",
            max_pages=1,
            allowed_domains=["alo.bg", "www.alo.bg"],
        )
        self.settings = Settings(
            sites=[self.site],
            city_filter=None,
            scrape_detail_pages=False,
            _env_file=None,
        )
        self.scraper = BaseScraper(self.site, self.settings)

    def test_listing_page_is_delegated_to_alo_parser(self) -> None:
        html = """
        <html><body>
          <div id="adrows_123456" class="ad_block_normal">
            <a class="avn_seo" href="/obiava/123456/dvustaen-apartament-pod-naem">
              Двустаен апартамент под наем
            </a>
            <a class="avn_image" href="/obiava/123456/dvustaen-apartament-pod-naem">
              <img alt="квартира под наем 65 м²" src="/images/123456.jpg">
            </a>
            <div class="avn_price">700 €</div>
            <div class="avn_location">София, Център</div>
          </div>

          <div id="adrows_123456" class="ad_block_normal">
            <a class="avn_seo" href="/obiava/123456/dvustaen-apartament-pod-naem">
              Двустаен апартамент под наем
            </a>
            <div class="avn_price">700 €</div>
            <div class="avn_location">София, Център</div>
          </div>

          <div id="adrows_999999" class="ad_block_normal">
            <a class="avn_seo" href="/obiava/999999/transportni-uslugi">
              Транспортни услуги
            </a>
            <div class="avn_price">100 €</div>
            <div class="avn_location">София</div>
          </div>
        </body></html>
        """

        listings = self.scraper._parse_listing_page(
            html,
            base_url="https://www.alo.bg/obiavi/imoti-naemi/",
        )

        self.assertEqual(len(listings), 1)
        listing = listings[0]
        self.assertEqual(listing.ad_id, "123456")
        self.assertEqual(listing.title, "Двустаен апартамент под наем")
        self.assertEqual(listing.price, "700 €")
        self.assertEqual(listing.location, "София, Център")
        self.assertEqual(listing.size, "65 м²")
        self.assertEqual(listing.source_site, "alo.bg")
        self.assertEqual(
            listing.link,
            "https://www.alo.bg/obiava/123456/dvustaen-apartament-pod-naem",
        )

    def test_detail_enrichment_stays_source_specific(self) -> None:
        listing = self.scraper._parse_listing_page(
            """
            <div id="adrows_123456" class="ad_block_normal">
              <a class="avn_seo" href="/obiava/123456/apartament-pod-naem">
                Апартамент под наем
              </a>
              <div class="avn_price">650 €</div>
              <div class="avn_location">София</div>
            </div>
            """,
            base_url="https://www.alo.bg/",
        )[0]

        soup = BeautifulSoup(
            """
            <html><body>
              <h1 class="large-headline">Обновен апартамент под наем</h1>
              <div class="ads-params-price">720 € Цената е около 1400 лв</div>
              <div class="ads-params-row">
                <div class="ads-param-title">Местоположение</div>
                <div class="ads-params-cell">label</div>
                <div class="ads-params-cell">София, Лозенец</div>
              </div>
              <div class="ads-params-row">
                <div class="ads-param-title">Квадратура</div>
                <div class="ads-params-cell">label</div>
                <div class="ads-params-cell">72 м²</div>
              </div>
              <div class="contacts header">
                <span>Контакт с подателя</span>
                <strong>Example Agency</strong>
              </div>
              <div class="agent_div"><div class="contact_value">Иван Иванов</div></div>
              <div class="contact_phone"><span class="ocd_span">+359 888 123 456</span></div>
              <div class="contacts_wrapper_flex has_agents"></div>
            </body></html>
            """,
            "lxml",
        )

        self.scraper._alo_parser.enrich_detail(soup, listing)

        self.assertEqual(listing.title, "Обновен апартамент под наем")
        self.assertEqual(listing.price, "720 €")
        self.assertEqual(listing.location, "София, Лозенец")
        self.assertEqual(listing.size, "72 м²")
        self.assertEqual(listing.seller_name, "Example Agency")
        self.assertEqual(listing.contact_name, "Иван Иванов")
        self.assertTrue(listing.phone)
        self.assertEqual(listing.ad_type, "agency")


if __name__ == "__main__":
    unittest.main()
