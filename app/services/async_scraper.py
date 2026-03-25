from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Sequence
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from app.core.config import Settings, SiteConfig

# Добавляем логгер для этого модуля
logger = logging.getLogger(__name__)

APARTMENT_KEYWORDS: tuple[str, ...] = (
    "апартамент",
    "едностаен",
    "двустаен",
    "тристаен",
    "четиристаен",
    "многостаен",
    "квартира",
    "апартаменти",
    "аренда",
    "наем",
    "жилье",
    "житло",
)


@dataclass(slots=True)
class ScrapedListing:
    ad_id: str
    title: str
    price: str
    location: str
    size: str
    link: str
    source_site: str = ""
    phone: str = ""
    seller_name: str = ""
    ad_type: str = ""
    contact_name: str = "-"
    contact_email: str = "-"
    date_seen: str = ""


class BaseScraper:
    def __init__(self, site_config: SiteConfig, settings: Settings) -> None:
        self.site_config = site_config
        self.settings = settings
        self._sem = asyncio.Semaphore(max(1, settings.scrape_concurrency))

    async def scrape(self) -> list[ScrapedListing]:
        """Base method to be implemented by child classes"""
        today = date.today().isoformat()
        async with httpx.AsyncClient(
            timeout=self.settings.scrape_timeout_seconds,
            headers={"User-Agent": self.settings.user_agent},
        ) as client:
            tasks = [
                self._scrape_page(client, page) for page in range(1, self.site_config.max_pages + 1)
            ]
            page_results = await asyncio.gather(*tasks, return_exceptions=True)

        listings: list[ScrapedListing] = []
        seen: set[str] = set()
        for result in page_results:
            if isinstance(result, Exception):
                logger.exception("Error scraping page: %s", result)
                continue
            if not isinstance(result, list):
                continue
            for row in result:
                if row.ad_id in seen:
                    continue
                row.date_seen = today
                row.source_site = self.site_config.name
                seen.add(row.ad_id)
                listings.append(row)

        # Обогащение данными
        if listings:
            await self._enrich_with_detail_pages(client=None, listings=listings)
        return listings

    async def _scrape_page(self, client: httpx.AsyncClient, page: int) -> list[ScrapedListing]:
        url = self.site_config.base_url.format(page=page)
        try:
            async with self._sem:
                # Добавляем экспоненциальную задержку в случае ошибок
                for attempt in range(3):
                    try:
                        response = await client.get(url)
                        break
                    except httpx.RequestError as e:
                        if attempt == 2:  # последняя попытка
                            raise
                        logger.warning(f"Request failed (attempt {attempt + 1}/3): {e}")
                        await asyncio.sleep(2**attempt)  # экспоненциальная задержка

                if response.status_code != 200:
                    logger.warning(f"Non-200 status code {response.status_code} for URL: {url}")
                    return []

                return self._parse_listing_page(response.text, base_url=url)
        except Exception as e:
            logger.error(f"Error scraping page {page} from {self.site_config.name}: {e}")
            return []

    def _parse_listing_page(self, html: str, base_url: str) -> list[ScrapedListing]:
        try:
            soup = BeautifulSoup(html, "lxml")
            cards = soup.select(self.site_config.selectors.get("card", "div"))
            results: list[ScrapedListing] = []
            for card in cards:
                parsed = self._parse_card(card, base_url)
                if parsed is None:
                    continue
                if (
                    self.settings.city_filter
                    and self.settings.city_filter.lower() not in parsed.location.lower()
                ):
                    continue
                results.append(parsed)
            return results
        except Exception as e:
            logger.error(f"Error parsing listing page from {self.site_config.name}: {e}")
            return []

    def _parse_card(self, card: BeautifulSoup, base_url: str) -> ScrapedListing | None:
        try:
            # Получаем элемент ссылки
            link_selector = self.site_config.selectors.get("link", "a[href]")
            link_el = card.select_one(link_selector)
            href = link_el.get("href", "").strip() if link_el else ""
            if not href:
                return None
            link = urljoin(base_url, href)

            # Получаем заголовок
            title_selector = self.site_config.selectors.get("title", "h2, h3, .title, a[title]")
            title_el = card.select_one(title_selector)
            title = (title_el.get_text(" ", strip=True) if title_el else "").strip()

            if not title and link_el:
                title = link_el.get_text(" ", strip=True).strip()

            if not title or not any(word in title.lower() for word in APARTMENT_KEYWORDS):
                return None

            # Получаем ID объявления
            ad_id_match = (
                re.search(r"/(\d{6,})", link)
                or re.search(r"ad_id=(\d+)", link)
                or re.search(r"-([0-9]{6,})(?:\.htm|\.html)?$", link)
            )
            ad_id = ad_id_match.group(1) if ad_id_match else ""
            if not ad_id:
                # Попробуем извлечь ID другим способом
                ad_id = str(abs(hash(link)))[:10]

            card_text = card.get_text(" ", strip=True)

            # Разбор остальных полей
            price_selector = self.site_config.selectors.get(
                "price", ".price, .item-price, [class*='price']"
            )
            location_selector = self.site_config.selectors.get(
                "location", ".location, .item-location, [class*='location']"
            )
            size_selector = self.site_config.selectors.get(
                "size", ".size, [class*='area'], [class*='size']"
            )
            seller_selector = self.site_config.selectors.get(
                "seller", ".seller, [class*='agency'], [class*='owner']"
            )

            price_el = card.select_one(price_selector)
            location_el = card.select_one(location_selector)
            size_el = card.select_one(size_selector)
            seller_el = card.select_one(seller_selector)

            seller_name = (seller_el.get_text(" ", strip=True) if seller_el else "").strip()

            # Определяем тип объявления
            ad_type = (
                "agency"
                if any(word in seller_name.lower() for word in ["аген", "агент"])
                else "private"
            )

            return ScrapedListing(
                ad_id=ad_id,
                title=title,
                price=(price_el.get_text(" ", strip=True) if price_el else "").strip(),
                location=(location_el.get_text(" ", strip=True) if location_el else "").strip(),
                size=(size_el.get_text(" ", strip=True) if size_el else "").strip(),
                link=link,
                source_site=self.site_config.name,
                seller_name=seller_name,
                ad_type=ad_type,
            )
        except Exception as e:
            logger.error(f"Error parsing card from {self.site_config.name}: {e}")
            return None

    async def _enrich_with_detail_pages(
        self, client: httpx.AsyncClient | None, listings: list[ScrapedListing]
    ) -> None:
        """
        Подгружаем страницы конкретных объявлений и заполняем:
        - `phone`
        - `contact_email`
        - `contact_name`
        """
        async with httpx.AsyncClient(
            timeout=self.settings.scrape_timeout_seconds,
            headers={"User-Agent": self.settings.user_agent},
        ) as local_client:
            http = client or local_client
            tasks = [self._enrich_one(http, row) for row in listings]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Error enriching listing from {self.site_config.name}: {res}")

    async def _enrich_one(
        self, client: httpx.AsyncClient, listing: ScrapedListing
    ) -> ScrapedListing:
        try:
            # Добавляем экспоненциальную задержку в случае ошибок
            for attempt in range(3):
                try:
                    async with self._sem:
                        response = await client.get(listing.link)
                    if response.status_code == 200:
                        break
                    elif attempt == 2:  # последняя попытка
                        logger.warning(
                            f"Non-200 status code {response.status_code} for detail page: {listing.link}"
                        )
                        return listing
                    else:
                        await asyncio.sleep(2**attempt)
                except httpx.RequestError as e:
                    if attempt == 2:  # последняя попытка
                        logger.error(f"Request error on detail page: {listing.link}, error: {e}")
                        return listing
                    await asyncio.sleep(2**attempt)

            soup = BeautifulSoup(response.text, "lxml")
            text = soup.get_text(" ", strip=True)

            # Извлечение телефона
            tel_links = soup.select("a[href^='tel:']")
            phones: list[str] = []
            for a in tel_links:
                href = a.get("href", "")
                m = re.search(r"(?:\+?359)?[0-9]{9,10}", href.replace(" ", ""))
                if m:
                    phones.append(m.group(0))

            if not phones:
                try:
                    from app.utils import extract_phone_numbers

                    phones = extract_phone_numbers(text)
                except ImportError:
                    # Резервный вариант
                    candidates = re.findall(r"(?:\+?359[\s-]?)?(?:0[2-9]\d{8})", text)
                    phones = [c.replace(" ", "") for c in candidates]

            if phones:
                listing.phone = phones[0]

            # Извлечение email
            mailtos = soup.select("a[href^='mailto:']")
            emails: list[str] = []
            for a in mailtos:
                href = a.get("href", "")
                email = href.split(":", 1)[-1].split("?", 1)[0]
                if "@" in email:
                    emails.append(email)

            if not emails:
                try:
                    from app.utils import extract_emails

                    emails = extract_emails(text)
                except ImportError:
                    emails = re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, flags=re.I)

            if emails:
                listing.contact_email = emails[0]

            # Обновление размера/локации
            if not listing.size:
                listing.size = self._parse_size_from_text(text)
            if not listing.location:
                # Пытаемся извлечь местоположение из текста
                location_matches = re.findall(
                    r"Местоположение\s+(.*?)\s+(?:Цена|Ціна|Price)", text, flags=re.I | re.S
                )
                if location_matches:
                    listing.location = location_matches[0].replace("\n", " ").strip()

            # Имя контакта
            if listing.contact_name == "-" and listing.seller_name:
                listing.contact_name = listing.seller_name

            if (
                listing.contact_name == "-"
                and listing.contact_email
                and listing.contact_email != "-"
            ):
                domain = listing.contact_email.split("@", 1)[-1].split(".", 1)[0].strip()
                if domain:
                    listing.contact_name = domain[:1].upper() + domain[1:]

            return listing
        except Exception as e:
            logger.error(
                f"Error enriching listing {listing.ad_id} from {self.site_config.name}: {e}"
            )
            return listing

    def _parse_size_from_text(self, text: str) -> str:
        try:
            m = re.search(r"(\d+(?:[.,]\d+)?)\s*кв\.?\s*м", text, flags=re.I)
            return (m.group(1) if m else "").strip()
        except Exception:
            return ""


class MultiSiteScraper:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.scrapers = [BaseScraper(site_config, settings) for site_config in settings.sites]

    async def scrape_all_sites(self) -> list[ScrapedListing]:
        """Скрапит все сайты параллельно"""
        tasks = [scraper.scrape() for scraper in self.scrapers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_listings = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error scraping {self.settings.sites[i].name}: {result}")
            elif isinstance(result, list):
                all_listings.extend(result)

        return all_listings


def to_listing_rows(rows: Sequence[ScrapedListing]) -> list[tuple[str, ...]]:
    return [
        (
            r.ad_id,
            r.date_seen,
            r.title,
            r.price,
            r.location,
            r.size,
            r.link,
            r.phone,
            r.seller_name,
            r.ad_type,
            r.contact_name,
            r.contact_email,
            r.source_site,
        )
        for r in rows
    ]
