from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import date
from typing import Sequence
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from app.core.config import Settings

APARTMENT_KEYWORDS: tuple[str, ...] = (
    "апартамент",
    "едностаен",
    "двустаен",
    "тристаен",
    "четиристаен",
    "многостаен",
)


@dataclass(slots=True)
class ScrapedListing:
    ad_id: str
    title: str
    price: str
    location: str
    size: str
    link: str
    phone: str = ""
    seller_name: str = ""
    ad_type: str = ""
    contact_name: str = "-"
    contact_email: str = "-"
    date_seen: str = ""


class AsyncImotiScraper:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._sem = asyncio.Semaphore(max(1, settings.scrape_concurrency))

    async def scrape(self) -> list[ScrapedListing]:
        today = date.today().isoformat()
        async with httpx.AsyncClient(timeout=self._settings.scrape_timeout_seconds) as client:
            tasks = [self._scrape_page(client, page) for page in range(1, self._settings.scrape_max_pages + 1)]
            page_results = await asyncio.gather(*tasks, return_exceptions=True)

        listings: list[ScrapedListing] = []
        seen: set[str] = set()
        for result in page_results:
            if isinstance(result, Exception):
                continue
            for row in result:
                if row.ad_id in seen:
                    continue
                row.date_seen = today
                seen.add(row.ad_id)
                listings.append(row)

        # "Общая информация" (город/квадратура/контакты) обычно находится на странице объявления,
        # поэтому делаем дополнительное обогащение (может быть медленнее, но дает заполненность полей).
        if listings:
            await self._enrich_with_detail_pages(client=None, listings=listings)  # type: ignore[arg-type]
        return listings

    async def _scrape_page(self, client: httpx.AsyncClient, page: int) -> list[ScrapedListing]:
        url = self._settings.scrape_base_url.format(page=page)
        async with self._sem:
            response = await client.get(url)
        if response.status_code != 200:
            return []
        return self._parse_listing_page(response.text, base_url=url)

    def _parse_listing_page(self, html: str, base_url: str) -> list[ScrapedListing]:
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("div.item, article, .property-box")
        results: list[ScrapedListing] = []
        for card in cards:
            parsed = self._parse_card(card, base_url)
            if parsed is None:
                continue
            if self._settings.city_filter and self._settings.city_filter.lower() not in parsed.location.lower():
                continue
            results.append(parsed)
        return results

    def _parse_location_from_url(self, link: str, ad_id: str) -> str:
        """
        URL обычно имеет структуру:
          /наеми/<category>/<city>/<district>-<ad_id>.htm
        Берем `<city>` и `<district>` (если есть) и подставляем в `location`.
        """
        parsed = urlparse(link)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) < 2:
            return ""
        city_slug = parts[-2]
        district_slug_full = parts[-1]

        city = city_slug.replace("-", " ").strip()
        if city:
            city = city[:1].upper() + city[1:]

        district = re.sub(rf"-{re.escape(ad_id)}\.(?:htm|html)$", "", district_slug_full)
        district = district.replace("-", " ").strip()
        if not district or district == city_slug:
            return city
        return f"{city}, {district}"

    def _parse_size_from_text(self, text: str) -> str:
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*кв\.?\s*м", text, flags=re.I)
        return (m.group(1) if m else "").strip()

    async def _enrich_with_detail_pages(self, client: httpx.AsyncClient | None, listings: list[ScrapedListing]) -> None:
        """
        Подгружаем страницы конкретных объявлений и заполняем:
        - `phone`
        - `contact_email`
        - `contact_name` (если явного контакта нет, используем `seller_name`)
        - при необходимости корректируем `location`/`size`
        """
        async with httpx.AsyncClient(timeout=self._settings.scrape_timeout_seconds) as local_client:
            http = client or local_client
            tasks = [self._enrich_one(http, row) for row in listings]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                continue

    async def _enrich_one(self, client: httpx.AsyncClient, listing: ScrapedListing) -> ScrapedListing:
        try:
            async with self._sem:
                response = await client.get(listing.link)
            if response.status_code != 200:
                return listing

            soup = BeautifulSoup(response.text, "lxml")
            text = soup.get_text(" ", strip=True)

            # Phone: пробуем сначала tel: ссылки, потом текст.
            tel_links = soup.select("a[href^='tel:']")
            phones: list[str] = []
            for a in tel_links:
                href = a.get("href", "")
                # tel:+359xxxxxxxxx or tel:0xxxxxxxxx
                m = re.search(r"(?:\+?359)?[0-9]{9,10}", href.replace(" ", ""))
                if m:
                    phones.append(m.group(0))

            if not phones:
                # Упрощенный поиск номеров (обычно 087... или 070...)
                candidates = re.findall(r"(?:\+?359[\s-]?)?(?:0[2-9]\d{8})", text)
                phones = [c.replace(" ", "") for c in candidates]

            if phones:
                listing.phone = phones[0]

            # Email: mailto: либо регулярка.
            mailtos = soup.select("a[href^='mailto:']")
            emails: list[str] = []
            for a in mailtos:
                href = a.get("href", "")
                email = href.split(":", 1)[-1].split("?", 1)[0]
                if "@" in email:
                    emails.append(email)

            if not emails:
                emails = re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, flags=re.I)

            if emails:
                listing.contact_email = emails[0]

            # Размер/локация иногда есть на странице объявления.
            if not listing.size:
                listing.size = self._parse_size_from_text(text)
            if not listing.location:
                m_loc = re.search(r"Местоположение\s+(.*?)\s+Цена", text, flags=re.I | re.S)
                if m_loc:
                    district = re.sub(r"\s+", " ", m_loc.group(1)).strip()
                    city = self._parse_location_from_url(listing.link, listing.ad_id).split(",", 1)[0].strip()
                    listing.location = f"{city}, {district}" if city and district else district or listing.location

            # Имя контакта: часто это форма без имени.
            if listing.contact_name == "-" and listing.seller_name:
                listing.contact_name = listing.seller_name
            # Если на карточке/странице не удалось выделить конкретное имя,
            # используем домен из email как "общее" имя контакта.
            if listing.contact_name == "-" and listing.contact_email and listing.contact_email != "-":
                # office@imoti.bg -> imoti
                domain = listing.contact_email.split("@", 1)[-1].split(".", 1)[0].strip()
                if domain:
                    listing.contact_name = domain[:1].upper() + domain[1:]

            return listing
        except Exception:  # noqa: BLE001
            return listing

    def _parse_card(self, card: BeautifulSoup, base_url: str) -> ScrapedListing | None:
        link_el = card.select_one("a[href]")
        href = link_el.get("href", "").strip() if link_el else ""
        if not href:
            return None
        link = urljoin(base_url, href)

        # Title/text on the page is inside an <a> tag (often without `title` attribute),
        # so we need to derive it from link text that contains apartment keywords.
        title = ""
        for a in card.select("a[href]"):
            t = a.get_text(" ", strip=True).strip()
            if t and any(word in t.lower() for word in APARTMENT_KEYWORDS):
                title = t
                break
        if not title:
            title_el = card.select_one("h2, h3, .title, a[title]")
            title = (title_el.get_text(" ", strip=True) if title_el else "").strip()
        if not title and link_el:
            title = link_el.get_text(" ", strip=True).strip()

        if not title or not any(word in title.lower() for word in APARTMENT_KEYWORDS):
            return None

        # The ad id is typically the trailing number in URLs like:
        #   .../двустаен-апартамент/...-514546.htm
        ad_id_match = (
            re.search(r"/(\d{6,})", link)
            or re.search(r"ad_id=(\d+)", link)
            or re.search(r"-([0-9]{6,})(?:\.htm|\.html)?$", link)
        )
        ad_id = ad_id_match.group(1) if ad_id_match else ""
        if not ad_id:
            return None

        card_text = card.get_text(" ", strip=True)
        location_from_url = self._parse_location_from_url(link, ad_id)
        size_from_text = self._parse_size_from_text(card_text)

        price_el = card.select_one(".price, .item-price, [class*='price']")
        location_el = card.select_one(".location, .item-location, [class*='location']")
        size_el = card.select_one(".size, [class*='area'], [class*='size']")
        seller_el = card.select_one(".seller, [class*='agency'], [class*='owner']")

        seller_name = (seller_el.get_text(" ", strip=True) if seller_el else "").strip()
        # Если на карточке не получилось выделить агентство/продавца селектором,
        # берем имя "наивным" парсингом из текста:
        #   "... Двустаен апартамент <SELLER> Пловдив, Център ..."
        if not seller_name:
            normalized = re.sub(r"\s+", " ", card_text).strip()
            city = location_from_url.split(",", 1)[0].strip() if location_from_url else ""
            if city:
                pos_title = normalized.lower().find(title.lower())
                pos_city = normalized.lower().find(city.lower())
                if pos_title != -1 and pos_city != -1 and pos_city > pos_title:
                    candidate = normalized[pos_title + len(title) : pos_city].strip(" ,-/")
                    # Иногда рядом бывают лишние токены; попробуем оставить только "длинную" часть.
                    candidate = re.sub(r"^[0-9\s]+", "", candidate).strip()
                    if 2 <= len(candidate) <= 120:
                        seller_name = candidate
        ad_type = "від агенції" if "аген" in seller_name.lower() else "приватний"

        return ScrapedListing(
            ad_id=ad_id,
            title=title,
            price=(price_el.get_text(" ", strip=True) if price_el else "").strip(),
            location=((location_el.get_text(" ", strip=True) if location_el else "").strip() or location_from_url),
            size=(size_el.get_text(" ", strip=True) if size_el else "").strip() or size_from_text,
            link=link,
            seller_name=seller_name,
            ad_type=ad_type,
        )


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
        )
        for r in rows
    ]
