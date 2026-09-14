from __future__ import annotations

import asyncio
import random
import re
from dataclasses import dataclass
from datetime import date
from typing import Sequence
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag

from app.core.config import Settings, SiteConfig
from app.core.logging import get_logger

try:
    from utils import extract_names, extract_phone_numbers, looks_like_person_name, normalize_phone_number
except ImportError:  # pragma: no cover - fallback path for isolated runtimes
    extract_names = None
    extract_phone_numbers = None
    looks_like_person_name = None
    normalize_phone_number = None

logger = get_logger("async_scraper")

APARTMENT_KEYWORDS: tuple[str, ...] = (
    "апартамент",
    "апартаменти",
    "едностаен",
    "двустаен",
    "тристаен",
    "четиристаен",
    "многостаен",
    "квартира",
    "квартири",
    "аренда",
    "наем",
    "оренда",
    "жилье",
    "житло",
    "apartment",
    "flat",
    "rent",
)
NEGATIVE_TITLE_KEYWORDS: tuple[str, ...] = (
    "подобово",
    "посуточно",
    "новини",
    "новости",
    "реклама",
)
ALO_REQUIRED_KEYWORDS: tuple[str, ...] = (
    "апартамент",
    "квартира",
    "студио",
)
ALO_RENT_KEYWORDS: tuple[str, ...] = (
    "под наем",
    "апартаменти под наем",
    "квартира под наем",
)
ALO_NEGATIVE_KEYWORDS: tuple[str, ...] = (
    "нощувк",
    "хамалск",
    "транспортн",
    "почистван",
    "товарни",
    "извозва",
)
PRICE_RE = re.compile(
    r"(?P<amount>\d[\d\s.,]{2,})\s*(?P<currency>EUR|BGN|USD|UAH|грн\.?|лв\.?|€|\$)",
    flags=re.I,
)
SIZE_RE = re.compile(
    r"(?P<size>\d+(?:[.,]\d+)?)\s*(?:м²|m²|sqm|sq\.?\s*m|кв\.?\s*м)",
    flags=re.I,
)
PHONE_RE = re.compile(
    r"(?:\+?359[\s-]?\d[\d\s-]{7,12}|\+?380[\s-]?\d[\d\s-]{8,12}|0\d[\d\s-]{7,11})"
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", flags=re.I)


@dataclass(slots=True)
class ScrapedListing:
    ad_id: str
    title: str
    price: str
    location: str
    size: str
    link: str
    image_url: str = ""
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
        self._sem = asyncio.Semaphore(max(1, site_config.concurrency or settings.scrape_concurrency))
        self._proxy_index = 0

    def _get_client_kwargs(self) -> dict[str, object]:
        headers = {
            "User-Agent": self._pick_user_agent(),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7,bg;q=0.6",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "Connection": "keep-alive",
        }
        kwargs: dict[str, object] = {
            "timeout": httpx.Timeout(self.site_config.timeout or self.settings.scrape_timeout_seconds),
            "headers": headers,
            "follow_redirects": self.settings.scrape_follow_redirects,
            "verify": self.site_config.verify_ssl if self.site_config else self.settings.scrape_verify_ssl,
            "limits": httpx.Limits(
                max_connections=self.settings.http_max_connections,
                max_keepalive_connections=self.settings.http_max_keepalive_connections,
            ),
        }
        proxy = self._pick_proxy()
        if proxy:
            kwargs["proxy"] = proxy
        return kwargs

    def _pick_user_agent(self) -> str:
        user_agents = self.settings.user_agents or [self.settings.user_agent]
        return random.choice(user_agents)

    def _pick_proxy(self) -> str | None:
        pool = self.settings.proxy_pool
        if not self.settings.proxy_enabled or not pool:
            return None
        strategy = self.settings.proxy_rotation_strategy
        if strategy == "round_robin":
            proxy = pool[self._proxy_index % len(pool)]
            self._proxy_index += 1
            return proxy
        return random.choice(pool)

    async def scrape(self) -> list[ScrapedListing]:
        today = date.today().isoformat()
        async with httpx.AsyncClient(**self._get_client_kwargs()) as client:
            tasks = [
                self._scrape_page(client, page)
                for page in range(1, self.site_config.max_pages + 1)
                if self.site_config.enabled
            ]
            page_results = await asyncio.gather(*tasks, return_exceptions=True)

            listings: list[ScrapedListing] = []
            seen_ids: set[str] = set()
            for result in page_results:
                if isinstance(result, Exception):
                    logger.exception(
                        "Page scraping failed",
                        site=self.site_config.name,
                        error=str(result),
                    )
                    continue
                for row in result:
                    if row.ad_id in seen_ids:
                        continue
                    row.date_seen = today
                    row.source_site = self.site_config.name
                    seen_ids.add(row.ad_id)
                    listings.append(row)

            if listings and self.settings.scrape_detail_pages and self.site_config.detail_pages_enabled:
                await self._enrich_with_detail_pages(client, listings)

        logger.info(
            "Site scrape completed",
            site=self.site_config.name,
            extracted=len(listings),
        )
        return listings

    async def _scrape_page(self, client: httpx.AsyncClient, page: int) -> list[ScrapedListing]:
        url = self._build_page_url(page)
        response = await self._request_with_retries(client, url=url, context=f"list page {page}")
        if response is None:
            return []
        return self._parse_listing_page(response.text, base_url=str(response.url))

    async def _request_with_retries(
        self,
        client: httpx.AsyncClient,
        url: str,
        context: str,
    ) -> httpx.Response | None:
        response: httpx.Response | None = None
        for attempt in range(1, self.settings.scrape_retry_count + 1):
            try:
                async with self._sem:
                    await self._polite_delay()
                    response = await client.get(url)

                if response.status_code == 404:
                    logger.warning(
                        "Page returned 404",
                        site=self.site_config.name,
                        url=url,
                        context=context,
                    )
                    return None

                if response.status_code in {403, 408, 425, 429, 500, 502, 503, 504}:
                    if attempt == self.settings.scrape_retry_count:
                        logger.warning(
                            "Request exhausted retries",
                            site=self.site_config.name,
                            url=url,
                            status_code=response.status_code,
                            context=context,
                        )
                        return None
                    await self._sleep_backoff(attempt)
                    continue

                response.raise_for_status()
                return response
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                if attempt == self.settings.scrape_retry_count:
                    logger.warning(
                        "Request failed",
                        site=self.site_config.name,
                        url=url,
                        context=context,
                        error=str(exc),
                    )
                    return None
                await self._sleep_backoff(attempt)
        return response

    async def _polite_delay(self) -> None:
        low = min(self.settings.scrape_delay_min_seconds, self.settings.scrape_delay_max_seconds)
        high = max(self.settings.scrape_delay_min_seconds, self.settings.scrape_delay_max_seconds)
        if high > 0:
            await asyncio.sleep(random.uniform(low, high))

    async def _sleep_backoff(self, attempt: int) -> None:
        delay = min(
            self.settings.scrape_backoff_cap_seconds,
            self.settings.scrape_backoff_base_seconds * (2 ** (attempt - 1)),
        )
        jitter = random.uniform(0, 0.5)
        await asyncio.sleep(delay + jitter)

    def _build_page_url(self, page: int) -> str:
        base_url = self.site_config.base_url
        if "{page}" not in base_url:
            return base_url if page == 1 else f"{base_url}?page={page}"
        if page == 1 and self.site_config.name == "imoti.bg":
            return base_url.replace("/page:{page}", "")
        return base_url.format(page=page)

    def _parse_listing_page(self, html: str, base_url: str) -> list[ScrapedListing]:
        parsers = {
            "imoti.bg": self._parse_imoti_listing_page,
            "alo.bg": self._parse_alo_listing_page,
            "dom.ria.com": self._parse_generic_anchor_page,
            "olx.ua": self._parse_generic_anchor_page,
            "lun.ua": self._parse_generic_anchor_page,
        }
        parser = parsers.get(self.site_config.name, self._parse_generic_cards)
        return parser(html=html, base_url=base_url)

    def _parse_imoti_listing_page(self, html: str, base_url: str) -> list[ScrapedListing]:
        soup = BeautifulSoup(html, "html.parser")
        exact_cards = soup.select("article.product-classic")
        if exact_cards:
            results: list[ScrapedListing] = []
            seen_ids: set[str] = set()
            for article in exact_cards:
                listing = self._parse_imoti_card_exact(article, base_url)
                if listing is None or listing.ad_id in seen_ids:
                    continue
                seen_ids.add(listing.ad_id)
                if self._passes_filters(listing):
                    results.append(listing)
            return results

        soup = BeautifulSoup(html, "lxml")
        links = soup.select(self.site_config.selectors.get("link", "a[href*='/наеми/']"))
        results: list[ScrapedListing] = []
        seen_links: set[str] = set()

        for link_el in links:
            link = self._normalize_link(base_url, link_el.get("href"))
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            title = self._clean_text(link_el.get_text(" ", strip=True))
            if not self._is_listing_candidate(title):
                continue

            card = self._pick_card_container(link_el)
            card_text = card.get_text("\n", strip=True) if card else title
            listing = ScrapedListing(
                ad_id=self._extract_ad_id(link),
                title=title,
                price=self._extract_price(card_text),
                location=self._extract_location(card_text),
                size=self._extract_size(card_text),
                link=link,
                image_url=self._extract_image(card, base_url),
                seller_name=self._extract_seller_name(card),
                ad_type=self._detect_ad_type(self._extract_seller_name(card)),
            )
            if self._passes_filters(listing):
                results.append(listing)

        return results

    def _parse_imoti_card_exact(self, article: Tag, base_url: str) -> ScrapedListing | None:
        title_anchor = article.select_one("h4.product-classic-title a")
        if title_anchor is None:
            return None

        link = self._normalize_link(base_url, title_anchor.get("href"))
        if not link:
            return None

        ad_id = self._extract_ad_id(link)
        if not ad_id:
            return None

        title = self._clean_text(title_anchor.get_text(strip=True))
        if not self._is_listing_candidate(title):
            return None

        price_el = article.select_one(".product-classic-price")
        if price_el:
            price_lines = [line.strip() for line in price_el.get_text("\n").splitlines() if line.strip()]
            price = price_lines[0] if price_lines else ""
        else:
            price = ""

        location_el = article.select_one(".btext")
        location = self._clean_text(location_el.get_text(strip=True) if location_el else "")

        size = ""
        for li in article.select(".product-classic-list li"):
            text = self._clean_text(li.get_text(strip=True))
            if "кв.м." in text or "м²" in text:
                size = text.replace("м2", "м²")
                break

        seller_name = self._extract_seller_name_from_imoti_card(article)
        phone = self._extract_phone_from_imoti_card(article)

        return ScrapedListing(
            ad_id=ad_id,
            title=title,
            price=price,
            location=location,
            size=size,
            link=link,
            image_url=self._extract_image(article, base_url),
            source_site=self.site_config.name,
            phone=phone,
            seller_name=seller_name,
            ad_type=self._detect_ad_type(seller_name),
        )

    def _parse_alo_listing_page(self, html: str, base_url: str) -> list[ScrapedListing]:
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("div.ad_block_normal")
        results: list[ScrapedListing] = []
        seen_ids: set[str] = set()

        for card in cards:
            listing = self._parse_alo_card(card, base_url)
            if listing is None or listing.ad_id in seen_ids:
                continue
            seen_ids.add(listing.ad_id)
            if self._passes_filters(listing):
                results.append(listing)

        return results

    def _parse_alo_card(self, card: Tag, base_url: str) -> ScrapedListing | None:
        link_el = card.select_one("a.avn_seo[href], a.avn_image[href], a[href]")
        if link_el is None:
            return None

        link = self._normalize_link(base_url, link_el.get("href"))
        if not link:
            return None

        title_el = card.select_one("a.avn_seo[href]")
        title = self._clean_text(
            title_el.get_text(" ", strip=True) if title_el is not None else link_el.get_text(" ", strip=True)
        )
        if not title:
            title = self._title_from_url(link)

        image = card.select_one("a.avn_image img[alt], img[alt]")
        image_alt = self._clean_text(image.get("alt", "") if image is not None else "")
        card_text = self._clean_text(card.get_text(" ", strip=True))
        combined_text = self._clean_text(f"{title} {image_alt} {card_text}")

        if not self._is_alo_listing_candidate(title=title, combined_text=combined_text):
            return None

        price_el = card.select_one(".avn_price")
        price = self._clean_text(price_el.get_text(" ", strip=True) if price_el is not None else "")

        location_el = card.select_one(".avn_location")
        location = self._clean_text(location_el.get_text(" ", strip=True) if location_el is not None else "")

        ad_id_match = re.search(r"adrows_(\d{4,12})", " ".join(card.get("id", []) if isinstance(card.get("id"), list) else [card.get("id", "")]))
        ad_id = ad_id_match.group(1) if ad_id_match else self._extract_ad_id(link)

        return ScrapedListing(
            ad_id=ad_id,
            title=title,
            price=price,
            location=location,
            size=self._extract_size(combined_text),
            link=link,
            image_url=self._extract_image(card, base_url),
            source_site=self.site_config.name,
        )

    def _parse_generic_cards(self, html: str, base_url: str) -> list[ScrapedListing]:
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select(self.site_config.selectors.get("card", "article, li, div"))
        results: list[ScrapedListing] = []
        seen_ids: set[str] = set()

        for card in cards:
            listing = self._parse_card(card, base_url)
            if listing is None or listing.ad_id in seen_ids:
                continue
            seen_ids.add(listing.ad_id)
            if self._passes_filters(listing):
                results.append(listing)

        return results

    def _parse_generic_anchor_page(self, html: str, base_url: str) -> list[ScrapedListing]:
        soup = BeautifulSoup(html, "lxml")
        results: list[ScrapedListing] = []
        seen_links: set[str] = set()

        for anchor in soup.select("a[href]"):
            link = self._normalize_link(base_url, anchor.get("href"))
            if not link or link in seen_links or not self._link_looks_like_listing(link):
                continue

            card = self._pick_card_container(anchor)
            card_text = self._clean_text(card.get_text("\n", strip=True) if card else "")
            title = self._clean_text(anchor.get_text(" ", strip=True)) or self._title_from_url(link)

            if not self._looks_like_property_block(title=title, card_text=card_text):
                continue

            seen_links.add(link)
            listing = ScrapedListing(
                ad_id=self._extract_ad_id(link),
                title=title,
                price=self._extract_price(card_text),
                location=self._extract_location(card_text),
                size=self._extract_size(card_text),
                link=link,
                image_url=self._extract_image(card, base_url),
                seller_name=self._extract_seller_name(card),
                ad_type=self._detect_ad_type(self._extract_seller_name(card)),
            )
            if self._passes_filters(listing):
                results.append(listing)

        return results

    def _parse_card(self, card: Tag, base_url: str) -> ScrapedListing | None:
        try:
            link_selector = self.site_config.selectors.get("link", "a[href]")
            link_el = card.select_one(link_selector)
            link = self._normalize_link(base_url, link_el.get("href") if link_el else None)
            if not link:
                return None

            title_selector = self.site_config.selectors.get("title", "h2, h3, .title, a[href]")
            title_el = card.select_one(title_selector)
            title = self._clean_text(title_el.get_text(" ", strip=True) if title_el else "")
            if not title and link_el:
                title = self._clean_text(link_el.get_text(" ", strip=True))

            card_text = self._clean_text(card.get_text(" ", strip=True))
            if not self._looks_like_property_block(title=title, card_text=card_text):
                return None

            seller_selector = self.site_config.selectors.get(
                "seller", ".seller, [class*='agency'], [class*='owner']"
            )
            seller_el = card.select_one(seller_selector)
            seller_name = self._clean_text(seller_el.get_text(" ", strip=True) if seller_el else "")

            return ScrapedListing(
                ad_id=self._extract_ad_id(link),
                title=title or self._title_from_url(link),
                price=self._extract_price(card_text),
                location=self._extract_location(card_text),
                size=self._extract_size(card_text),
                link=link,
                image_url=self._extract_image(card, base_url),
                seller_name=seller_name,
                ad_type=self._detect_ad_type(seller_name),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to parse card",
                site=self.site_config.name,
                error=str(exc),
            )
            return None

    async def _enrich_with_detail_pages(
        self,
        client: httpx.AsyncClient,
        listings: list[ScrapedListing],
    ) -> None:
        tasks = [self._enrich_one(client, listing) for listing in listings]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Detail page enrichment failed", error=str(result), site=self.site_config.name)

    async def _enrich_one(self, client: httpx.AsyncClient, listing: ScrapedListing) -> ScrapedListing:
        response = await self._request_with_retries(client, url=listing.link, context="detail page")
        if response is None:
            return listing

        soup = BeautifulSoup(response.text, "lxml")
        if self.site_config.name == "imoti.bg":
            self._enrich_imoti_detail(soup, listing)
        elif self.site_config.name == "alo.bg":
            self._enrich_alo_detail(soup, listing)
        else:
            self._enrich_generic_detail(soup, listing)

        return listing

    def _enrich_generic_detail(self, soup: BeautifulSoup, listing: ScrapedListing) -> None:
        text = self._clean_text(soup.get_text(" ", strip=True))

        detail_phone = self._extract_phone_from_detail_soup(soup)
        if detail_phone:
            listing.phone = detail_phone

        mailto_links = soup.select("a[href^='mailto:']")
        emails = [
            a.get("href", "").split(":", 1)[-1].split("?", 1)[0].strip()
            for a in mailto_links
            if a.get("href")
        ]
        if not emails:
            emails = EMAIL_RE.findall(text)
        if emails:
            listing.contact_email = emails[0]

        if not listing.location:
            listing.location = self._extract_location(text)
        if not listing.size:
            listing.size = self._extract_size(text)
        if not listing.contact_name or listing.contact_name == "-":
            listing.contact_name = listing.seller_name or self._guess_contact_name(text)

    def _enrich_imoti_detail(self, soup: BeautifulSoup, listing: ScrapedListing) -> None:
        if not self._looks_like_real_seller_name(listing.seller_name):
            listing.seller_name = ""

        for block in soup.select("div.block-person-link"):
            icon = block.select_one("span.icon")
            icon_classes = " ".join(icon.get("class", [])) if icon is not None else ""
            block_text = self._clean_text(block.get_text(" ", strip=True))

            if "mdi-account" in icon_classes and block_text:
                listing.seller_name = block_text
                if not listing.contact_name or listing.contact_name == "-":
                    listing.contact_name = block_text
                continue

            if "mdi-phone" in icon_classes and not listing.phone:
                tel = block.select_one("a[href^='tel:']")
                phone_source = tel.get("href", "") if tel is not None else block_text
                phone = self._extract_phone_from_text(phone_source)
                if phone:
                    listing.phone = phone
                continue

            if "mdi-email" in icon_classes and (not listing.contact_email or listing.contact_email == "-"):
                email_anchor = block.select_one("a[href]")
                email_text = self._clean_text(
                    email_anchor.get_text(" ", strip=True) if email_anchor is not None else block_text
                )
                email_match = EMAIL_RE.search(email_text)
                if email_match:
                    listing.contact_email = email_match.group(0)

        if not listing.seller_name:
            for sel in (
                "h1 a",
                ".product-title a",
                ".property-title",
                "[class*='owner']",
                "[class*='seller']",
                "[class*='agency']",
            ):
                el = soup.select_one(sel)
                if el is not None:
                    candidate = self._clean_text(el.get_text(" ", strip=True))
                    if self._looks_like_real_seller_name(candidate):
                        listing.seller_name = candidate
                        break

        if not listing.contact_name or listing.contact_name == "-":
            listing.contact_name = self._extract_contact_name_from_detail_soup(soup)
        if listing.seller_name:
            listing.ad_type = self._detect_ad_type(listing.seller_name)

    def _enrich_alo_detail(self, soup: BeautifulSoup, listing: ScrapedListing) -> None:
        title_el = soup.select_one("h1.large-headline, h1")
        if title_el is not None:
            title = self._clean_text(title_el.get_text(" ", strip=True))
            if title:
                listing.title = title

        price_text = self._extract_alo_detail_price(soup)
        if price_text:
            listing.price = price_text

        params = self._extract_alo_params(soup)
        if params.get("Местоположение"):
            listing.location = params["Местоположение"]
        if params.get("Квадратура"):
            listing.size = params["Квадратура"].replace("\xa0", " ")

        seller_name = self._extract_alo_seller_name(soup)
        if seller_name:
            listing.seller_name = seller_name

        contact_name = self._extract_alo_contact_name(soup)
        if contact_name:
            listing.contact_name = contact_name

        visible_phone = self._extract_alo_phone(soup)
        if visible_phone:
            listing.phone = visible_phone

        if soup.select_one(".contacts_wrapper_flex.has_agents"):
            listing.ad_type = "agency"
        elif listing.seller_name:
            listing.ad_type = self._detect_ad_type(listing.seller_name)
        else:
            listing.ad_type = "private"

    def _extract_alo_detail_price(self, soup: BeautifulSoup) -> str:
        for cell in soup.select(".ads-params-price"):
            text = self._clean_text(cell.get_text(" ", strip=True))
            if "€" in text or "лв" in text.lower():
                return text.split("Цената е около", 1)[0].strip()
        return ""

    def _extract_alo_params(self, soup: BeautifulSoup) -> dict[str, str]:
        params: dict[str, str] = {}
        for row in soup.select(".ads-params-row"):
            title_el = row.select_one(".ads-param-title")
            value_candidates = row.select(".ads-params-cell")
            if title_el is None or len(value_candidates) < 2:
                continue
            title = self._clean_text(title_el.get_text(" ", strip=True))
            value = self._clean_text(value_candidates[-1].get_text(" ", strip=True))
            if title and value:
                params[title] = value
        return params

    def _extract_alo_seller_name(self, soup: BeautifulSoup) -> str:
        header = soup.select_one(".contacts.header")
        if header is None:
            return ""

        for candidate in header.stripped_strings:
            value = self._clean_text(candidate)
            if not value:
                continue
            if value in {"Контакт с подателя", "Контакт с подателя на обявата", "Изпрати съобщение"}:
                continue
            if value.endswith(".alo.bg"):
                continue
            if "Вход" in value and "Регистрация" in value:
                continue
            if "*" in value:
                continue
            return value
        return ""

    def _extract_alo_contact_name(self, soup: BeautifulSoup) -> str:
        el = soup.select_one(".agent_div .contact_value, .contact_value")
        return self._clean_text(el.get_text(" ", strip=True) if el is not None else "")

    def _extract_alo_phone(self, soup: BeautifulSoup) -> str:
        for span in soup.select(".contact_phone .ocd_span, .ocd_span"):
            masked = self._clean_text(span.get_text(" ", strip=True))
            if "X" in masked.upper():
                continue
            phone = self._extract_phone_from_text(masked)
            if phone:
                return phone
        return ""

    def _looks_like_real_seller_name(self, value: str) -> bool:
        normalized = self._clean_text(value).lower()
        if not normalized:
            return False
        if "*" in normalized:
            return False
        if any(word in normalized for word in APARTMENT_KEYWORDS):
            return False
        if any(token in normalized for token in ("кв.м", "месец", "eur", "лв", "€", "$")):
            return False
        return True

    def _extract_contact_name_from_detail_soup(self, soup: BeautifulSoup) -> str:
        for block in soup.select("div.block-person-link"):
            text = self._clean_text(block.get_text(" ", strip=True))
            if not text:
                continue
            if looks_like_person_name is not None and looks_like_person_name(text):
                return text

        for sel in (
            ".contact-name",
            ".agent-name",
            ".block-agent-contact .name",
            ".contact-person",
            "[class*='contact-name']",
        ):
            el = soup.select_one(sel)
            if el is None:
                continue
            candidate = self._clean_text(el.get_text(" ", strip=True))
            if not candidate:
                continue
            if looks_like_person_name is None or looks_like_person_name(candidate):
                return candidate

        return "-"

    def _extract_contact_email_from_detail_soup(self, soup: BeautifulSoup) -> str:
        for root in soup.select("div.block-person-link, div.block-info, div.block-agent"):
            mail = root.select_one("a[href^='mailto:']")
            if mail is not None:
                email = mail.get("href", "").replace("mailto:", "").split("?", 1)[0].strip()
                if email:
                    return email
            match = EMAIL_RE.search(root.get_text(" ", strip=True))
            if match:
                return match.group(0)

        mail = soup.select_one("a[href^='mailto:']")
        if mail is not None:
            email = mail.get("href", "").replace("mailto:", "").split("?", 1)[0].strip()
            if email:
                return email

        match = EMAIL_RE.search(soup.get_text(" ", strip=True))
        return match.group(0) if match else "-"

    def _passes_filters(self, listing: ScrapedListing) -> bool:
        if self.settings.city_filter and self.settings.city_filter.lower() not in listing.location.lower():
            return False
        return True

    def _normalize_link(self, base_url: str, href: str | None) -> str:
        if not href:
            return ""
        href = href.strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            return ""
        link = urljoin(base_url, href)
        parsed = urlparse(link)
        if parsed.scheme not in {"http", "https"}:
            return ""
        return link

    def _pick_card_container(self, anchor: Tag) -> Tag:
        parent = anchor
        for _ in range(6):
            parent = parent.parent if isinstance(parent.parent, Tag) else parent
            if parent.name in {"article", "li", "section", "div"}:
                return parent
        return anchor

    def _clean_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", value or "").strip()

    def _clean_phone(self, value: str) -> str:
        if not value:
            return ""
        if normalize_phone_number is not None:
            normalized = normalize_phone_number(value)
            if normalized:
                return normalized
        cleaned = re.sub(r"[^\d+]", "", value)
        return cleaned[:20]

    def _extract_phone_from_text(self, text: str) -> str:
        if not text:
            return ""
        if extract_phone_numbers is not None:
            phones = extract_phone_numbers(text)
            if phones:
                return phones[0]
        fallback = PHONE_RE.search(text)
        return self._clean_phone(fallback.group(0)) if fallback else ""

    def _extract_phone_from_detail_soup(self, soup: BeautifulSoup) -> str:
        tel_link = soup.select_one("a[href^='tel:']")
        if tel_link is not None:
            phone = self._extract_phone_from_text(tel_link.get("href", ""))
            if phone:
                return phone

        for selector in (
            ".phone-number",
            ".contact-phone",
            "[class*='phone']",
            "[class*='tel']",
        ):
            el = soup.select_one(selector)
            if el is not None:
                phone = self._extract_phone_from_text(el.get_text(" ", strip=True))
                if phone:
                    return phone

        return self._extract_phone_from_text(soup.get_text(" ", strip=True))

    def _is_listing_candidate(self, title: str) -> bool:
        normalized = title.lower()
        return any(word in normalized for word in APARTMENT_KEYWORDS) and not any(
            word in normalized for word in NEGATIVE_TITLE_KEYWORDS
        )

    def _is_alo_listing_candidate(self, title: str, combined_text: str) -> bool:
        normalized = f"{title} {combined_text}".lower()
        has_property = any(word in normalized for word in ALO_REQUIRED_KEYWORDS)
        has_rent = any(word in normalized for word in ALO_RENT_KEYWORDS)
        is_negative = any(word in normalized for word in ALO_NEGATIVE_KEYWORDS)
        return has_property and has_rent and not is_negative

    def _looks_like_property_block(self, title: str, card_text: str) -> bool:
        normalized = f"{title} {card_text}".lower()
        has_keyword = any(word in normalized for word in APARTMENT_KEYWORDS)
        has_price = PRICE_RE.search(card_text) is not None
        has_size = SIZE_RE.search(card_text) is not None
        has_rooms = any(token in normalized for token in ("кімнат", "комнат", "стаен", "room"))
        return (has_keyword or has_rooms) and (has_price or has_size)

    def _link_looks_like_listing(self, link: str) -> bool:
        parsed = urlparse(link)
        if self.site_config.allowed_domains and parsed.netloc not in self.site_config.allowed_domains:
            return False
        if self.site_config.listing_path_keywords:
            return any(keyword in parsed.path for keyword in self.site_config.listing_path_keywords)

        query = parse_qs(parsed.query)
        return any(query.values())

    def _extract_ad_id(self, link: str) -> str:
        patterns = (
            r"-(\d{4,12})(?:\.htm|\.html)?$",
            r"/(\d{4,12})(?:/)?$",
            r"ID([A-Za-z0-9]{5,16})",
            r"/([A-Za-z0-9]{8,20})\.html?$",
        )
        for pattern in patterns:
            match = re.search(pattern, link)
            if match:
                return match.group(1)
        return str(abs(hash(link)))[:12]

    def _extract_price(self, text: str) -> str:
        match = PRICE_RE.search(text)
        if not match:
            return ""
        amount = re.sub(r"\s+", " ", match.group("amount")).strip()
        currency = match.group("currency").upper().replace("ГРН.", "ГРН").replace("ЛВ.", "ЛВ")
        return f"{amount} {currency}"

    def _extract_size(self, text: str) -> str:
        match = SIZE_RE.search(text)
        return f"{match.group('size')} м²" if match else ""

    def _extract_location(self, text: str) -> str:
        chunks = [self._clean_text(chunk) for chunk in re.split(r"[\n|]+", text)]
        for chunk in chunks:
            if len(chunk) < 3:
                continue
            if any(token in chunk for token in (" · ", ", ")):
                if PRICE_RE.search(chunk) or SIZE_RE.search(chunk):
                    continue
                return chunk[:180]
        fallback = re.search(
            r"([A-ZА-ЯЁЇІЄ][^0-9,\n]{1,80},\s*[A-ZА-ЯЁЇІЄ][^0-9\n]{1,80})",
            text,
            flags=re.U,
        )
        if fallback:
            return self._clean_text(fallback.group(1))[:180]
        return ""

    def _extract_image(self, card: Tag | None, base_url: str) -> str:
        if card is None:
            return ""
        image = card.select_one("img[src], img[data-src]")
        if image is None:
            return ""
        return self._normalize_link(base_url, image.get("src") or image.get("data-src"))

    def _extract_seller_name(self, card: Tag | None) -> str:
        if card is None:
            return ""
        seller_selector = self.site_config.selectors.get(
            "seller", ".seller, [class*='agency'], [class*='owner'], [class*='broker']"
        )
        seller = card.select_one(seller_selector)
        return self._clean_text(seller.get_text(" ", strip=True) if seller else "")

    def _detect_ad_type(self, seller_name: str) -> str:
        normalized = seller_name.lower()
        if any(word in normalized for word in ("agency", "аген", "broker", "рієл", "agent")):
            return "agency"
        return "private"

    def _guess_contact_name(self, text: str) -> str:
        sentences = [chunk.strip() for chunk in re.split(r"[,.]", text) if chunk.strip()]
        for sentence in sentences[:10]:
            if len(sentence.split()) in {2, 3} and not PRICE_RE.search(sentence):
                return sentence[:120]
        return "-"

    def _extract_phone_from_imoti_card(self, article: Tag) -> str:
        tel_link = article.select_one('a[href^="tel:"]')
        if tel_link:
            phone = self._extract_phone_from_text(tel_link.get("href", ""))
            if phone:
                return phone

        for selector in (
            "[class*='phone']",
            "[class*='tel']",
            ".contact-phone",
            ".phone-number",
            "span.phone",
        ):
            el = article.select_one(selector)
            if el is not None:
                phone = self._extract_phone_from_text(el.get_text(" ", strip=True))
                if phone:
                    return phone

        full_text = article.get_text(" ", strip=True)
        return self._extract_phone_from_text(full_text)

    def _extract_seller_name_from_imoti_card(self, article: Tag) -> str:
        for selector in (
            ".product-classic-agency",
            ".agency-name",
            ".seller-name",
            "[class*='agency']",
            "[class*='seller']",
            ".block-info h3",
        ):
            el = article.select_one(selector)
            if el is not None:
                name = self._clean_text(el.get_text(strip=True))
                if self._looks_like_real_seller_name(name):
                    return name

        text_content = article.get_text(" ", strip=True)
        if extract_names is not None:
            names = extract_names(text_content)
            if names:
                return names[0]

        if looks_like_person_name is not None:
            chunks = [self._clean_text(chunk) for chunk in text_content.split("  ") if chunk.strip()]
            for chunk in chunks:
                if looks_like_person_name(chunk):
                    return chunk
        return ""

    def _title_from_url(self, link: str) -> str:
        path = urlparse(link).path.rstrip("/").split("/")[-1]
        path = re.sub(r"[-_]+", " ", path)
        path = re.sub(r"\.\w+$", "", path)
        return path.strip().title()[:200]


class AsyncImotiScraper(BaseScraper):
    """Backward-compatible alias for older imports."""


class MultiSiteScraper:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.scrapers = [BaseScraper(site_config, settings) for site_config in settings.sites if site_config.enabled]

    async def scrape_all_sites(self) -> list[ScrapedListing]:
        tasks = [scraper.scrape() for scraper in self.scrapers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_listings: list[ScrapedListing] = []
        for index, result in enumerate(results):
            if isinstance(result, Exception):
                logger.exception(
                    "Site scrape failed",
                    site=self.scrapers[index].site_config.name,
                    error=str(result),
                )
                continue
            logger.info(
                "Site results merged",
                site=self.scrapers[index].site_config.name,
                extracted=len(result),
            )
            all_listings.extend(result)

        deduplicated: dict[str, ScrapedListing] = {}
        for listing in all_listings:
            deduplicated[listing.ad_id] = listing
        return list(deduplicated.values())

    async def scrape(self) -> list[ScrapedListing]:
        return await self.scrape_all_sites()


def to_listing_rows(rows: Sequence[ScrapedListing]) -> list[tuple[str, ...]]:
    return [
        (
            row.ad_id,
            row.date_seen,
            row.title,
            row.price,
            row.location,
            row.size,
            row.link,
            row.source_site,
            row.phone,
            row.seller_name,
            row.ad_type,
            row.contact_name,
            row.contact_email,
        )
        for row in rows
    ]
