from __future__ import annotations

import re
from typing import Callable, Protocol

from bs4 import BeautifulSoup, Tag

from app.scraper.models import ScrapedListing

extract_names: Callable[[str], list[str]] | None
looks_like_person_name: Callable[[str], bool] | None

try:
    from utils import (
        extract_names as _extract_names,
        looks_like_person_name as _looks_like_person_name,
    )

    extract_names = _extract_names
    looks_like_person_name = _looks_like_person_name
except ImportError:  # pragma: no cover - fallback path for isolated runtimes
    extract_names = None
    looks_like_person_name = None

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", flags=re.I)
PROPERTY_NAME_TOKENS: tuple[str, ...] = (
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


class ImotiParserContext(Protocol):
    @staticmethod
    def _attr_str(tag: Tag | None, name: str, default: str = "") -> str: ...

    def _normalize_link(self, base_url: str, href: str | None) -> str: ...

    def _clean_text(self, value: str) -> str: ...

    def _is_listing_candidate(self, title: str) -> bool: ...

    def _pick_card_container(self, anchor: Tag) -> Tag: ...

    def _extract_ad_id(self, link: str) -> str: ...

    def _extract_price(self, text: str) -> str: ...

    def _extract_location(self, text: str) -> str: ...

    def _extract_size(self, text: str) -> str: ...

    def _extract_image(self, card: Tag | None, base_url: str) -> str: ...

    def _extract_seller_name(self, card: Tag | None) -> str: ...

    def _detect_ad_type(self, seller_name: str) -> str: ...

    def _passes_filters(self, listing: ScrapedListing) -> bool: ...

    def _extract_phone_from_text(self, text: str) -> str: ...


class ImotiSourceParser:
    def __init__(
        self,
        context: ImotiParserContext,
        source_site: str,
        selectors: dict[str, str],
    ) -> None:
        self._context = context
        self._source_site = source_site
        self._selectors = selectors

    def parse_listing_page(self, html: str, base_url: str) -> list[ScrapedListing]:
        soup = BeautifulSoup(html, "html.parser")
        exact_cards = soup.select("article.product-classic")
        if exact_cards:
            exact_results: list[ScrapedListing] = []
            seen_ids: set[str] = set()
            for article in exact_cards:
                listing = self._parse_exact_card(article, base_url)
                if listing is None or listing.ad_id in seen_ids:
                    continue
                seen_ids.add(listing.ad_id)
                if self._context._passes_filters(listing):
                    exact_results.append(listing)
            return exact_results

        soup = BeautifulSoup(html, "lxml")
        links = soup.select(self._selectors.get("link", "a[href*='/наеми/']"))
        results: list[ScrapedListing] = []
        seen_links: set[str] = set()

        for link_el in links:
            link = self._context._normalize_link(
                base_url,
                self._context._attr_str(link_el, "href"),
            )
            if not link or link in seen_links:
                continue
            seen_links.add(link)

            title = self._context._clean_text(link_el.get_text(" ", strip=True))
            if not self._context._is_listing_candidate(title):
                continue

            card = self._context._pick_card_container(link_el)
            card_text = card.get_text("\n", strip=True) if card else title
            seller_name = self._context._extract_seller_name(card)
            listing = ScrapedListing(
                ad_id=self._context._extract_ad_id(link),
                title=title,
                price=self._context._extract_price(card_text),
                location=self._context._extract_location(card_text),
                size=self._context._extract_size(card_text),
                link=link,
                image_url=self._context._extract_image(card, base_url),
                seller_name=seller_name,
                ad_type=self._context._detect_ad_type(seller_name),
            )
            if self._context._passes_filters(listing):
                results.append(listing)

        return results

    def _parse_exact_card(self, article: Tag, base_url: str) -> ScrapedListing | None:
        context = self._context
        title_anchor = article.select_one("h4.product-classic-title a")
        if title_anchor is None:
            return None

        link = context._normalize_link(base_url, context._attr_str(title_anchor, "href"))
        if not link:
            return None

        ad_id = context._extract_ad_id(link)
        if not ad_id:
            return None

        title = context._clean_text(title_anchor.get_text(strip=True))
        if not context._is_listing_candidate(title):
            return None

        price_el = article.select_one(".product-classic-price")
        if price_el:
            price_lines = [
                line.strip()
                for line in price_el.get_text("\n").splitlines()
                if line.strip()
            ]
            price = price_lines[0] if price_lines else ""
        else:
            price = ""

        location_el = article.select_one(".btext")
        location = context._clean_text(
            location_el.get_text(strip=True) if location_el else ""
        )

        size = ""
        for li in article.select(".product-classic-list li"):
            text = context._clean_text(li.get_text(strip=True))
            if "кв.м." in text or "м²" in text:
                size = text.replace("м2", "м²")
                break

        seller_name = self._extract_seller_name(article)
        phone = self._extract_phone(article)

        return ScrapedListing(
            ad_id=ad_id,
            title=title,
            price=price,
            location=location,
            size=size,
            link=link,
            image_url=context._extract_image(article, base_url),
            source_site=self._source_site,
            phone=phone,
            seller_name=seller_name,
            ad_type=context._detect_ad_type(seller_name),
        )

    def enrich_detail(self, soup: BeautifulSoup, listing: ScrapedListing) -> None:
        context = self._context

        if not self._looks_like_real_seller_name(listing.seller_name):
            listing.seller_name = ""

        for block in soup.select("div.block-person-link"):
            icon = block.select_one("span.icon")
            icon_classes = context._attr_str(icon, "class")
            block_text = context._clean_text(block.get_text(" ", strip=True))

            if "mdi-account" in icon_classes and block_text:
                listing.seller_name = block_text
                if not listing.contact_name or listing.contact_name == "-":
                    listing.contact_name = block_text
                continue

            if "mdi-phone" in icon_classes and not listing.phone:
                tel = block.select_one("a[href^='tel:']")
                phone_source = (
                    context._attr_str(tel, "href")
                    if tel is not None
                    else block_text
                )
                phone = context._extract_phone_from_text(phone_source)
                if phone:
                    listing.phone = phone
                continue

            if "mdi-email" in icon_classes and (
                not listing.contact_email or listing.contact_email == "-"
            ):
                email_anchor = block.select_one("a[href]")
                email_text = context._clean_text(
                    email_anchor.get_text(" ", strip=True)
                    if email_anchor is not None
                    else block_text
                )
                email_match = EMAIL_RE.search(email_text)
                if email_match:
                    listing.contact_email = email_match.group(0)

        if not listing.seller_name:
            for selector in (
                "h1 a",
                ".product-title a",
                ".property-title",
                "[class*='owner']",
                "[class*='seller']",
                "[class*='agency']",
            ):
                element = soup.select_one(selector)
                if element is None:
                    continue
                candidate = context._clean_text(element.get_text(" ", strip=True))
                if self._looks_like_real_seller_name(candidate):
                    listing.seller_name = candidate
                    break

        if not listing.contact_name or listing.contact_name == "-":
            listing.contact_name = self._extract_contact_name(soup)

        if listing.seller_name:
            listing.ad_type = context._detect_ad_type(listing.seller_name)

    def _extract_phone(self, article: Tag) -> str:
        context = self._context
        tel_link = article.select_one('a[href^="tel:"]')
        if tel_link:
            phone = context._extract_phone_from_text(
                context._attr_str(tel_link, "href")
            )
            if phone:
                return phone

        for selector in (
            "[class*='phone']",
            "[class*='tel']",
            ".contact-phone",
            ".phone-number",
            "span.phone",
        ):
            element = article.select_one(selector)
            if element is not None:
                phone = context._extract_phone_from_text(
                    element.get_text(" ", strip=True)
                )
                if phone:
                    return phone

        return context._extract_phone_from_text(article.get_text(" ", strip=True))

    def _extract_seller_name(self, article: Tag) -> str:
        context = self._context
        for selector in (
            ".product-classic-agency",
            ".agency-name",
            ".seller-name",
            "[class*='agency']",
            "[class*='seller']",
            ".block-info h3",
        ):
            element = article.select_one(selector)
            if element is None:
                continue
            name = context._clean_text(element.get_text(strip=True))
            if self._looks_like_real_seller_name(name):
                return name

        text_content = article.get_text(" ", strip=True)
        if extract_names is not None:
            names = extract_names(text_content)
            if names:
                return names[0]

        if looks_like_person_name is not None:
            chunks = [
                context._clean_text(chunk)
                for chunk in text_content.split("  ")
                if chunk.strip()
            ]
            for chunk in chunks:
                if looks_like_person_name(chunk):
                    return chunk
        return ""

    def _extract_contact_name(self, soup: BeautifulSoup) -> str:
        context = self._context
        for block in soup.select("div.block-person-link"):
            text = context._clean_text(block.get_text(" ", strip=True))
            if not text:
                continue
            if looks_like_person_name is not None and looks_like_person_name(text):
                return text

        for selector in (
            ".contact-name",
            ".agent-name",
            ".block-agent-contact .name",
            ".contact-person",
            "[class*='contact-name']",
        ):
            element = soup.select_one(selector)
            if element is None:
                continue
            candidate = context._clean_text(element.get_text(" ", strip=True))
            if not candidate:
                continue
            if looks_like_person_name is None or looks_like_person_name(candidate):
                return candidate

        return "-"

    def _looks_like_real_seller_name(self, value: str) -> bool:
        normalized = self._context._clean_text(value).lower()
        if not normalized:
            return False
        if "*" in normalized:
            return False
        if any(word in normalized for word in PROPERTY_NAME_TOKENS):
            return False
        if any(
            token in normalized
            for token in ("кв.м", "месец", "eur", "лв", "€", "$")
        ):
            return False
        return True
