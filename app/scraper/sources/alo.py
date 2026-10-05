from __future__ import annotations

import re
from typing import Protocol

from bs4 import BeautifulSoup, Tag

from app.scraper.models import ScrapedListing


class AloParserContext(Protocol):
    @staticmethod
    def _attr_str(tag: Tag | None, name: str, default: str = "") -> str: ...

    def _normalize_link(self, base_url: str, href: str | None) -> str: ...

    def _clean_text(self, value: str) -> str: ...

    def _title_from_url(self, link: str) -> str: ...

    def _is_alo_listing_candidate(self, title: str, combined_text: str) -> bool: ...

    def _extract_ad_id(self, link: str) -> str: ...

    def _extract_size(self, text: str) -> str: ...

    def _extract_image(self, card: Tag | None, base_url: str) -> str: ...

    def _passes_filters(self, listing: ScrapedListing) -> bool: ...

    def _detect_ad_type(self, seller_name: str) -> str: ...

    def _extract_phone_from_text(self, text: str) -> str: ...


class AloSourceParser:
    def __init__(self, context: AloParserContext, source_site: str) -> None:
        self._context = context
        self._source_site = source_site

    def parse_listing_page(self, html: str, base_url: str) -> list[ScrapedListing]:
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("div.ad_block_normal")
        results: list[ScrapedListing] = []
        seen_ids: set[str] = set()

        for card in cards:
            listing = self._parse_card(card, base_url)
            if listing is None or listing.ad_id in seen_ids:
                continue
            seen_ids.add(listing.ad_id)
            if self._context._passes_filters(listing):
                results.append(listing)

        return results

    def _parse_card(self, card: Tag, base_url: str) -> ScrapedListing | None:
        context = self._context
        link_el = card.select_one("a.avn_seo[href], a.avn_image[href], a[href]")
        if link_el is None:
            return None

        link = context._normalize_link(base_url, context._attr_str(link_el, "href"))
        if not link:
            return None

        title_el = card.select_one("a.avn_seo[href]")
        title = context._clean_text(
            title_el.get_text(" ", strip=True)
            if title_el is not None
            else link_el.get_text(" ", strip=True)
        )
        if not title:
            title = context._title_from_url(link)

        image = card.select_one("a.avn_image img[alt], img[alt]")
        image_alt = context._clean_text(context._attr_str(image, "alt"))
        card_text = context._clean_text(card.get_text(" ", strip=True))
        combined_text = context._clean_text(f"{title} {image_alt} {card_text}")

        if not context._is_alo_listing_candidate(title=title, combined_text=combined_text):
            return None

        price_el = card.select_one(".avn_price")
        price = context._clean_text(
            price_el.get_text(" ", strip=True) if price_el is not None else ""
        )

        location_el = card.select_one(".avn_location")
        location = context._clean_text(
            location_el.get_text(" ", strip=True) if location_el is not None else ""
        )

        ad_id_match = re.search(
            r"adrows_(\d{4,12})",
            context._attr_str(card, "id"),
        )
        ad_id = ad_id_match.group(1) if ad_id_match else context._extract_ad_id(link)

        return ScrapedListing(
            ad_id=ad_id,
            title=title,
            price=price,
            location=location,
            size=context._extract_size(combined_text),
            link=link,
            image_url=context._extract_image(card, base_url),
            source_site=self._source_site,
        )

    def enrich_detail(self, soup: BeautifulSoup, listing: ScrapedListing) -> None:
        context = self._context

        title_el = soup.select_one("h1.large-headline, h1")
        if title_el is not None:
            title = context._clean_text(title_el.get_text(" ", strip=True))
            if title:
                listing.title = title

        price_text = self._extract_detail_price(soup)
        if price_text:
            listing.price = price_text

        params = self._extract_params(soup)
        if params.get("Местоположение"):
            listing.location = params["Местоположение"]
        if params.get("Квадратура"):
            listing.size = params["Квадратура"].replace("\xa0", " ")

        seller_name = self._extract_seller_name(soup)
        if seller_name:
            listing.seller_name = seller_name

        contact_name = self._extract_contact_name(soup)
        if contact_name:
            listing.contact_name = contact_name

        visible_phone = self._extract_phone(soup)
        if visible_phone:
            listing.phone = visible_phone

        if soup.select_one(".contacts_wrapper_flex.has_agents"):
            listing.ad_type = "agency"
        elif listing.seller_name:
            listing.ad_type = context._detect_ad_type(listing.seller_name)
        else:
            listing.ad_type = "private"

    def _extract_detail_price(self, soup: BeautifulSoup) -> str:
        for cell in soup.select(".ads-params-price"):
            text = self._context._clean_text(cell.get_text(" ", strip=True))
            if "€" in text or "лв" in text.lower():
                return text.split("Цената е около", 1)[0].strip()
        return ""

    def _extract_params(self, soup: BeautifulSoup) -> dict[str, str]:
        params: dict[str, str] = {}
        for row in soup.select(".ads-params-row"):
            title_el = row.select_one(".ads-param-title")
            value_candidates = row.select(".ads-params-cell")
            if title_el is None or len(value_candidates) < 2:
                continue
            title = self._context._clean_text(title_el.get_text(" ", strip=True))
            value = self._context._clean_text(value_candidates[-1].get_text(" ", strip=True))
            if title and value:
                params[title] = value
        return params

    def _extract_seller_name(self, soup: BeautifulSoup) -> str:
        header = soup.select_one(".contacts.header")
        if header is None:
            return ""

        for candidate in header.stripped_strings:
            value = self._context._clean_text(candidate)
            if not value:
                continue
            if value in {
                "Контакт с подателя",
                "Контакт с подателя на обявата",
                "Изпрати съобщение",
            }:
                continue
            if value.endswith(".alo.bg"):
                continue
            if "Вход" in value and "Регистрация" in value:
                continue
            if "*" in value:
                continue
            return value
        return ""

    def _extract_contact_name(self, soup: BeautifulSoup) -> str:
        el = soup.select_one(".agent_div .contact_value, .contact_value")
        return self._context._clean_text(el.get_text(" ", strip=True) if el is not None else "")

    def _extract_phone(self, soup: BeautifulSoup) -> str:
        for span in soup.select(".contact_phone .ocd_span, .ocd_span"):
            masked = self._context._clean_text(span.get_text(" ", strip=True))
            if "X" in masked.upper():
                continue
            phone = self._context._extract_phone_from_text(masked)
            if phone:
                return phone
        return ""
