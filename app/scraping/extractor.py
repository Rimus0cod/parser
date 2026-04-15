from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urljoin, urlparse

from app.scraping.models import ScrapedListing

if TYPE_CHECKING:
    from app.core.config import SiteConfig
    from app.scraping.site_profiles import SiteProfile

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
EMPTY_RESULTS_MARKERS: tuple[str, ...] = (
    "няма резултати",
    "няма обяви",
    "no listings found",
    "no results",
    "not found",
    "обяви не бяха намерени",
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
class SelectorContext:
    site_name: str
    page_type: str
    selector_version: str

    def identifier(self, field_name: str) -> str:
        return f"{self.site_name}:{self.selector_version}:{self.page_type}:{field_name}"


class ListingExtractor:
    def __init__(self, site_config: SiteConfig, site_profile: SiteProfile | None = None) -> None:
        self.site_config = site_config
        self.site_profile = site_profile
        self.selector_version = (
            site_profile.selector_version if site_profile is not None else site_config.selector_version
        )

    def detect_list_page_issue(self, page: Any) -> str | None:
        if self.site_profile is None:
            return None

        raw_text = self._node_text(page).lower()
        for marker in self.site_profile.blocked_markers:
            if marker and marker in raw_text:
                return f"blocked_marker:{marker}"

        if self.site_profile.list_wait_selector and not self._selector_exists(
            page, self.site_profile.list_wait_selector
        ):
            if any(marker in raw_text for marker in EMPTY_RESULTS_MARKERS):
                return None
            if any(marker in raw_text for marker in self.site_profile.requires_js_on):
                js_marker = next(
                    marker for marker in self.site_profile.requires_js_on if marker in raw_text
                )
                return f"js_required:{js_marker}"
            return f"missing_wait_selector:{self.site_profile.list_wait_selector}"

        link_selector = self.site_config.selectors.get("link", "a[href]")
        if link_selector and not self._selector_exists(page, link_selector):
            return f"missing_listing_selector:{link_selector}"

        return None

    def detect_detail_page_issue(self, page: Any) -> str | None:
        if self.site_profile is None:
            return None

        raw_text = self._node_text(page).lower()
        for marker in self.site_profile.blocked_markers:
            if marker and marker in raw_text:
                return f"blocked_marker:{marker}"

        if self.site_profile.detail_wait_selector and not self._selector_exists(
            page, self.site_profile.detail_wait_selector
        ):
            if self.site_profile.detail_requires_browser:
                return f"detail_requires_browser:{self.site_profile.detail_wait_selector}"
            return f"missing_detail_selector:{self.site_profile.detail_wait_selector}"
        return None

    def extract_listings(self, page: Any, *, base_url: str) -> list[ScrapedListing]:
        context = SelectorContext(
            site_name=self.site_config.name,
            page_type="list",
            selector_version=self.selector_version,
        )
        cards = self._collect_cards(page, context)
        listings: list[ScrapedListing] = []
        seen_links: set[str] = set()

        for index, card in enumerate(cards):
            listing = self._parse_card(card=card, base_url=base_url, position=index)
            if listing is None or listing.link in seen_links:
                continue
            seen_links.add(listing.link)
            listings.append(listing)
        return listings

    def enrich_listing(self, page: Any, listing: ScrapedListing) -> ScrapedListing:
        raw_text = self._node_text(page)
        phone = self._find_by_regex_text(page, PHONE_RE)
        if phone:
            listing.phone = self._clean_phone(phone)

        email = self._find_by_regex_text(page, EMAIL_RE)
        if email:
            listing.contact_email = email

        if not listing.location:
            listing.location = self._extract_location(raw_text)
        if not listing.size:
            listing.size = self._extract_size(raw_text)
        if not listing.contact_name or listing.contact_name == "-":
            listing.contact_name = self.seller_or_contact_name(raw_text, listing.seller_name)

        return listing

    def seller_or_contact_name(self, raw_text: str, seller_name: str) -> str:
        seller_name = self._clean_text(seller_name)
        return seller_name or self._guess_contact_name(raw_text)

    def _collect_cards(self, page: Any, context: SelectorContext) -> list[Any]:
        card_selector = self.site_config.selectors.get("card", "article, li, section, div")
        first_card = self._css_first(page, card_selector, identifier=context.identifier("card_first"))
        cards: list[Any] = []
        if first_card is not None:
            cards.append(first_card)
            cards.extend(self._find_similar(first_card))

        try:
            cards.extend(list(page.css(card_selector)))
        except Exception:
            pass

        unique_cards: list[Any] = []
        seen_signatures: set[str] = set()
        for card in cards:
            signature = self._card_signature(card)
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            unique_cards.append(card)
        return unique_cards

    def _parse_card(self, *, card: Any, base_url: str, position: int) -> ScrapedListing | None:
        context = SelectorContext(
            site_name=self.site_config.name,
            page_type=f"card_{position}",
            selector_version=self.selector_version,
        )
        link_selector = self.site_config.selectors.get("link", "a[href]")
        title_selector = self.site_config.selectors.get("title", "h2, h3, a[href]")
        seller_selector = self.site_config.selectors.get(
            "seller", ".seller, [class*='agency'], [class*='owner'], [class*='broker']"
        )

        link_el = self._css_first(card, link_selector, identifier=context.identifier("link"))
        title_el = self._css_first(card, title_selector, identifier=context.identifier("title"))
        seller_el = self._css_first(card, seller_selector, identifier=context.identifier("seller"))

        link = self._normalize_link(base_url, self._node_attr(link_el, "href") if link_el else "")
        if not link or not self._link_looks_like_listing(link):
            return None

        title = self._clean_text(self._node_text(title_el) if title_el is not None else "")
        if not title:
            title = self._clean_text(self._node_text(link_el) if link_el is not None else "")
        if not title:
            title = self._title_from_url(link)

        card_text = self._node_text(card)
        if not self._looks_like_property_block(title=title, card_text=card_text):
            return None

        seller_name = self._clean_text(self._node_text(seller_el) if seller_el is not None else "")
        return ScrapedListing(
            ad_id=self._extract_ad_id(link),
            title=title,
            price=self._extract_price(card_text),
            location=self._extract_location(card_text),
            size=self._extract_size(card_text),
            link=link,
            image_url=self._extract_image(card, base_url),
            source_site=self.site_config.name,
            seller_name=seller_name,
            ad_type=self._detect_ad_type(seller_name),
        )

    def _css_first(self, node: Any, selector: str, *, identifier: str) -> Any | None:
        if not selector:
            return None
        try:
            result = node.css_first(selector, identifier=identifier, auto_save=True, auto_match=True)
            if result is not None:
                return result
        except TypeError:
            try:
                result = node.css_first(selector)
                if result is not None and hasattr(node, "save"):
                    node.save(result, identifier=identifier)
                return result
            except Exception:
                return None
        except Exception:
            return None
        return None

    def _selector_exists(self, node: Any, selector: str) -> bool:
        if not selector:
            return True
        try:
            if self._css_first(node, selector, identifier=f"{self.site_config.name}:probe:{selector}") is not None:
                return True
        except Exception:
            return False
        return False

    def _find_similar(self, node: Any) -> list[Any]:
        try:
            similar = node.find_similar()
        except Exception:
            return []
        if similar is None:
            return []
        if isinstance(similar, list):
            return similar
        try:
            return list(similar)
        except TypeError:
            return [similar]

    def _node_text(self, node: Any) -> str:
        if node is None:
            return ""
        for attribute in ("text", "text_content", "get_all_text"):
            value = getattr(node, attribute, None)
            if callable(value):
                try:
                    return self._clean_text(value())
                except Exception:
                    continue
            if isinstance(value, str):
                return self._clean_text(value)
        try:
            return self._clean_text(str(node))
        except Exception:
            return ""

    def _node_attr(self, node: Any, attr_name: str) -> str:
        if node is None:
            return ""
        attrib = getattr(node, "attrib", None)
        if isinstance(attrib, dict):
            value = attrib.get(attr_name)
            return value.strip() if isinstance(value, str) else ""
        try:
            value = node.get(attr_name)
            return value.strip() if isinstance(value, str) else ""
        except Exception:
            return ""

    def _card_signature(self, card: Any) -> str:
        text = self._node_text(card)
        return hashlib.sha256(text[:500].encode("utf-8")).hexdigest()

    def _find_by_regex_text(self, page: Any, pattern: re.Pattern[str]) -> str:
        try:
            match = page.find_by_regex(pattern.pattern, first_match=True)
            return self._node_text(match)
        except Exception:
            raw = self._node_text(page)
            found = pattern.search(raw)
            return found.group(0) if found else ""

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

    def _clean_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", value or "").strip()

    def _clean_phone(self, value: str) -> str:
        cleaned = re.sub(r"[^\d+]", "", value or "")
        return cleaned[:20]

    def _link_looks_like_listing(self, link: str) -> bool:
        parsed = urlparse(link)
        if self.site_config.allowed_domains and parsed.netloc not in self.site_config.allowed_domains:
            return False
        if self.site_config.listing_path_keywords:
            return any(keyword in parsed.path for keyword in self.site_config.listing_path_keywords)
        query = parse_qs(parsed.query)
        return any(query.values())

    def _looks_like_property_block(self, title: str, card_text: str) -> bool:
        normalized = f"{title} {card_text}".lower()
        has_keyword = any(word in normalized for word in APARTMENT_KEYWORDS)
        has_price = PRICE_RE.search(card_text) is not None
        has_size = SIZE_RE.search(card_text) is not None
        has_rooms = any(token in normalized for token in ("кімнат", "комнат", "стаен", "room"))
        has_negative = any(word in normalized for word in NEGATIVE_TITLE_KEYWORDS)
        return (has_keyword or has_rooms) and (has_price or has_size) and not has_negative

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
        return hashlib.sha256(link.encode("utf-8")).hexdigest()[:24]

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
        return ""

    def _extract_image(self, card: Any, base_url: str) -> str:
        image = self._css_first(card, "img[src], img[data-src]", identifier="image")
        return self._normalize_link(
            base_url,
            self._node_attr(image, "src") or self._node_attr(image, "data-src"),
        )

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

    def _title_from_url(self, link: str) -> str:
        path = urlparse(link).path.rstrip("/").split("/")[-1]
        path = re.sub(r"[-_]+", " ", path)
        path = re.sub(r"\.\w+$", "", path)
        return path.strip().title()[:200]
