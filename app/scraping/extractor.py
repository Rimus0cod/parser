from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
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
PRICE_AMOUNT_PATTERN = (
    r"(?:\d{1,3}(?:[\s.]\d{3})+(?:,\d{1,2})?|\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?|\d+(?:[.,]\d{1,2})?)"
)
PRICE_SUFFIX_RE = re.compile(
    rf"(?P<amount>{PRICE_AMOUNT_PATTERN})\s*(?P<currency>EUR|BGN|USD|лв\.?|€|\$)",
    flags=re.I,
)
PRICE_PREFIX_RE = re.compile(
    rf"(?P<currency>EUR|BGN|USD|лв\.?|€|\$)\s*(?P<amount>{PRICE_AMOUNT_PATTERN})",
    flags=re.I,
)
PRICE_RE = PRICE_SUFFIX_RE
SIZE_RE = re.compile(
    r"(?P<size>\d+(?:[.,]\d+)?)\s*(?:м²|м2|m²|m2|sqm|sq\.?\s*m|кв\.?\s*м)",
    flags=re.I,
)
BULGARIAN_MOBILE_PREFIXES: tuple[str, ...] = ("086", "087", "088", "089", "098", "099")
BULGARIAN_LANDLINE_PREFIXES: tuple[str, ...] = ("02", "03", "04", "05", "06", "07")
PHONE_RE = re.compile(
    r"(?:(?:\+|00)?359[\s().-]*(?:0[\s().-]*)?\d(?:[\s().-]*\d){7,8}|"
    r"0\d(?:[\s().-]*\d){7,8})"
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", flags=re.I)
CONTACT_SECTION_SELECTORS: tuple[str, ...] = (
    "[data-testid*='contact']",
    "[data-testid*='phone']",
    "[data-testid*='seller']",
    "[class*='contact']",
    "[class*='phone']",
    "[class*='seller']",
    "[class*='owner']",
    "[class*='agent']",
    "[class*='broker']",
    "[itemprop='seller']",
    "[itemprop='author']",
)
CONTACT_KEYWORDS: tuple[str, ...] = (
    "contact",
    "contacts",
    "phone",
    "call",
    "email",
    "seller",
    "owner",
    "agent",
    "broker",
    "контакт",
    "контакти",
    "контакты",
    "телефон",
    "телефоны",
    "власник",
    "власниця",
    "имя",
    "iм'я",
    "ім'я",
    "rieltor",
    "рієлтор",
    "риелтор",
    "пошта",
    "почта",
)
CONTACT_LABEL_RE = re.compile(
    r"^(?:contact|contacts|contact person|phone|email|seller|owner|agent|broker|"
    r"контакт(?:на|ное)?(?:\s+особа|\s+лицо)?|контакти|контакты|"
    r"телефон(?:ы)?|власник|власниця|iм'я|ім'я|имя|рієлтор|риелтор)\s*[:\-]?\s*",
    flags=re.I,
)
AGENCY_WORDS: tuple[str, ...] = (
    "agency",
    "agency.",
    "broker",
    "agent",
    "realtor",
    "realty",
    "estate",
    "agencyalpha",
    "аген",
    "агент",
    "брокер",
    "рієлт",
    "риелт",
)
INVALID_NAME_MARKERS: tuple[str, ...] = (
    "http",
    "www.",
    "icon",
    "font",
    "fallback",
    "roboto",
    "arial",
    "mailto:",
    "tel:",
)


@dataclass(slots=True)
class SelectorContext:
    site_name: str
    page_type: str
    selector_version: str

    def identifier(self, field_name: str) -> str:
        return f"{self.site_name}:{self.selector_version}:{self.page_type}:{field_name}"


@dataclass(slots=True, frozen=True)
class PriceParts:
    raw: str
    amount: Decimal | None
    currency: str


class ListingExtractor:
    def __init__(self, site_config: SiteConfig, site_profile: SiteProfile | None = None) -> None:
        self.site_config = site_config
        self.site_profile = site_profile
        self.selector_version = (
            site_profile.selector_version
            if site_profile is not None
            else site_config.selector_version
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
        contact_sections = self._detail_contact_sections(page)

        phone = self._extract_phone_from_sections(
            contact_sections
        ) or self._extract_phone_from_text(raw_text)
        if phone:
            listing.phone = phone

        email = self._extract_email_from_sections(
            contact_sections
        ) or self._extract_email_from_text(raw_text)
        if email:
            listing.contact_email = email

        if not listing.location:
            listing.location = self._extract_location(raw_text)
            listing.location_raw = listing.location
        if not listing.size:
            listing.size = self._extract_size(raw_text)
            listing.size_raw = listing.size
            listing.area_m2 = self._extract_area_m2(raw_text)
        if not listing.contact_name or listing.contact_name == "-":
            listing.contact_name = self.seller_or_contact_name(
                raw_text,
                listing.seller_name,
                contact_sections=contact_sections,
            )

        return listing

    def seller_or_contact_name(
        self,
        raw_text: str,
        seller_name: str,
        *,
        contact_sections: list[Any] | None = None,
    ) -> str:
        seller_candidate = self._clean_contact_name_candidate(seller_name)
        if seller_candidate:
            return seller_candidate

        for section in contact_sections or []:
            section_name = self._extract_name_from_text(self._node_text(section))
            if section_name:
                return section_name

        return self._guess_contact_name(raw_text)

    def _collect_cards(self, page: Any, context: SelectorContext) -> list[Any]:
        card_selector = self.site_config.selectors.get("card", "article, li, section, div")
        first_card = self._css_first(
            page, card_selector, identifier=context.identifier("card_first")
        )
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
        price_text = self._field_text(card, "price") or card_text
        location_text = self._field_text(card, "location") or card_text
        size_text = self._field_text(card, "size") or card_text
        price_parts = self._extract_price_parts(price_text)
        location = self._extract_location(location_text)
        size = self._extract_size(size_text)
        return ScrapedListing(
            ad_id=self._extract_ad_id(link),
            title=title,
            price=price_parts.raw,
            location=location,
            size=size,
            link=link,
            image_url=self._extract_image(card, base_url),
            source_site=self.site_config.name,
            seller_name=seller_name,
            ad_type=self._detect_ad_type(seller_name),
            price_raw=price_parts.raw,
            price_amount=price_parts.amount,
            currency=price_parts.currency,
            location_raw=location,
            size_raw=size,
            area_m2=self._extract_area_m2(size_text),
        )

    def _css_first(self, node: Any, selector: str, *, identifier: str) -> Any | None:
        if not selector:
            return None
        try:
            result = node.css_first(
                selector, identifier=identifier, auto_save=True, auto_match=True
            )
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
            if (
                self._css_first(
                    node, selector, identifier=f"{self.site_config.name}:probe:{selector}"
                )
                is not None
            ):
                return True
        except Exception:
            return False
        return False

    def _css_all(self, node: Any, selector: str) -> list[Any]:
        if not selector:
            return []
        try:
            return list(node.css(selector))
        except Exception:
            return []

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
        link_hints = " ".join(
            self._node_attr(link, "href") for link in self._css_all(card, "a[href]")[:3]
        )
        return hashlib.sha256(f"{link_hints}::{text[:500]}".encode("utf-8")).hexdigest()

    def _field_text(self, card: Any, field_name: str) -> str:
        selector = self.site_config.selectors.get(field_name, "")
        if not selector:
            return ""
        node = self._css_first(
            card, selector, identifier=f"{self.site_config.name}:field:{field_name}"
        )
        return self._node_text(node) if node is not None else ""

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
        if cleaned.count("+") > 1:
            cleaned = f"+{cleaned.replace('+', '')}"
        if "+" in cleaned and not cleaned.startswith("+"):
            cleaned = f"+{cleaned.replace('+', '')}"
        if cleaned.startswith("00"):
            cleaned = f"+{cleaned[2:]}"
        if cleaned.startswith("359"):
            cleaned = f"+{cleaned}"
        if cleaned.startswith("+3590"):
            cleaned = f"+359{cleaned[5:]}"
        return cleaned[:20]

    def _link_looks_like_listing(self, link: str) -> bool:
        parsed = urlparse(link)
        if (
            self.site_config.allowed_domains
            and parsed.netloc not in self.site_config.allowed_domains
        ):
            return False
        if self.site_config.listing_path_keywords:
            return any(keyword in parsed.path for keyword in self.site_config.listing_path_keywords)
        query = parse_qs(parsed.query)
        return any(query.values())

    def _looks_like_property_block(self, title: str, card_text: str) -> bool:
        normalized = f"{title} {card_text}".lower()
        has_keyword = any(word in normalized for word in APARTMENT_KEYWORDS)
        has_price = self._search_price(card_text) is not None
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
        return self._extract_price_parts(text).raw

    def _extract_price_parts(self, text: str) -> PriceParts:
        match = self._search_price(text)
        if not match:
            return PriceParts(raw="", amount=None, currency="")
        amount = re.sub(r"\s+", " ", match.group("amount")).strip()
        currency = self._normalize_currency(match.group("currency"))
        return PriceParts(
            raw=f"{amount} {currency}",
            amount=self._parse_decimal_amount(amount),
            currency=currency,
        )

    def _search_price(self, text: str) -> re.Match[str] | None:
        return PRICE_SUFFIX_RE.search(text or "") or PRICE_PREFIX_RE.search(text or "")

    def _normalize_currency(self, value: str) -> str:
        normalized = value.strip().lower().rstrip(".")
        if normalized in {"€", "eur"}:
            return "EUR"
        if normalized in {"$", "usd"}:
            return "USD"
        if normalized in {"лв", "bgn"}:
            return "BGN"
        return value.strip().upper()

    def _parse_decimal_amount(self, value: str) -> Decimal | None:
        compact = re.sub(r"\s+", "", value or "")
        if not compact:
            return None

        comma_count = compact.count(",")
        dot_count = compact.count(".")
        if comma_count and dot_count:
            decimal_separator = "," if compact.rfind(",") > compact.rfind(".") else "."
            thousands_separator = "." if decimal_separator == "," else ","
            compact = compact.replace(thousands_separator, "").replace(decimal_separator, ".")
        elif comma_count == 1 and len(compact.rsplit(",", 1)[-1]) in {1, 2}:
            compact = compact.replace(",", ".")
        else:
            compact = compact.replace(",", "").replace(".", "")

        try:
            return Decimal(compact)
        except InvalidOperation:
            return None

    def _extract_size(self, text: str) -> str:
        match = SIZE_RE.search(text)
        return f"{match.group('size')} м²" if match else ""

    def _extract_area_m2(self, text: str) -> Decimal | None:
        match = SIZE_RE.search(text)
        if not match:
            return None
        try:
            return Decimal(match.group("size").replace(",", "."))
        except InvalidOperation:
            return None

    def _extract_location(self, text: str) -> str:
        compact = self._clean_text(text)
        location_match = re.search(
            r"([A-ZА-ЯІЇЄҐ][A-Za-zÀ-ÿА-Яа-яІіЇїЄєҐґ' -]{1,40},\s*[A-ZА-ЯІЇЄҐ][A-Za-zÀ-ÿА-Яа-яІіЇїЄєҐґ' -]{1,40})",
            compact,
        )
        if location_match:
            candidate = self._clean_text(location_match.group(1))
            candidate = re.sub(r"^(?:EUR|BGN|USD|лв\.?)\s+", "", candidate, flags=re.I)
            if not self._search_price(candidate) and not SIZE_RE.search(candidate):
                return candidate[:180]

        chunks = [self._clean_text(chunk) for chunk in re.split(r"[\n|]+", text)]
        for chunk in chunks:
            if len(chunk) < 3:
                continue
            if any(token in chunk for token in (" · ", ", ")):
                if self._search_price(chunk) or SIZE_RE.search(chunk):
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
        chunks = [chunk.strip() for chunk in re.split(r"[\n|;]+", text) if chunk.strip()]
        prioritized = [
            chunk
            for chunk in chunks
            if any(keyword in chunk.lower() for keyword in CONTACT_KEYWORDS)
        ]
        for chunk in prioritized[:15]:
            candidate = self._extract_name_from_text(chunk)
            if candidate:
                return candidate
        return "-"

    def _detail_contact_sections(self, page: Any) -> list[Any]:
        selectors = tuple(
            dict.fromkeys(
                [
                    *(
                        self.site_profile.detail_contact_selectors
                        if self.site_profile is not None
                        else ()
                    ),
                    *CONTACT_SECTION_SELECTORS,
                ]
            )
        )
        sections: list[Any] = []
        seen_signatures: set[str] = set()
        for selector in selectors:
            for node in self._css_all(page, selector):
                signature = self._section_signature(node)
                if signature in seen_signatures:
                    continue
                seen_signatures.add(signature)
                if self._node_looks_like_contact_block(node):
                    sections.append(node)
        return sections or [page]

    def _section_signature(self, node: Any) -> str:
        attrs = " ".join(
            filter(
                None,
                (
                    self._node_attr(node, "class"),
                    self._node_attr(node, "data-testid"),
                    self._node_attr(node, "href"),
                ),
            )
        )
        return hashlib.sha256(f"{attrs}::{self._node_text(node)[:300]}".encode("utf-8")).hexdigest()

    def _node_looks_like_contact_block(self, node: Any) -> bool:
        text = self._node_text(node)
        if self._extract_phone_from_text(text) or EMAIL_RE.search(text):
            return True

        lowered = text.lower()
        if any(keyword in lowered for keyword in CONTACT_KEYWORDS):
            return True

        for link in self._css_all(node, "a[href]"):
            href = self._node_attr(link, "href").lower()
            if href.startswith("tel:") or href.startswith("mailto:"):
                return True
        return False

    def _extract_phone_from_sections(self, sections: list[Any]) -> str:
        for section in sections:
            for link in self._css_all(section, "a[href]"):
                phone = self._extract_phone_from_href(self._node_attr(link, "href"))
                if phone:
                    return phone
                phone = self._extract_phone_from_text(self._node_text(link))
                if phone:
                    return phone

        for section in sections:
            phone = self._extract_phone_from_text(self._node_text(section))
            if phone:
                return phone
        return ""

    def _extract_phone_from_href(self, href: str) -> str:
        if not href.lower().startswith("tel:"):
            return ""
        return self._extract_phone_from_text(href.removeprefix("tel:"))

    def _extract_phone_from_text(self, text: str) -> str:
        for match in PHONE_RE.finditer(text or ""):
            phone = self._clean_phone(match.group(0))
            if self._is_valid_phone_candidate(phone):
                return phone
        return ""

    def _is_valid_phone_candidate(self, phone: str) -> bool:
        digits = re.sub(r"\D", "", phone)
        if not digits:
            return False

        national_number = self._to_bulgarian_national_number(digits)
        if not national_number:
            return False
        if len(set(national_number)) == 1:
            return False
        return self._is_valid_bulgarian_mobile(
            national_number
        ) or self._is_valid_bulgarian_landline(national_number)

    def _to_bulgarian_national_number(self, digits: str) -> str:
        if digits.startswith("00359"):
            digits = digits[5:]
        elif digits.startswith("359"):
            digits = digits[3:]
        elif digits.startswith("0"):
            return digits if 9 <= len(digits) <= 10 else ""
        else:
            return ""

        if digits.startswith("0"):
            national_number = digits
        else:
            national_number = f"0{digits}"
        return national_number if 9 <= len(national_number) <= 10 else ""

    def _is_valid_bulgarian_mobile(self, national_number: str) -> bool:
        return len(national_number) == 10 and national_number.startswith(
            BULGARIAN_MOBILE_PREFIXES
        )

    def _is_valid_bulgarian_landline(self, national_number: str) -> bool:
        return len(national_number) in {9, 10} and national_number.startswith(
            BULGARIAN_LANDLINE_PREFIXES
        )

    def _extract_email_from_sections(self, sections: list[Any]) -> str:
        for section in sections:
            for link in self._css_all(section, "a[href]"):
                email = self._extract_email_from_href(self._node_attr(link, "href"))
                if email:
                    return email
                email = self._extract_email_from_text(self._node_text(link))
                if email:
                    return email

        for section in sections:
            email = self._extract_email_from_text(self._node_text(section))
            if email:
                return email
        return ""

    def _extract_email_from_href(self, href: str) -> str:
        if not href.lower().startswith("mailto:"):
            return ""
        return self._extract_email_from_text(href.removeprefix("mailto:"))

    def _extract_email_from_text(self, text: str) -> str:
        match = EMAIL_RE.search(text or "")
        if match is None:
            return ""
        return match.group(0).strip().lower()[:255]

    def _extract_name_from_text(self, text: str) -> str:
        compact = self._clean_text(text)
        if not compact:
            return ""

        label_match = re.search(
            r"(?:contact person|contact|owner|seller|agent|broker|"
            r"контакт(?:на|ное)?(?:\s+особа|\s+лицо)?|власник|власниця|"
            r"iм'я|ім'я|имя|рієлтор|риелтор)\s*[:\-]?\s*(?P<name>[^|,;]{2,80})",
            compact,
            flags=re.I,
        )
        if label_match:
            candidate = self._clean_contact_name_candidate(label_match.group("name"))
            if candidate:
                return candidate

        for chunk in re.split(r"[\n|;,]+", compact):
            candidate = self._clean_contact_name_candidate(chunk)
            if candidate:
                return candidate
        return ""

    def _clean_contact_name_candidate(self, value: str) -> str:
        candidate = self._clean_text(value)
        if not candidate or candidate == "-":
            return ""

        candidate = re.sub(r"https?://\S+|www\.\S+", " ", candidate, flags=re.I)
        candidate = EMAIL_RE.sub(" ", candidate)
        candidate = PHONE_RE.sub(" ", candidate)
        candidate = CONTACT_LABEL_RE.sub("", candidate)
        candidate = re.sub(r"[|;:/]+", " ", candidate)
        candidate = self._clean_text(candidate)
        lowered = candidate.lower()

        if not candidate or any(marker in lowered for marker in INVALID_NAME_MARKERS):
            return ""
        if any(token in lowered for token in AGENCY_WORDS):
            return ""
        if any(char.isdigit() for char in candidate):
            return ""
        if "@" in candidate:
            return ""

        words = re.findall(r"[A-Za-zÀ-ÿА-Яа-яІіЇїЄєҐґ'’-]+", candidate)
        if not 1 <= len(words) <= 4:
            return ""
        if sum(len(word) for word in words) < 4:
            return ""

        cleaned = " ".join(words)
        if any(keyword == cleaned.lower() for keyword in CONTACT_KEYWORDS):
            return ""
        return cleaned[:120]

    def _title_from_url(self, link: str) -> str:
        path = urlparse(link).path.rstrip("/").split("/")[-1]
        path = re.sub(r"[-_]+", " ", path)
        path = re.sub(r"\.\w+$", "", path)
        return path.strip().title()[:200]
