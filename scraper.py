from __future__ import annotations

import argparse
import logging
import random
import re
import sys
import time
import concurrent.futures
import threading
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from config import Config, load_config
from email_sender import send_email
from mysql_store import MySQLStore
from sheets import SheetsClient, normalise_phone

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

console = Console()


def _setup_logging(config: Config) -> None:
    """Configure the root logger with Rich (coloured) output + optional file."""
    handlers: list[logging.Handler] = [
        RichHandler(
            console=console,
            rich_tracebacks=True,
            markup=True,
            show_path=False,
        )
    ]

    if config.log_file:
        file_handler = logging.FileHandler(config.log_file, encoding="utf-8")
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s")
        )
        handlers.append(file_handler)

    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        handlers=handlers,
        format="%(message)s",
        datefmt="[%X]",
    )


logger = logging.getLogger("imoti_scraper")

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

APARTMENT_KEYWORDS = (
    "апартамент",
    "едностаен",
    "двустаен",
    "тристаен",
    "четиристаен",
    "многостаен",
)


@dataclass
class Listing:
    ad_id: str
    title: str
    price: str
    location: str
    size: str
    link: str
    # Populated during enrichment (detail page visit or list page extraction):
    phone: str = ""
    seller_name: str = ""  # e.g. "Частно лице" or "Агенция XYZ"
    ad_type: str = ""  # "приватний" | "від агенції"
    contact_name: str = "-"
    contact_email: str = "-"

    def as_row(self, today: str) -> list[str]:

        return [
            today,
            self.ad_id,
            self.title,
            self.price,
            self.location,
            self.size,
            self.link,
            self.phone,
            self.seller_name,
            self.ad_type,
            self.contact_name or "-",
            self.contact_email or "-",
        ]

    def as_dict(self) -> dict[str, str]:
        """Return a dict keyed by Config.new_ads_headers for the email template."""
        return {
            "Date": "",
            "Ad_ID": self.ad_id,
            "Title": self.title,
            "Price": self.price,
            "Location": self.location,
            "Size": self.size,
            "Link": self.link,
            "Phone": self.phone,
            "Seller_Name": self.seller_name,
            "Type": self.ad_type,
            "Contact_Name": self.contact_name or "-",
            "Contact_Email": self.contact_email or "-",
        }


@dataclass
class AgencyRecord:
    agency_name: str
    phones: list[str]
    city: str = ""
    email: str = ""
    contact_name: str = ""
    detail_url: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "Agency_Name": self.agency_name,
            "Phones": ",".join(self.phones),
            "City": self.city,
            "Email": self.email,
            "Contact_Name": self.contact_name,
        }


@dataclass
class DetailPageInfo:
    phone: str = ""
    seller_name: str = ""
    contact_name: str = "-"
    contact_email: str = "-"
    agency_profile_url: str = ""


# ---------------------------------------------------------------------------
# HTTP session factory — with User-Agent rotation
# ---------------------------------------------------------------------------


def _make_session(config: Config) -> requests.Session:

    ua = random.choice(config.user_agents)
    logger.debug("Using User-Agent: %s", ua)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": ua,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "bg-BG,bg;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            # Referrer to mimic a real browser navigation.
            "Referer": "https://imoti.bg/",
        }
    )
    return session


def _polite_get(
    session: requests.Session,
    url: str,
    config: Config,
    retries: int = 3,
    backoff: float = 5.0,
) -> Optional[requests.Response]:

    delay = random.uniform(config.request_delay_min, config.request_delay_max)
    logger.debug("Sleeping %.1f s before fetching %s", delay, url)
    time.sleep(delay)

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            logger.warning(
                "HTTP %s for %s (attempt %d/%d)", status, url, attempt, retries
            )
        except requests.RequestException as exc:
            logger.warning(
                "Request error for %s (attempt %d/%d): %s", url, attempt, retries, exc
            )

        if attempt < retries:
            wait = backoff * attempt
            logger.debug("Waiting %.1f s before retry …", wait)
            time.sleep(wait)
            # Rotate User-Agent on retry.
            session.headers["User-Agent"] = random.choice(config.user_agents)

    logger.error("Giving up on %s after %d attempts.", url, retries)
    return None


# ---------------------------------------------------------------------------
# Listing-page parser
# ---------------------------------------------------------------------------

_AD_ID_RE = re.compile(r"-(\d{4,10})\.htm$", re.IGNORECASE)


_PHONE_TEXT_RE = re.compile(
    r"(?:тел\.?\s*:?\s*)?(\+?359[\s\-]?|0)(\d[\d\s\-]{6,12}\d)",
    re.IGNORECASE,
)
_EMAIL_RE = re.compile(r"[\w.\-+]+@[\w.\-]+\.[a-z]{2,}", re.IGNORECASE)
_AGU_HREF_RE = re.compile(r"agu:\d+", re.IGNORECASE)
CONTACT_SENTINEL = "-"


def _extract_ad_id(url: str) -> str:

    match = _AD_ID_RE.search(url)
    return match.group(1) if match else ""


def _is_apartment(title: str, url: str) -> bool:

    combined = (title + " " + url).lower()
    return any(kw in combined for kw in APARTMENT_KEYWORDS)


def _extract_phone_from_text(text: str) -> str:

    match = _PHONE_TEXT_RE.search(text)
    if match:
        raw = match.group(0)
        return normalise_phone(raw)
    return ""


def parse_listing_page(
    html: str,
    base_url: str,
    city_filter: Optional[str] = None,
) -> list[Listing]:

    soup = BeautifulSoup(html, "html.parser")
    listings: list[Listing] = []

    # Each listing card is wrapped in <article class="product-classic">
    for article in soup.select("article.product-classic"):
        try:
            listing = _parse_card(article, base_url)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Skipping malformed card: %s", exc)
            continue

        if listing is None:
            continue

        # ── Filter: apartments only ──────────────────────────────────────
        if not _is_apartment(listing.title, listing.link):
            logger.debug("Skipping non-apartment: '%s'", listing.title)
            continue

        # ── Filter: optional city filter ─────────────────────────────────
        if city_filter and city_filter.lower() not in listing.location.lower():
            logger.debug(
                "Skipping ad outside city filter ('%s'): %s",
                city_filter,
                listing.location,
            )
            continue

        listings.append(listing)

    return listings


def _parse_card(
    article: BeautifulSoup,
    base_url: str,
) -> Optional[Listing]:

    # ── Link & Ad ID ─────────────────────────────────────────────────────
    title_anchor = article.select_one("h4.product-classic-title a")
    if not title_anchor:
        return None

    raw_href: str = title_anchor.get("href", "")
    link = raw_href if raw_href.startswith("http") else urljoin(base_url, raw_href)
    ad_id = _extract_ad_id(link)
    if not ad_id:
        return None

    # ── Title ─────────────────────────────────────────────────────────────
    title = title_anchor.get_text(strip=True)

    # ── Price ─────────────────────────────────────────────────────────────
    price_el = article.select_one(".product-classic-price")
    if price_el:
        price_lines = [
            ln.strip() for ln in price_el.get_text("\n").splitlines() if ln.strip()
        ]
        price = price_lines[0] if price_lines else ""
    else:
        price = ""

    # ── Location ──────────────────────────────────────────────────────────
    location_el = article.select_one(".btext")
    location = location_el.get_text(strip=True) if location_el else ""

    # ── Size ──────────────────────────────────────────────────────────────
    size = ""
    for li in article.select(".product-classic-list li"):
        text = li.get_text(strip=True)
        if "кв.м." in text:
            size = text
            break

    # ── Phone (opportunistic from card) ───────────────────────────────────

    phone = _extract_phone_from_card(article)

    # ── Seller Name (opportunistic from card) ─────────────────────────────

    seller_name = _extract_seller_name_from_card(article)

    return Listing(
        ad_id=ad_id,
        title=title,
        price=price,
        location=location,
        size=size,
        link=link,
        phone=phone,
        seller_name=seller_name,
    )


def _extract_phone_from_card(article: BeautifulSoup) -> str:

    # 1. href="tel:..." anchor
    tel_link = article.select_one('a[href^="tel:"]')
    if tel_link:
        raw = tel_link.get("href", "").replace("tel:", "")
        phone = normalise_phone(raw)
        if phone:
            return phone

    # 2. Elements with phone-related class names
    for selector in (
        "[class*='phone']",
        "[class*='tel']",
        ".contact-phone",
        ".phone-number",
        "span.phone",
    ):
        el = article.select_one(selector)
        if el:
            phone = normalise_phone(el.get_text(strip=True))
            if len(phone) >= 9:  # sanity check: real Bulgarian numbers are 9-12 digits
                return phone

    # 3. Text scan for "Тел" prefix in the full card text
    full_text = article.get_text(" ", strip=True)
    if "тел" in full_text.lower():
        phone = _extract_phone_from_text(full_text)
        if phone:
            return phone

    return ""


def _extract_seller_name_from_card(article: BeautifulSoup) -> str:

    # Try several common CSS patterns for the seller block on listing cards.
    for selector in (
        ".product-classic-agency",
        ".agency-name",
        ".seller-name",
        "[class*='agency']",
        "[class*='seller']",
        ".block-info h3",  # may appear in card previews
    ):
        el = article.select_one(selector)
        if el:
            name = el.get_text(strip=True)
            if name:
                return name

    return ""


# ---------------------------------------------------------------------------
# Detail-page parser
# ---------------------------------------------------------------------------


def parse_detail_page(html: str) -> DetailPageInfo:

    soup = BeautifulSoup(html, "html.parser")
    info = DetailPageInfo()

    # ── Phone + seller from primary listing blocks ────────────────────────
    block_info = soup.select_one("div.block-info")
    if block_info:
        h3 = block_info.select_one("h3")
        if h3:
            info.seller_name = h3.get_text(strip=True)

        tel_link = block_info.select_one('a[href^="tel:"]')
        if tel_link:
            info.phone = normalise_phone(tel_link.get("href", "").replace("tel:", ""))

        if not info.phone:
            person_link = block_info.select_one(".block-person-link")
            if person_link:
                info.phone = normalise_phone(person_link.get_text(strip=True))

    if not info.phone or not info.seller_name:
        block_agent = soup.select_one("div.block-agent")
        if block_agent:
            if not info.seller_name:
                name_el = block_agent.select_one(
                    "h3, .block-agent-name, [class*='name']"
                )
                if name_el:
                    info.seller_name = name_el.get_text(strip=True)
            if not info.phone:
                tel_link = block_agent.select_one('a[href^="tel:"]')
                if tel_link:
                    info.phone = normalise_phone(
                        tel_link.get("href", "").replace("tel:", "")
                    )

    if not info.phone:
        any_tel = soup.select_one('a[href^="tel:"]')
        if any_tel:
            info.phone = normalise_phone(any_tel.get("href", "").replace("tel:", ""))

    if not info.seller_name:
        for el in soup.select(
            ".published-by, .posted-by, [class*='author'], [class*='contact']"
        ):
            text = el.get_text(strip=True)
            if text:
                info.seller_name = text
                break

    # ── Contact name/email from card blocks ───────────────────────────────
    info.contact_name = _extract_contact_name_from_detail_soup(soup)
    info.contact_email = _extract_contact_email_from_detail_soup(soup)
    info.agency_profile_url = _extract_agency_profile_url_from_detail_soup(soup)

    return info


def _extract_contact_name_from_detail_soup(soup: BeautifulSoup) -> str:
    blocks = soup.select("div.block-person-link")

    for block in blocks:
        # First non-mailto/non-tel anchor often contains profile owner name.
        for a_tag in block.find_all("a", href=True):
            href = a_tag.get("href", "").lower()
            if href.startswith(("mailto:", "tel:")):
                continue
            if _AGU_HREF_RE.search(href):
                continue
            candidate = a_tag.get_text(" ", strip=True)
            if _looks_like_person_name(candidate):
                return candidate

        # Fallback tags inside block.
        for tag in block.find_all(["span", "strong", "b", "p", "div"]):
            candidate = tag.get_text(" ", strip=True)
            if _looks_like_person_name(candidate):
                return candidate

        # Last resort: use block text stripped from phone/email.
        text = block.get_text(" ", strip=True)
        text = _EMAIL_RE.sub("", text)
        text = _PHONE_TEXT_RE.sub("", text)
        text = text.strip(" @,;:|–-")
        if _looks_like_person_name(text):
            return text

    # Secondary generic selectors.
    for sel in (
        ".contact-name",
        ".agent-name",
        ".block-agent-contact .name",
        ".contact-person",
        "[class*='contact-name']",
    ):
        el = soup.select_one(sel)
        if not el:
            continue
        candidate = el.get_text(" ", strip=True)
        if _looks_like_person_name(candidate):
            return candidate

    return "-"


def _extract_contact_email_from_detail_soup(soup: BeautifulSoup) -> str:
    # 1) Prefer mailto inside person/contact blocks.
    for root in soup.select("div.block-person-link, div.block-info, div.block-agent"):
        mail = root.select_one('a[href^="mailto:"]')
        if mail:
            email = mail.get("href", "").replace("mailto:", "").split("?")[0].strip()
            if email:
                return email
        txt = root.get_text(" ", strip=True)
        match = _EMAIL_RE.search(txt)
        if match:
            return match.group(0)

    # 2) Any mailto on the page.
    mail = soup.select_one('a[href^="mailto:"]')
    if mail:
        email = mail.get("href", "").replace("mailto:", "").split("?")[0].strip()
        if email:
            return email

    # 3) Regex fallback over visible text.
    for container in ("main", "#main", ".main-content", ".content", "article", "body"):
        el = soup.select_one(container)
        if not el:
            continue
        match = _EMAIL_RE.search(el.get_text(" ", strip=True))
        if match:
            return match.group(0)

    return "-"


def _extract_agency_profile_url_from_detail_soup(soup: BeautifulSoup) -> str:
    for root in soup.select("div.block-info, div.block-agent, body"):
        for a_tag in root.select("a[href]"):
            href = a_tag.get("href", "").strip()
            if not href or not _AGU_HREF_RE.search(href):
                continue
            return (
                href if href.startswith("http") else urljoin("https://imoti.bg/", href)
            )
    return ""


def _looks_like_person_name(text: str) -> bool:
    value = (text or "").strip()
    if len(value) < 2:
        return False
    lower = value.lower()
    banned_parts = ("частно лице", "агенция", "imoti", "@", "http")
    if any(p in lower for p in banned_parts):
        return False
    if _EMAIL_RE.search(value):
        return False
    digits = sum(ch.isdigit() for ch in value)
    if digits > 0 and digits / max(len(value), 1) > 0.3:
        return False
    words = [w for w in value.split() if w]
    return len(words) >= 2


# ---------------------------------------------------------------------------
# Pagination helper
# ---------------------------------------------------------------------------


def _has_next_page(soup: BeautifulSoup) -> bool:

    # First, check for common pagination patterns in text
    for a in soup.select("ul.pagination a, .pagination a, a.next, li.next a"):
        text = a.get_text(strip=True).lower()
        if "»" in a.get_text() or text in ("следваща", "next", ">"):
            return True

    # Also check for any link with page:N pattern (more reliable)
    for a in soup.select("a[href*='/page:']"):
        href = a.get("href", "")
        # If there's a link to a higher page number, we have pagination
        if "/page:" in href:
            # Check if it's not just linking to current page
            # Look for > or » symbols
            text = a.get_text(strip=True)
            if ">" in text or "»" in text or text == ">":
                return True

    return False


# ---------------------------------------------------------------------------
# Core scraping logic — listing pages
# ---------------------------------------------------------------------------


def scrape_all_pages(
    session: requests.Session,
    config: Config,
) -> list[Listing]:

    all_listings: list[Listing] = []
    seen_ids: set[str] = set()  # dedup within a single scrape run

    for page_num in range(1, config.max_pages + 1):
        url = config.base_url.format(page=page_num)
        logger.info("Scraping listing page %d → %s", page_num, url)

        resp = _polite_get(session, url, config)
        if resp is None:
            logger.warning("Failed to fetch page %d — stopping pagination.", page_num)
            break

        page_listings = parse_listing_page(
            resp.text,
            base_url="https://imoti.bg/",
            city_filter=config.city_filter,
        )

        # Deduplicate within the current run (site may repeat ads across pages).
        new_on_page: list[Listing] = []
        for lst in page_listings:
            if lst.ad_id not in seen_ids:
                seen_ids.add(lst.ad_id)
                new_on_page.append(lst)

        logger.info(
            "  Page %d: %d apartment listing(s) found (%d after dedup).",
            page_num,
            len(page_listings),
            len(new_on_page),
        )
        all_listings.extend(new_on_page)

        # If the page returned zero results, we've gone past the last page.
        if not page_listings:
            logger.info(
                "No listings on page %d — reached the end of results.", page_num
            )
            break

        # Check for a "next page" link as a secondary stop condition.
        soup = BeautifulSoup(resp.text, "html.parser")
        if not _has_next_page(soup) and page_num > 1:
            logger.info("No 'next page' link on page %d — stopping.", page_num)
            break

    logger.info("Total apartment listings collected: %d", len(all_listings))
    return all_listings


def _resolve_agency_contact_key(
    seller_name_lower: str,
    agency_contacts: dict[str, dict[str, str]],
) -> str:
    """Resolve agency key by exact/prefix/substring match (longest wins)."""
    if not seller_name_lower:
        return ""
    if seller_name_lower in agency_contacts:
        return seller_name_lower

    candidates: list[tuple[int, str]] = []
    for key in agency_contacts:
        if seller_name_lower.startswith(key) or key.startswith(seller_name_lower):
            candidates.append((len(key), key))
        elif key in seller_name_lower or seller_name_lower in key:
            candidates.append((len(key), key))
    if not candidates:
        return ""
    candidates.sort(key=lambda t: t[0], reverse=True)
    return candidates[0][1]


class ContactResolver:
    """Resolve listing contacts using sheet cache, detail page and agency profile."""

    def __init__(
        self,
        session: requests.Session,
        config: Config,
        agency_contacts: dict[str, dict[str, str]],
    ) -> None:
        self._session = session
        self._config = config
        self._agency_contacts_source = agency_contacts
        self._detail_cache: dict[str, DetailPageInfo] = {}
        self._agency_cache: dict[str, dict[str, str]] = {}
        self._agu_cache: dict[str, dict[str, str]] = {}

    def get_listing_detail(self, listing: Listing) -> DetailPageInfo:
        key = listing.ad_id or listing.link
        if key in self._detail_cache:
            return self._detail_cache[key]

        resp = _polite_get(self._session, listing.link, self._config)
        if resp is None:
            info = DetailPageInfo()
        else:
            info = parse_detail_page(resp.text)
        self._detail_cache[key] = info
        return info

    def resolve_from_agency_sheet(self, seller_name: str) -> dict[str, str]:
        seller_name_lower = (seller_name or "").strip().lower()
        if not seller_name_lower:
            return {"contact_name": CONTACT_SENTINEL, "contact_email": CONTACT_SENTINEL}

        if seller_name_lower in self._agency_cache:
            return self._agency_cache[seller_name_lower]

        key = _resolve_agency_contact_key(
            seller_name_lower, self._agency_contacts_source
        )
        if not key:
            info = {"contact_name": CONTACT_SENTINEL, "contact_email": CONTACT_SENTINEL}
            self._agency_cache[seller_name_lower] = info
            return info

        row = self._agency_contacts_source.get(key, {})
        info = {
            "contact_name": (row.get("contact_name", "").strip() or CONTACT_SENTINEL),
            "contact_email": (row.get("contact_email", "").strip() or CONTACT_SENTINEL),
        }
        self._agency_cache[seller_name_lower] = info
        return info

    def resolve_from_agency_profile(self, profile_url: str) -> dict[str, str]:
        if not profile_url:
            return {"contact_name": CONTACT_SENTINEL, "contact_email": CONTACT_SENTINEL}

        key = profile_url.strip()
        if key in self._agu_cache:
            return self._agu_cache[key]

        details = parse_agency_details(profile_url, self._session, self._config)
        info = {
            "contact_name": details.get("contact_name", "").strip() or CONTACT_SENTINEL,
            "contact_email": details.get("email", "").strip() or CONTACT_SENTINEL,
        }
        self._agu_cache[key] = info
        return info


def enrich_listing(
    listing: Listing,
    agency_phones: set[str],
    agency_names: set[str],
    contact_resolver: ContactResolver,
) -> None:

    detail_info: Optional[DetailPageInfo] = None

    def ensure_detail() -> DetailPageInfo:
        nonlocal detail_info
        if detail_info is None:
            logger.debug(
                "Fetching detail page for ad %s (phone=%r, seller=%r)",
                listing.ad_id,
                listing.phone,
                listing.seller_name,
            )
            detail_info = contact_resolver.get_listing_detail(listing)
        return detail_info

    # Fill core fields first (needed for classification).
    if not listing.phone or not listing.seller_name:
        detail = ensure_detail()
        if not listing.phone and detail.phone:
            listing.phone = detail.phone
        if not listing.seller_name and detail.seller_name:
            listing.seller_name = detail.seller_name

    if not listing.phone and not listing.seller_name:
        logger.warning(
            "Ad %s: could not find phone or seller name on list page or detail page.",
            listing.ad_id,
        )

    listing.ad_type = _classify_listing(
        phone=listing.phone,
        seller_name=listing.seller_name,
        agency_phones=agency_phones,
        agency_names=agency_names,
    )

    if listing.ad_type == "приватний":
        detail = ensure_detail()
        listing.contact_name = detail.contact_name or CONTACT_SENTINEL
        listing.contact_email = detail.contact_email or CONTACT_SENTINEL
    else:
        sheet_contact = contact_resolver.resolve_from_agency_sheet(listing.seller_name)
        name = sheet_contact["contact_name"]
        email = sheet_contact["contact_email"]

        if name == CONTACT_SENTINEL or email == CONTACT_SENTINEL:
            detail = ensure_detail()
            if name == CONTACT_SENTINEL and detail.contact_name != CONTACT_SENTINEL:
                name = detail.contact_name
            if email == CONTACT_SENTINEL and detail.contact_email != CONTACT_SENTINEL:
                email = detail.contact_email

            if (
                name == CONTACT_SENTINEL or email == CONTACT_SENTINEL
            ) and detail.agency_profile_url:
                profile_contact = contact_resolver.resolve_from_agency_profile(
                    detail.agency_profile_url
                )
                if (
                    name == CONTACT_SENTINEL
                    and profile_contact["contact_name"] != CONTACT_SENTINEL
                ):
                    name = profile_contact["contact_name"]
                if (
                    email == CONTACT_SENTINEL
                    and profile_contact["contact_email"] != CONTACT_SENTINEL
                ):
                    email = profile_contact["contact_email"]

        listing.contact_name = name or CONTACT_SENTINEL
        listing.contact_email = email or CONTACT_SENTINEL

    logger.debug(
        "  Ad %s → phone=%s, seller=%r, type=%s, contact=%r, email=%r",
        listing.ad_id,
        listing.phone,
        listing.seller_name,
        listing.ad_type,
        listing.contact_name,
        listing.contact_email,
    )


def _classify_listing(
    phone: str,
    seller_name: str,
    agency_phones: set[str],
    agency_names: set[str],
) -> str:

    # 1. Phone in agency phone set.
    if phone and phone in agency_phones:
        return "від агенції"

    # 2. Seller name contains any agency name substring.
    if seller_name:
        seller_lower = seller_name.lower()
        for agency_name in agency_names:
            if agency_name and agency_name in seller_lower:
                return "від агенції"

    return "приватний"


# ---------------------------------------------------------------------------
# Agency scraping — for --update-agencies
# ---------------------------------------------------------------------------


def scrape_agencies(
    session: requests.Session,
    config: Config,
) -> list[AgencyRecord]:

    all_agencies: list[AgencyRecord] = []
    seen_names: set[str] = set()  # dedup by lowercased name within this run

    # ── Pass 1: collect from list pages ───────────────────────────────────
    for page_num in range(1, config.max_agency_pages + 1):
        url = config.agencies_url.format(page=page_num)
        logger.info("Scraping agencies page %d → %s", page_num, url)

        resp = _polite_get(session, url, config)
        if resp is None:
            logger.warning("Failed to fetch agency page %d — stopping.", page_num)
            break

        page_agencies = _parse_agency_page(resp.text)

        new_on_page: list[AgencyRecord] = []
        for rec in page_agencies:
            key = rec.agency_name.lower()
            if key not in seen_names:
                seen_names.add(key)
                new_on_page.append(rec)

        logger.info(
            "  Agency page %d: %d record(s) (%d after dedup).",
            page_num,
            len(page_agencies),
            len(new_on_page),
        )
        all_agencies.extend(new_on_page)

        if not page_agencies:
            logger.info("No agencies on page %d — stopping.", page_num)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        if not _has_next_page(soup) and page_num > 1:
            logger.info("No 'next page' link on agency page %d — stopping.", page_num)
            break

    logger.info(
        "Agency list pages done — %d unique agencies collected. "
        "Now fetching profile pages for Contact Name / Email …",
        len(all_agencies),
    )

    # ── Pass 2: enrich each agency from its profile page ──────────────────
    # The delay is handled inside parse_agency_details() via _polite_get()
    # which already applies config.request_delay_min / max (2–5 s).
    # That is polite enough; no extra sleep is needed here.
    for idx, rec in enumerate(all_agencies, start=1):
        if not rec.detail_url:
            logger.debug(
                "  [%d/%d] %s — no profile URL, skipping detail fetch.",
                idx,
                len(all_agencies),
                rec.agency_name,
            )
            # Ensure sentinel values so the row is complete.
            if not rec.contact_name:
                rec.contact_name = "-"
            if not rec.email:
                rec.email = "-"
            continue

        logger.info(
            "  [%d/%d] Fetching profile: %s → %s",
            idx,
            len(all_agencies),
            rec.agency_name,
            rec.detail_url,
        )
        try:
            details = parse_agency_details(rec.detail_url, session, config)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "  Could not fetch profile for '%s': %s",
                rec.agency_name,
                exc,
            )
            details = {"contact_name": "-", "email": "-"}

        # Apply fetched contact_name (always — list page doesn't have it).
        rec.contact_name = details["contact_name"]

        detail_email = details["email"]
        if detail_email != "-" and (not rec.email or rec.email == "-"):
            rec.email = detail_email
        elif not rec.email:
            rec.email = "-"

        logger.debug(
            "  %s → contact=%r  email=%r",
            rec.agency_name,
            rec.contact_name,
            rec.email,
        )

    logger.info("Total agencies fully enriched: %d", len(all_agencies))
    return all_agencies


def _parse_agency_page(html: str) -> list[AgencyRecord]:

    soup = BeautifulSoup(html, "html.parser")
    records: list[AgencyRecord] = []

    # Current imoti.bg structure: div.agency_info inside div.agency_list
    containers = soup.select("div.agency_info")

    # Fallback to older structure if needed
    if not containers:
        containers = soup.select(
            ".agency-item, .agency-list-item, article.agency, .company-item"
        )

    if not containers:
        # Last fallback: look for any block that contains an agency name
        containers = [
            el.parent
            for el in soup.select("h3.agency-name, a.agency-name, h2.agency-name")
            if el.parent
        ]

    for container in containers:
        try:
            record = _parse_agency_container(container)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Skipping malformed agency container: %s", exc)
            continue
        if record is not None:
            records.append(record)

    return records


# ── City parsing helper ─────────────────────────────────────────────────────
BULGARIAN_CITIES = [
    "София",
    "Пловдив",
    "Варна",
    "Бургас",
    "Русе",
    "Стара Загора",
    "Плевен",
    "Добрич",
    "Сливен",
    "Шумен",
    "Перник",
    "Хасково",
    "Казанлък",
    "Кюстендил",
    "Монтана",
    "Велико Търново",
    "Асеновград",
    "Видин",
    "Враца",
    "Габрово",
    "Димитровград",
    "Ловеч",
    "Силистра",
]


def _parse_agency_container(container: BeautifulSoup) -> Optional[AgencyRecord]:

    # ── Agency name + detail URL ──────────────────────────────────────────
    agency_name = ""
    detail_url = ""

    # Try selectors from most specific to most generic.
    for name_sel in (
        "h3.agency-name a",
        "a.agency-name",
        "h3.agency-name",
        "h2.agency-name",
        "h3 a",
        "h2 a",
        "h3",
    ):
        el = container.select_one(name_sel)
        if el:
            agency_name = el.get_text(strip=True)
            if agency_name:
                # If the matched element is an <a>, grab its href as detail URL.
                if el.name == "a":
                    raw_href = el.get("href", "").strip()
                    if raw_href:
                        detail_url = (
                            raw_href
                            if raw_href.startswith("http")
                            else urljoin("https://imoti.bg/", raw_href)
                        )
                break

    if not agency_name:
        return None

    # ── Phone numbers ─────────────────────────────────────────────────────
    phones: list[str] = []

    # 1. Elements with phone-related class / selector
    for phone_sel in (
        "span.phone",
        ".phone",
        ".phones",
        "[class*='phone']",
        "[class*='tel']",
    ):
        for phone_el in container.select(phone_sel):
            raw = phone_el.get_text(strip=True)
            normalised = normalise_phone(raw)
            if normalised and len(normalised) >= 9:
                phones.append(normalised)

    # 2. <a href="tel:..."> links
    for tel_link in container.select('a[href^="tel:"]'):
        raw = tel_link.get("href", "").replace("tel:", "")
        normalised = normalise_phone(raw)
        if normalised and len(normalised) >= 9:
            phones.append(normalised)

    # 3. Regex scan over full container text as last resort
    if not phones:
        full_text = container.get_text(" ", strip=True)
        for match in _PHONE_TEXT_RE.finditer(full_text):
            normalised = normalise_phone(match.group(0))
            if normalised and len(normalised) >= 9:
                phones.append(normalised)

    # Deduplicate while preserving order.
    seen_p: set[str] = set()
    unique_phones: list[str] = []
    for p in phones:
        if p not in seen_p:
            seen_p.add(p)
            unique_phones.append(p)

    # ── Email (quick pass from list page) ─────────────────────────────────
    # A richer email will be sought on the profile page by parse_agency_details().
    email = ""
    mailto = container.select_one('a[href^="mailto:"]')
    if mailto:
        email = mailto.get("href", "").replace("mailto:", "").strip()
    else:
        _email_re = re.compile(r"[\w.\-+]+@[\w.\-]+\.[a-z]{2,}", re.IGNORECASE)
        full_text = container.get_text(" ", strip=True)
        em_match = _email_re.search(full_text)
        if em_match:
            email = em_match.group(0)

    return AgencyRecord(
        agency_name=agency_name,
        phones=unique_phones,
        email=email,
        detail_url=detail_url,
    )


# ---------------------------------------------------------------------------
# Agency profile-page parser  (parse_agency_details)
# ---------------------------------------------------------------------------


def parse_agency_details(
    url: str,
    session: requests.Session,
    config: Config,
) -> dict[str, str]:

    # ── Fetch with the shared polite delay ────────────────────────────────
    # Use a slightly longer minimum delay for detail pages to be extra polite.
    resp = _polite_get(
        session=session,
        url=url,
        config=config,
        retries=2,
        backoff=3.0,
    )

    if resp is None:
        logger.warning("parse_agency_details: could not fetch %s", url)
        return {"contact_name": "-", "email": "-"}

    # Ensure correct encoding — imoti.bg serves UTF-8 but requests sometimes
    # mis-detects it.  Force UTF-8 then fall back to apparent encoding.
    try:
        html = resp.content.decode("utf-8")
    except UnicodeDecodeError:
        html = resp.content.decode(resp.apparent_encoding, errors="replace")

    soup = BeautifulSoup(html, "html.parser")

    contact_name = _extract_contact_name(soup)
    email = _extract_email(soup)

    return {
        "contact_name": contact_name or "-",
        "email": email or "-",
    }


def _extract_contact_name(soup: BeautifulSoup) -> str:

    # ── Pattern 1: dedicated CSS selectors ───────────────────────────────
    for sel in (
        ".contact-name",
        ".agent-name",
        ".block-agent-contact .name",
        ".block-agent .agent-name",
        "[class*='contact-name']",
        "[class*='agent-name']",
        "[class*='contact_name']",
        # Some sites wrap it in a dt/dd pair
        "dd.contact-name",
        "span.name",
    ):
        el = soup.select_one(sel)
        if el:
            name = el.get_text(strip=True)
            if name and len(name) >= 2:
                return name

    # ── Pattern 2: labelled text ("Лице за контакти: Иван Петров") ────────

    label_patterns = (
        "лице за контакти",
        "контактно лице",
        "контакт",
        "мениджър",
        "брокер",
        "agent",
        "отговорник",
    )
    for label_tag in soup.find_all(["strong", "b", "span", "label", "dt"]):
        tag_text = label_tag.get_text(strip=True).lower()
        if any(pat in tag_text for pat in label_patterns):
            # Try next sibling text node
            sibling = label_tag.next_sibling
            if sibling:
                candidate = (
                    sibling.strip()
                    if isinstance(sibling, str)
                    else sibling.get_text(strip=True)
                )
                if candidate and len(candidate) >= 2:
                    return candidate
            # Try parent element text (strip the label itself)
            parent_text = (
                label_tag.parent.get_text(strip=True) if label_tag.parent else ""
            )
            label_text = label_tag.get_text(strip=True)
            candidate = parent_text.replace(label_text, "").strip(" :–-")
            if candidate and len(candidate) >= 2:
                return candidate

    # ── Pattern 3: generic broker/agent containers ────────────────────────
    for sel in (
        ".block-broker",
        ".broker-info",
        ".agent-info",
        ".contact-person",
        "[class*='broker']",
        "[class*='agent']",
    ):
        el = soup.select_one(sel)
        if el:
            # Take only the first line or the first <p>/<span> inside it
            inner = el.select_one("p, span, strong")
            if inner:
                name = inner.get_text(strip=True)
                if name and len(name) >= 2:
                    return name

    return ""


def _extract_email(soup: BeautifulSoup) -> str:

    # ── Pattern 1: mailto link ────────────────────────────────────────────
    mailto = soup.select_one('a[href^="mailto:"]')
    if mailto:
        email = mailto.get("href", "").replace("mailto:", "").strip()
        # Strip any query string (e.g. ?subject=...)
        email = email.split("?")[0].strip()
        if email:
            return email

    # ── Pattern 2: dedicated CSS selectors ───────────────────────────────
    for sel in (
        ".email",
        "[class*='email']",
        ".contact-email",
        "span.mail",
        "a.email",
    ):
        el = soup.select_one(sel)
        if el:
            # Could be a text node or an href
            text = el.get_text(strip=True)
            if _EMAIL_RE.match(text):
                return text

    # ── Pattern 3: regex scan over visible text ───────────────────────────

    for content_sel in (
        "main",
        "#main",
        ".main-content",
        ".content",
        ".agency-profile",
        ".block-agent",
        "article",
        "body",  # last resort
    ):
        container = soup.select_one(content_sel)
        if container:
            text = container.get_text(" ", strip=True)
            match = _EMAIL_RE.search(text)
            if match:
                return match.group(0)

    return ""


# ---------------------------------------------------------------------------
# Pretty-print summary table (Rich)
# ---------------------------------------------------------------------------


def _print_summary(listings: list[Listing], today: str) -> None:
    """Render a Rich table of new listings to stdout."""
    table = Table(
        title=f"Нові оголошення — {today}",
        show_lines=True,
        style="bold",
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Назва", style="white")
    table.add_column("Ціна", style="green")
    table.add_column("Місто", style="yellow")
    table.add_column("Площа", style="blue")
    table.add_column("Телефон", style="magenta")
    table.add_column("Продавець", style="white")
    table.add_column("Тип", style="red")
    table.add_column("Контакт", style="yellow")
    table.add_column("Email", style="green")

    for idx, lst in enumerate(listings, start=1):
        type_style = (
            "[green]приватний[/green]"
            if "приватний" in lst.ad_type
            else "[orange1]від агенції[/orange1]"
        )
        table.add_row(
            str(idx),
            lst.ad_id,
            lst.title,
            lst.price,
            lst.location,
            lst.size,
            lst.phone,
            lst.seller_name,
            type_style,
            lst.contact_name or CONTACT_SENTINEL,
            lst.contact_email or CONTACT_SENTINEL,
        )

    console.print(table)


def _print_agencies_summary(agencies: list[AgencyRecord]) -> None:
    """Render a Rich table of scraped + enriched agencies to stdout."""
    table = Table(
        title="Scraped Agencies",
        show_lines=True,
        style="bold",
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("Agency Name", style="cyan")
    table.add_column("Phones", style="magenta")
    table.add_column("Email", style="green")
    table.add_column("Contact Name", style="yellow")

    for idx, rec in enumerate(agencies, start=1):
        table.add_row(
            str(idx),
            rec.agency_name,
            ", ".join(rec.phones),
            rec.email or "-",
            rec.contact_name or "-",
        )

    console.print(table)


def export_agencies_to_csv(
    agencies: list[AgencyRecord],
    csv_path: str | Path,
) -> None:

    if not agencies:
        logger.warning("No agencies to export to CSV.")
        return

    # Convert agency records to list of dicts
    data = []
    for rec in agencies:
        # For each agency, create a row with the first phone (if any)
        # and the city
        phone = rec.phones[0] if rec.phones else ""
        data.append(
            {
                "Agency Name": rec.agency_name,
                "Phone Number": phone,
                "City": rec.city,
            }
        )

    # Create DataFrame with the specified columns
    df = pd.DataFrame(data, columns=["Agency Name", "Phone Number", "City"])

    # Ensure the parent directory exists
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

    # Write to CSV with UTF-8 encoding (Bulgarian characters)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    logger.info("Exported %d agencies to CSV: %s", len(agencies), csv_path)


def _is_missing_contact(value: str) -> bool:
    val = (value or "").strip()
    return not val or val == CONTACT_SENTINEL


def backfill_new_ads_contacts(
    sheets: SheetsClient,
    agency_phones: set[str],
    agency_names: set[str],
    contact_resolver: ContactResolver,
) -> int:
    """
    Backfill Contact_Name/Contact_Email in New_Ads for rows with missing values.
    """
    rows = sheets.load_new_ads_for_backfill()
    updates: list[dict[str, Any]] = []

    for idx, row in enumerate(rows, start=1):
        if not _is_missing_contact(row["contact_name"]) and not _is_missing_contact(
            row["contact_email"]
        ):
            continue

        link = row.get("link", "").strip()
        if not link:
            updates.append(
                {
                    "row_number": row["row_number"],
                    "contact_name": row.get("contact_name", "").strip()
                    or CONTACT_SENTINEL,
                    "contact_email": row.get("contact_email", "").strip()
                    or CONTACT_SENTINEL,
                }
            )
            continue

        listing = Listing(
            ad_id=row.get("ad_id", "") or f"row-{row['row_number']}",
            title=f"Backfill row {idx}",
            price="",
            location="",
            size="",
            link=link,
            phone=row.get("phone", "").strip(),
            seller_name=row.get("seller_name", "").strip(),
            ad_type=row.get("ad_type", "").strip(),
            contact_name=row.get("contact_name", "").strip() or CONTACT_SENTINEL,
            contact_email=row.get("contact_email", "").strip() or CONTACT_SENTINEL,
        )

        enrich_listing(
            listing=listing,
            agency_phones=agency_phones,
            agency_names=agency_names,
            contact_resolver=contact_resolver,
        )

        updates.append(
            {
                "row_number": row["row_number"],
                "contact_name": listing.contact_name or CONTACT_SENTINEL,
                "contact_email": listing.contact_email or CONTACT_SENTINEL,
            }
        )

    sheets.update_new_ads_contacts(updates)
    return len(updates)


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily apartment-rental scraper for imoti.bg",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py                          # normal daily run
  python scraper.py --force                  # re-check all ads (ignore Processed_IDs)
  python scraper.py --dry-run                # scrape but don't write to Sheets or send email
  python scraper.py --force --dry-run        # combine: re-check everything, preview only
  python scraper.py --update-agencies        # scrape agency directory, update Agencies sheet
  python scraper.py --update-agencies --dry-run  # preview agency update without saving
  python scraper.py --backfill-contacts      # fill Contact_Name/Contact_Email in existing New_Ads
  python scraper.py --backfill-contacts --dry-run  # preview backfill without writing
        """,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-process all ads, even those already in Processed_IDs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        dest="dry_run",
        help="Scrape and log results but do NOT write to Sheets or send email.",
    )
    parser.add_argument(
        "--update-agencies",
        action="store_true",
        default=False,
        dest="update_agencies",
        help=(
            "Scrape https://imoti.bg/агенції (agency directory) and upsert "
            "the results into the Agencies sheet.  Can be combined with "
            "--dry-run to preview without saving."
        ),
    )
    parser.add_argument(
        "--backfill-contacts",
        action="store_true",
        default=False,
        dest="backfill_contacts",
        help=(
            "Backfill Contact_Name and Contact_Email for existing New_Ads rows "
            "that are empty or '-'. Updates only these two columns."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> int:

    args = _parse_args()

    # ── Load configuration ────────────────────────────────────────────────
    config = load_config()
    config.force = args.force
    config.dry_run = args.dry_run
    config.update_agencies = args.update_agencies
    config.backfill_contacts = args.backfill_contacts

    _setup_logging(config)

    today = date.today().isoformat()
    logger.info(
        "=== imoti.bg rental scraper — %s (force=%s, dry_run=%s, update_agencies=%s, backfill_contacts=%s) ===",
        today,
        config.force,
        config.dry_run,
        config.update_agencies,
        config.backfill_contacts,
    )

    # ── Connect to Google Sheets ──────────────────────────────────────────
    sheets = SheetsClient(config)
    try:
        sheets.connect()
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to connect to Google Sheets: %s", exc)
        return 1

    # ── HTTP session ──────────────────────────────────────────────────────
    session = _make_session(config)
    mysql_store: Optional[MySQLStore] = None

    if config.mysql_enabled:
        try:
            mysql_store = MySQLStore(config)
            mysql_store.connect()
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to connect to MySQL: %s", exc)
            return 1

    # ═════════════════════════════════════════════════════════════════════
    # PATH A: --update-agencies

    # ═════════════════════════════════════════════════════════════════════
    if config.update_agencies:
        logger.info("--- Agency update mode ---")
        try:
            agency_records = scrape_agencies(session, config)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error scraping agencies: %s", exc, exc_info=True)
            return 1

        if agency_records:
            _print_agencies_summary(agency_records)
            agency_dicts = [rec.to_dict() for rec in agency_records]

            # Export to CSV if path is configured
            if config.agencies_csv_path:
                try:
                    export_agencies_to_csv(agency_records, config.agencies_csv_path)
                except Exception as exc:  # noqa: BLE001
                    logger.error("Failed to export agencies to CSV: %s", exc)

            try:
                sheets.upsert_agencies(agency_dicts)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to upsert agencies: %s", exc, exc_info=True)
                return 1
            if mysql_store is not None:
                if config.dry_run:
                    logger.info(
                        "[DRY-RUN] Would upsert %d agencies into MySQL.",
                        len(agency_dicts),
                    )
                else:
                    try:
                        mysql_store.upsert_agencies(agency_dicts)
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "Failed to upsert agencies into MySQL: %s",
                            exc,
                            exc_info=True,
                        )
                        return 1
        else:
            logger.warning(
                "No agency records were scraped from the agencies directory."
            )

        # If the user only wanted to update agencies (no rental scraping), stop here.
        # If they also want the normal scrape, fall through.
        logger.info("Agency update complete.")

    # ═════════════════════════════════════════════════════════════════════
    # PATH B: Normal daily scraping of rental listings
    # ═════════════════════════════════════════════════════════════════════

    # ── Load agency reference data ─────────────────────────────────────────
    try:
        agency_phones: set[str] = sheets.load_agency_phones()
        agency_names: set[str] = sheets.load_agency_names()
        agency_contact_map = sheets.load_agency_contact_map()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to load data from Google Sheets: %s", exc)
        return 1

    if mysql_store is not None:
        try:
            agency_phones |= mysql_store.load_agency_phones()
            agency_names |= mysql_store.load_agency_names()
            mysql_contacts = mysql_store.load_agency_contact_map()
            agency_contact_map.update(mysql_contacts)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to load agency reference data from MySQL: %s",
                exc,
                exc_info=True,
            )
            return 1

    contact_resolver = ContactResolver(
        session=session,
        config=config,
        agency_contacts=agency_contact_map,
    )

    if config.backfill_contacts:
        logger.info("--- Backfill contacts mode ---")
        try:
            updated = backfill_new_ads_contacts(
                sheets=sheets,
                agency_phones=agency_phones,
                agency_names=agency_names,
                contact_resolver=contact_resolver,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Backfill contacts failed: %s", exc, exc_info=True)
            return 1
        logger.info("Backfill contacts complete. Rows updated: %d", updated)
        if mysql_store is not None and not config.dry_run:
            try:
                rows_for_sync = sheets.load_new_ads_for_backfill()
                mysql_store.upsert_from_new_ads_sheet_rows(rows_for_sync)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Failed to sync backfilled New_Ads to MySQL: %s", exc, exc_info=True
                )
                return 1
        return 0

    # ── Load already-processed IDs ────────────────────────────────────────
    try:
        processed_ids: set[str] = sheets.load_processed_ids()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to load processed ids from Google Sheets: %s", exc)
        return 1
    if mysql_store is not None:
        try:
            processed_ids |= mysql_store.load_processed_ids()
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to load processed ids from MySQL: %s", exc, exc_info=True
            )
            return 1

    if config.force:
        logger.info("--force flag set: ignoring %d processed IDs.", len(processed_ids))
        processed_ids = set()

    # ── Scrape listing pages ──────────────────────────────────────────────
    try:
        all_listings = scrape_all_pages(session, config)
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error during scraping: %s", exc, exc_info=True)
        return 1

    # ── Filter out already-processed ads ─────────────────────────────────
    new_listings = [lst for lst in all_listings if lst.ad_id not in processed_ids]
    logger.info(
        "%d total apartments scraped; %d are new (not yet processed).",
        len(all_listings),
        len(new_listings),
    )

    if not new_listings:
        logger.info("No new listings today — nothing to do.")
        return 0

    # ── Enrich new listings ────────────────────────────────────────────────

    logger.info("Enriching %d new listing(s) concurrently …", len(new_listings))
    
    enrich_lock = threading.Lock()
    
    def _enrich_single(idx: int, lst: Listing) -> None:
        with enrich_lock:
            logger.info(
                "  [%d/%d] Ad %s — '%s'",
                idx,
                len(new_listings),
                lst.ad_id,
                lst.title,
            )
        try:
            enrich_listing(
                listing=lst,
                agency_phones=agency_phones,
                agency_names=agency_names,
                contact_resolver=contact_resolver,
            )
        except Exception as exc:  # noqa: BLE001
            with enrich_lock:
                logger.warning("Could not enrich ad %s: %s", lst.ad_id, exc)
            lst.ad_type = "невідомо"

        # Send Telegram notification if it's a private lead
        if "приватний" in lst.ad_type:
            from telegram_notifier import send_telegram_lead
            send_telegram_lead(config, lst)

    # Use max_workers=5 to speed it up while avoiding aggressive rate-limiting
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_enrich_single, i, lst): lst 
            for i, lst in enumerate(new_listings, start=1)
        }
        for future in concurrent.futures.as_completed(futures):
            exc = future.exception()
            if exc is not None:
                logger.error("Enrichment thread failed: %s", exc)

    # ── Display summary table ─────────────────────────────────────────────
    _print_summary(new_listings, today)

    # ── Write to Google Sheets ────────────────────────────────────────────
    new_rows = [lst.as_row(today) for lst in new_listings]
    new_ids = [lst.ad_id for lst in new_listings]

    if not config.dry_run:
        try:
            sheets.append_new_ads(new_rows)
            sheets.mark_processed(new_ids)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to write to Google Sheets: %s", exc, exc_info=True)
            return 1
        if mysql_store is not None:
            try:
                mysql_rows = []
                for lst in new_listings:
                    row = lst.as_dict()
                    row["Date"] = today
                    mysql_rows.append(row)
                mysql_store.upsert_listings(mysql_rows)
                mysql_store.mark_processed(new_ids)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to write data to MySQL: %s", exc, exc_info=True)
                return 1
    else:
        logger.info(
            "[DRY-RUN] Would write %d new ad row(s) and %d processed ID(s).",
            len(new_rows),
            len(new_ids),
        )
        if mysql_store is not None:
            logger.info(
                "[DRY-RUN] Would write %d listing row(s) and %d processed ID(s) to MySQL.",
                len(new_rows),
                len(new_ids),
            )

    # ── Send email notification ───────────────────────────────────────────
    try:
        send_email(
            config=config,
            ads=[lst.as_dict() for lst in new_listings],
            today=today,
        )
    except Exception as exc:  # noqa: BLE001
        # Email failure is non-fatal: data is already saved in Sheets.
        logger.error("Email notification failed (data was saved): %s", exc)

    logger.info(
        "=== Done: %d new listing(s) processed on %s ===",
        len(new_listings),
        today,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
