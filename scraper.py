from __future__ import annotations

import argparse
import concurrent.futures
import logging
import random
import re
import sys
import threading
import time
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
from utils import extract_email, extract_names, extract_phone_numbers, looks_like_person_name

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


def _ensure_utf8_stdio() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except Exception:  # noqa: BLE001
                pass


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


@dataclass
class ParserRunResult:
    exit_code: int
    today: str
    total_scraped: int = 0
    new_count: int = 0
    new_listings: list[Listing] | None = None
    message: str = ""


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
            logger.warning("HTTP %s for %s (attempt %d/%d)", status, url, attempt, retries)
        except requests.RequestException as exc:
            logger.warning("Request error for %s (attempt %d/%d): %s", url, attempt, retries, exc)

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
    """Extract the first valid phone number from text using improved logic."""
    if not text:
        return ""

    phones = extract_phone_numbers(text)
    return phones[0] if phones else ""


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
        price_lines = [ln.strip() for ln in price_el.get_text("\n").splitlines() if ln.strip()]
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
        phone = _extract_phone_from_text(raw)
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
            phones = extract_phone_numbers(el.get_text(strip=True))
            if phones:
                return phones[0]

    # 3. Text scan for "Тел" prefix in the full card text
    full_text = article.get_text(" ", strip=True)
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
                # Check if the name looks like a person's name
                if looks_like_person_name(name):
                    return name
                # If it doesn't look like a person's name, return as seller name
                return name

    # If no specific selector found, try to extract from the full article
    text_content = article.get_text(" ", strip=True)
    names = extract_names(text_content)
    if names:
        return names[0]

    return ""


def parse_detail_page(html: str, url: str = "") -> DetailPageInfo:

    soup = BeautifulSoup(html, "html.parser")
    info = DetailPageInfo()

    # ── Seller name from page title or header ─────────────────────────────
    for sel in (
        "h1 a",
        ".product-title a",
        ".property-title",
        "[class*='owner']",
        "[class*='seller']",
        "[class*='agency']",
    ):
        el = soup.select_one(sel)
        if el:
            info.seller_name = el.get_text(strip=True)
            if info.seller_name:
                break

    if not info.seller_name:
        title_tag = soup.select_one("title")
        if title_tag:
            # Extract seller from title like "Продава двустаен апартамент в Студентски | Частно лице"
            title_text = title_tag.get_text()
            if "|" in title_text:
                potential_seller = title_text.split("|")[-1].strip()
                if looks_like_person_name(potential_seller):
                    info.seller_name = potential_seller

    # ── Phone number extraction ────────────────────────────────────────────
    # First, try to extract from structured elements
    for sel in (
        ".phone-number",
        ".contact-phone",
        "[class*='phone']",
        "[class*='tel']",
    ):
        el = soup.select_one(sel)
        if el:
            phones = extract_phone_numbers(el.get_text(strip=True))
            if phones:
                info.phone = phones[0]
                break

    # If not found in structured elements, search in all text
    if not info.phone:
        all_text = soup.get_text(" ", strip=True)
        phones = extract_phone_numbers(all_text)
        if phones:
            info.phone = phones[0]

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
            if looks_like_person_name(candidate):
                return candidate

        # Fallback tags inside block.
        for tag in block.find_all(["span", "strong", "b", "p", "div"]):
            candidate = tag.get_text(" ", strip=True)
            if looks_like_person_name(candidate):
                return candidate

        # Last resort: use block text stripped from phone/email.
        text = block.get_text(" ", strip=True)
        text = _EMAIL_RE.sub("", text)
        text = _PHONE_TEXT_RE.sub("", text)
        text = text.strip(" @,;:|–-")
        if looks_like_person_name(text):
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
        if looks_like_person_name(candidate):
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
        matches = extract_email(txt)
        if matches:
            return matches[0]

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
        matches = extract_email(el.get_text(" ", strip=True))
        if matches:
            return matches[0]

    return "-"


def _extract_agency_profile_url_from_detail_soup(soup: BeautifulSoup) -> str:
    for root in soup.select("div.block-info, div.block-agent, body"):
        for a_tag in root.select("a[href]"):
            href = a_tag.get("href", "").strip()
            if not href or not _AGU_HREF_RE.search(href):
                continue
            return href if href.startswith("http") else urljoin("https://imoti.bg/", href)
    return ""


def _looks_like_person_name(text: str) -> bool:
    """Use improved name validation from utils module."""
    return looks_like_person_name(text)


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
        if not resp:
            logger.error("Failed to fetch page %d", page_num)
            continue

        try:
            listings = parse_listing_page(resp.text, url, config.city_filter)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to parse page %d: %s", page_num, exc)
            continue

        new_on_page = 0
        for listing in listings:
            if listing.ad_id not in seen_ids:
                all_listings.append(listing)
                seen_ids.add(listing.ad_id)
                new_on_page += 1

        logger.info(
            "Found %d new listings on page %d (total so far: %d)",
            new_on_page,
            page_num,
            len(all_listings),
        )

        if not _has_next_page(BeautifulSoup(resp.text, "html.parser")):
            logger.info("No more pages detected, stopping early.")
            break

    return all_listings


def _resolve_agency_contact_key(
    listing: Listing,
    agency_phones: set[str],
    agency_names: set[str],
) -> Optional[tuple[str, str]]:
    """
    Resolve if a listing belongs to an agency based on phone or name match.

    Returns:
        (agency_phone, agency_name) tuple if match found, None otherwise
    """
    if not listing.phone and not listing.seller_name:
        return None

    # Match by phone
    if listing.phone in agency_phones:
        return (listing.phone, "")

    # Match by name
    if listing.seller_name and listing.seller_name in agency_names:
        return ("", listing.seller_name)

    return None


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
            info = parse_detail_page(resp.text, listing.link)
        self._detail_cache[key] = info
        return info

    def resolve_from_agency_sheet(self, seller_name: str) -> dict[str, str]:
        seller_name_lower = (seller_name or "").strip().lower()
        if not seller_name_lower:
            return {"contact_name": CONTACT_SENTINEL, "contact_email": CONTACT_SENTINEL}

        if seller_name_lower in self._agency_cache:
            return self._agency_cache[seller_name_lower]

        key = _resolve_agency_contact_key(seller_name_lower, self._agency_contacts_source)
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

    if listing.ad_type == "частно лице":
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
                if name == CONTACT_SENTINEL and profile_contact["contact_name"] != CONTACT_SENTINEL:
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
        return "от агенция"

    # 2. Seller name contains any agency name substring.
    if seller_name:
        seller_lower = seller_name.lower()
        for agency_name in agency_names:
            if agency_name and agency_name in seller_lower:
                return "от агенция"

    return "частно лице"


# ---------------------------------------------------------------------------
# Agency scraping
# ---------------------------------------------------------------------------


def scrape_agencies(
    session: requests.Session,
    config: Config,
) -> list[AgencyRecord]:

    all_agencies: list[AgencyRecord] = []
    seen_agencies: set[str] = set()

    for page_num in range(1, config.max_agency_pages + 1):
        url = config.agencies_url.format(page=page_num)
        logger.info("Scraping agencies page %d → %s", page_num, url)

        resp = _polite_get(session, url, config)
        if not resp:
            logger.error("Failed to fetch agencies page %d", page_num)
            continue

        try:
            agencies = _parse_agency_page(resp.text, url)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to parse agencies page %d: %s", page_num, exc)
            continue

        new_on_page = 0
        for agency in agencies:
            # Use agency name and primary phone as unique identifier
            unique_id = (agency.agency_name or "").lower()
            if agency.phones:
                unique_id += "_" + agency.phones[0]

            if unique_id not in seen_agencies:
                all_agencies.append(agency)
                seen_agencies.add(unique_id)
                new_on_page += 1

        logger.info(
            "Found %d new agencies on page %d (total so far: %d)",
            new_on_page,
            page_num,
            len(all_agencies),
        )

        soup = BeautifulSoup(resp.text, "html.parser")
        if not _has_next_page(soup):
            logger.info("No more agency pages detected, stopping early.")
            break

    return all_agencies


def _parse_agency_page(html: str, base_url: str) -> list[AgencyRecord]:
    soup = BeautifulSoup(html, "html.parser")
    agencies: list[AgencyRecord] = []

    # Find agency containers - typically in grid or list format
    for container in soup.select("article.product-classic, .agency-item, .agency-card"):
        agency = _parse_agency_container(container, base_url)
        if agency:
            agencies.append(agency)

    return agencies


def _parse_agency_container(container: BeautifulSoup, base_url: str) -> Optional[AgencyRecord]:
    try:
        # Agency name
        name_selector = (".agency-name", ".product-classic-title a", "h3", "[class*='title']")
        agency_name = ""
        for sel in name_selector:
            el = container.select_one(sel)
            if el:
                agency_name = el.get_text(strip=True)
                break

        if not agency_name:
            return None

        # Agency detail URL
        detail_url = ""
        link_el = container.select_one("a[href]")
        if link_el:
            href = link_el.get("href", "")
            detail_url = href if href.startswith("http") else urljoin(base_url, href)

        # Extract contact information
        agency_data = parse_agency_details(detail_url) if detail_url else {}

        # Fallback to extracting from container if no detail page was fetched
        if not agency_data.get("phones"):
            # Extract phones from the listing container
            phones = []
            phone_elements = container.select("[class*='phone'], [class*='tel'], .contact-info")
            for el in phone_elements:
                phone_text = el.get_text(strip=True)
                extracted_phones = extract_phone_numbers(phone_text)
                phones.extend(extracted_phones)
            agency_data["phones"] = list(set(phones))  # Remove duplicates

        if not agency_data.get("contact_name"):
            # Extract names from the container
            text_content = container.get_text(" ", strip=True)
            names = extract_names(text_content)
            if names:
                agency_data["contact_name"] = names[0]

        if not agency_data.get("email"):
            # Extract emails from the container
            text_content = container.get_text(" ", strip=True)
            emails = extract_email(text_content)
            if emails:
                agency_data["email"] = emails[0]

        # City - might be in .btext or similar location indicator
        city_el = container.select_one(".btext, .location, .city")
        city = city_el.get_text(strip=True) if city_el else ""

        return AgencyRecord(
            agency_name=agency_name,
            phones=agency_data.get("phones", []),
            city=city,
            email=agency_data.get("email", ""),
            contact_name=agency_data.get("contact_name", ""),
            detail_url=detail_url,
        )
    except Exception as e:
        logger.debug("Error parsing agency container: %s", e)
        return None


def parse_agency_details(
    detail_url: str, session: requests.Session, config: Config
) -> dict[str, Any]:
    """
    Parse agency detail page to extract comprehensive contact information.
    """
    if not detail_url:
        return {}

    # Use _polite_get to respect delays and handle retries
    resp = _polite_get(session, detail_url, config)
    if not resp:
        logger.debug("Could not fetch agency detail page %s", detail_url)
        return {}

    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        result = {
            "phones": [],
            "contact_name": "",
            "email": "",
        }

        # Extract phone numbers from the detail page
        text_content = soup.get_text(" ", strip=True)
        result["phones"] = extract_phone_numbers(text_content)

        # Extract contact name from detail page
        names = extract_names(text_content)
        if names:
            result["contact_name"] = names[0]

        # Extract email from detail page
        emails = extract_email(text_content)
        if emails:
            result["email"] = emails[0]

        return result
    except Exception as e:
        logger.debug("Error parsing agency detail page %s: %s", detail_url, e)
        return {}


def _extract_contact_name(soup: BeautifulSoup) -> str:
    """
    Extract contact name from soup with improved logic from utils.
    """
    text_content = soup.get_text(" ", strip=True)
    names = extract_names(text_content)
    return names[0] if names else ""


def _extract_email(soup: BeautifulSoup) -> str:
    """
    Extract email from soup with improved logic from utils.
    """
    text_content = soup.get_text(" ", strip=True)
    emails = extract_email(text_content)
    return emails[0] if emails else ""


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
            "[green]частно лице[/green]"
            if "частно лице" in lst.ad_type
            else "[orange1]от агенция[/orange1]"
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


def export_agencies_to_csv(agencies: list[AgencyRecord], path: Path) -> None:
    df = pd.DataFrame([agency.to_dict() for agency in agencies])
    df.to_csv(path, index=False, encoding="utf-8")
    logger.info("Exported %d agencies to %s", len(agencies), path)


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
                    "contact_name": row.get("contact_name", "").strip() or CONTACT_SENTINEL,
                    "contact_email": row.get("contact_email", "").strip() or CONTACT_SENTINEL,
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
# Command-line interface
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape imoti.bg rental listings and upload to Google Sheets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
 Examples:
  python scraper.py --dry-run          # Test run without saving
  python scraper.py --update-agencies  # Update agency contacts
  python scraper.py --backfill         # Backfill missing contacts
  python scraper.py --force            # Re-process all ads
        """.strip(),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't save to Google Sheets or send email",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-process all ads even if already in Processed_IDs",
    )
    parser.add_argument(
        "--update-agencies",
        action="store_true",
        help="Scrape agencies and update the Agencies sheet",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        dest="backfill_contacts",
        help="Enrich existing New_Ads rows with Contact_Name/Contact_Email",
    )
    parser.add_argument(
        "--city-filter",
        type=str,
        help="Only process listings from this city (case-insensitive)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=30,
        help="Max number of listing pages to scrape (default: 30)",
    )
    parser.add_argument(
        "--max-agency-pages",
        type=int,
        default=15,
        help="Max number of agency pages to scrape (default: 15)",
    )

    return parser.parse_args()


def run_parser_once(config: Config) -> ParserRunResult:
    """Execute one complete run of the parser."""
    today = date.today().strftime("%Y-%m-%d")
    logger.info(
        "=== imoti.bg rental scraper — %s (force=%s, dry_run=%s, update_agencies=%s, backfill_contacts=%s) ===",
        today,
        config.force,
        config.dry_run,
        config.update_agencies,
        config.backfill_contacts,
    )

    # ── Connect to Google Sheets ──────────────────────────────────────────
    sheets = SheetsClient(
        sheet_id=config.google_sheet_id,
        service_account_file=config.service_account_json,
        sheet_name=config.sheet_name,
        ws_new_ads=config.ws_new_ads,
        ws_agencies=config.ws_agencies,
        ws_processed=config.ws_processed,
        ws_renters=config.ws_renters,
        new_ads_headers=config.new_ads_headers,
        agencies_headers=config.agencies_headers,
        renters_headers=config.renters_headers,
    )

    try:
        sheets.connect()
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        return ParserRunResult(exit_code=1, today=today, message=str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to connect to Google Sheets: %s", exc)
        return ParserRunResult(
            exit_code=1, today=today, message=f"Failed to connect to Google Sheets: {exc}"
        )

    # ── HTTP session ──────────────────────────────────────────────────────
    session = _make_session(config)
    mysql_store: Optional[MySQLStore] = None

    if config.mysql_enabled:
        try:
            mysql_store = MySQLStore(config)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to connect to MySQL: %s", exc)
            return ParserRunResult(
                exit_code=1, today=today, message=f"Failed to connect to MySQL: {exc}"
            )

    # ═════════════════════════════════════════════════════════════════════
    # PATH A: --update-agencies

    # ═════════════════════════════════════════════════════════════════════
    if config.update_agencies:
        logger.info("--- Agency update mode ---")
        try:
            agency_records = scrape_agencies(session, config)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error scraping agencies: %s", exc, exc_info=True)
            return ParserRunResult(
                exit_code=1, today=today, message=f"Error scraping agencies: {exc}"
            )

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
                return ParserRunResult(
                    exit_code=1, today=today, message=f"Failed to upsert agencies: {exc}"
                )
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
                        return ParserRunResult(
                            exit_code=1,
                            today=today,
                            message=f"Failed to upsert agencies into MySQL: {exc}",
                        )
        else:
            logger.warning("No agency records were scraped from the agencies directory.")

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
        return ParserRunResult(
            exit_code=1, today=today, message=f"Failed to load data from Google Sheets: {exc}"
        )

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
            return ParserRunResult(
                exit_code=1,
                today=today,
                message=f"Failed to load agency reference data from MySQL: {exc}",
            )

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
            return ParserRunResult(
                exit_code=1, today=today, message=f"Backfill contacts failed: {exc}"
            )
        logger.info("Backfill contacts complete. Rows updated: %d", updated)
        if mysql_store is not None and not config.dry_run:
            try:
                rows_for_sync = sheets.load_new_ads_for_backfill()
                mysql_store.upsert_from_new_ads_sheet_rows(rows_for_sync)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to sync backfilled New_Ads to MySQL: %s", exc, exc_info=True)
                return ParserRunResult(
                    exit_code=1,
                    today=today,
                    message=f"Failed to sync backfilled New_Ads to MySQL: {exc}",
                )
        return ParserRunResult(
            exit_code=0, today=today, message=f"Backfill complete. Updated rows: {updated}"
        )

    # ── Load already-processed IDs ────────────────────────────────────────
    try:
        processed_ids: set[str] = sheets.load_processed_ids()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to load processed ids from Google Sheets: %s", exc)
        return ParserRunResult(
            exit_code=1,
            today=today,
            message=f"Failed to load processed ids from Google Sheets: {exc}",
        )
    if mysql_store is not None:
        try:
            processed_ids |= mysql_store.load_processed_ids()
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to load processed ids from MySQL: %s", exc, exc_info=True)
            return ParserRunResult(
                exit_code=1, today=today, message=f"Failed to load processed ids from MySQL: {exc}"
            )

    if config.force:
        logger.info("--force flag set: ignoring %d processed IDs.", len(processed_ids))
        processed_ids = set()

    # ── Scrape listing pages ──────────────────────────────────────────────
    try:
        all_listings = scrape_all_pages(session, config)
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error during scraping: %s", exc, exc_info=True)
        return ParserRunResult(
            exit_code=1, today=today, message=f"Unexpected error during scraping: {exc}"
        )

    # ── Filter out already-processed ads ─────────────────────────────────
    new_listings = [lst for lst in all_listings if lst.ad_id not in processed_ids]
    logger.info(
        "%d total apartments scraped; %d are new (not yet processed).",
        len(all_listings),
        len(new_listings),
    )

    if not new_listings:
        logger.info("No new listings today — nothing to do.")
        return ParserRunResult(
            exit_code=0,
            today=today,
            total_scraped=len(all_listings),
            new_count=0,
            new_listings=[],
            message="No new listings found.",
        )

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
            lst.ad_type = "unknown"

        # Send Telegram notification if it's a private lead
        if "частно лице" in lst.ad_type:
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
            sheets.append_new_ads(new_listings)
            sheets.mark_processed(new_ids)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to write to Google Sheets: %s", exc, exc_info=True)
            return ParserRunResult(
                exit_code=1, today=today, message=f"Failed to write to Google Sheets: {exc}"
            )
        if mysql_store is not None:
            try:
                mysql_rows = []
                for lst in new_listings:
                    row = lst.as_dict()
                    row["Date"] = today
                    mysql_rows.append(row)
                    mysql_store.store_listings(mysql_rows)
                mysql_store.mark_processed(new_ids)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to write data to MySQL: %s", exc, exc_info=True)
                return ParserRunResult(
                    exit_code=1, today=today, message=f"Failed to write data to MySQL: {exc}"
                )
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
    return ParserRunResult(
        exit_code=0,
        today=today,
        total_scraped=len(all_listings),
        new_count=len(new_listings),
        new_listings=new_listings,
        message=f"Done: {len(new_listings)} new listing(s) processed on {today}.",
    )


def main() -> int:
    _ensure_utf8_stdio()

    args = _parse_args()

    # Load config and apply CLI overrides
    config = load_config()
    config.force = args.force
    config.dry_run = args.dry_run
    config.update_agencies = args.update_agencies
    config.backfill_contacts = args.backfill_contacts
    if hasattr(args, "city_filter") and args.city_filter:
        config.city_filter = args.city_filter
    if hasattr(args, "max_pages") and args.max_pages:
        config.max_pages = args.max_pages
    if hasattr(args, "max_agency_pages") and args.max_agency_pages:
        config.max_agency_pages = args.max_agency_pages

    _setup_logging(config)
    logger.info("Parser started with config: dry_run=%s, force=%s", config.dry_run, config.force)

    try:
        if config.backfill_contacts:
            session = _make_session(config)
            sheets = SheetsClient(config)

            if not config.dry_run:
                sheets.connect()

            updated = backfill_new_ads_contacts(sheets, config, session)
            logger.info(f"Backfill completed: updated {updated} listings")
            return 0

        result = run_parser_once(config)
        logger.info("Parser run completed successfully")
        return result.exit_code
    except KeyboardInterrupt:
        logger.info("Parser interrupted by user")
        return 130  # Standard exit code for Ctrl+C
    except Exception as e:
        logger.error(f"Parser failed with error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
