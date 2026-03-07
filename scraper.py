#!/usr/bin/env python3
"""
scraper.py — Daily apartment-rental scraper for imoti.bg
=========================================================

Run directly:
    python scraper.py                        # normal daily run
    python scraper.py --force                # re-check ALL ads, ignore Processed_IDs
    python scraper.py --dry-run              # scrape but don't write to Sheets or send email
    python scraper.py --update-agencies      # scrape agencies page and refresh Agencies sheet
    python scraper.py --force --dry-run      # combine flags as needed
    python scraper.py --update-agencies --dry-run  # preview agency update without saving

The script:
1.  Paginates through https://imoti.bg/наеми/page:N and collects listing cards.
2.  Filters for apartments ("апартамент" in title or URL slug).
3.  Skips Ad IDs already in the "Processed_IDs" Google Sheet.
4.  For each new ad:
      a. Tries to extract Phone + Seller_Name from the listing card (list page).
      b. If either is missing, visits the detail page to fill in the blanks.
5.  Classifies each listing as "приватний" or "від агенції" using:
      • Agency phone set (from Agencies sheet).
      • Agency name substring matching against Seller_Name.
6.  Appends new rows to "New_Ads" and records Ad IDs in "Processed_IDs".
7.  Sends an HTML summary email if any new ads were found.

With --update-agencies:
    Scrapes https://imoti.bg/агенції/page:N and upserts into the Agencies sheet.

Project layout
──────────────
    imoti_scraper/
    ├── scraper.py       ← this file (entry point)
    ├── config.py        ← settings loaded from .env
    ├── sheets.py        ← Google Sheets client
    ├── email_sender.py  ← SMTP email helpers
    ├── .env             ← your secrets (not committed to git)
    ├── .env.example     ← template
    ├── requirements.txt
    └── README.md
"""

from __future__ import annotations

import argparse
import logging
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
import pandas as pd

from config import Config, load_config
from email_sender import send_email
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
    """Represents a single apartment-rental listing collected from imoti.bg."""

    ad_id: str
    title: str
    price: str
    location: str
    size: str
    link: str
    # Populated during enrichment (detail page visit or list page extraction):
    phone: str = ""
    seller_name: str = ""     # e.g. "Частно лице" or "Агенция XYZ"
    ad_type: str = ""         # "приватний" | "від агенції"

    def as_row(self, today: str) -> list[str]:
        """
        Return a flat list matching Config.new_ads_headers column order:
            [Date, Ad_ID, Title, Price, Location, Size, Link, Phone, Seller_Name, Type]
        """
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
        ]

    def as_dict(self) -> dict[str, str]:
        """Return a dict keyed by Config.new_ads_headers for the email template."""
        return {
            "Date":        "",
            "Ad_ID":       self.ad_id,
            "Title":       self.title,
            "Price":       self.price,
            "Location":    self.location,
            "Size":        self.size,
            "Link":        self.link,
            "Phone":       self.phone,
            "Seller_Name": self.seller_name,
            "Type":        self.ad_type,
        }


@dataclass
class AgencyRecord:
    """Represents one agency scraped from the imoti.bg agencies directory."""

    agency_name: str
    phones: list[str]  # list of normalised phone strings (digits only)
    email: str = ""
    city: str = ""  # Optional city (e.g., Варна, Пловдив)

    def to_dict(self) -> dict[str, str]:
        return {
            "Agency_Name": self.agency_name,
            "Phones":      ",".join(self.phones),
            "Email":       self.email,
            "City":        self.city,
        }


# ---------------------------------------------------------------------------
# HTTP session factory — with User-Agent rotation
# ---------------------------------------------------------------------------

def _make_session(config: Config) -> requests.Session:
    """
    Create a requests.Session with a randomly chosen User-Agent and realistic
    browser headers.

    A new User-Agent is selected from config.user_agents on each call, which
    means that different runs use different UAs (harder to fingerprint).
    """
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
            "Connection":      "keep-alive",
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
    """
    Fetch *url* with a random polite delay and simple retry logic.

    Rotates the User-Agent header on every retry to avoid detection.

    Args:
        session:  Active requests.Session.
        url:      URL to fetch.
        config:   Config object (for delay settings and UA pool).
        retries:  Number of attempts before giving up.
        backoff:  Extra seconds added between retry attempts.

    Returns:
        A requests.Response on success, or None if all retries fail.
    """
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

# Pattern to extract a Bulgarian phone number from free text.
# Matches common formats: 0888123456, +359888123456, 0 888 123 456, etc.
_PHONE_TEXT_RE = re.compile(
    r"(?:тел\.?\s*:?\s*)?(\+?359[\s\-]?|0)(\d[\d\s\-]{6,12}\d)",
    re.IGNORECASE,
)


def _extract_ad_id(url: str) -> str:
    """
    Extract the numeric Ad ID from a detail-page URL.

    Example:
        "https://imoti.bg/наеми/двустаен-апартамент/софия/бункера-513894.htm"
        → "513894"
    """
    match = _AD_ID_RE.search(url)
    return match.group(1) if match else ""


def _is_apartment(title: str, url: str) -> bool:
    """
    Return True if either the card title or the URL slug contains an apartment
    keyword.

    This covers listings like:
    • "Двустаен апартамент"
    • URL slug "едностаен-апартамент"
    """
    combined = (title + " " + url).lower()
    return any(kw in combined for kw in APARTMENT_KEYWORDS)


def _extract_phone_from_text(text: str) -> str:
    """
    Attempt to extract a phone number from arbitrary text using a regex.

    Returns the normalised (digits-only) phone string, or "" if not found.
    """
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
    """
    Parse one results page and return a list of Listing objects.

    Only apartment-type listings are returned.  If *city_filter* is set, only
    listings whose location contains that string (case-insensitive) are kept.

    This function also attempts to extract the phone number and seller name
    directly from the listing card (avoiding a detail-page request).

    Args:
        html:         Raw HTML string of the listings page.
        base_url:     The base URL of the site (used to build absolute links).
        city_filter:  Optional city substring to filter by.

    Returns:
        List of Listing dataclass instances.
    """
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
    """
    Extract fields from a single <article class="product-classic"> element.

    In addition to the basic fields, this also attempts to extract:
    • Phone number — from plain text "Тел: 0894..." or <a href="tel:...">
    • Seller name — from the agency/seller block on the card (if visible)

    Returns None if the card has no valid link.
    """
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
        # The price block often has two values (EUR and BGN).  Keep the first
        # line which is typically the EUR price.
        price_lines = [ln.strip() for ln in price_el.get_text("\n").splitlines() if ln.strip()]
        price = price_lines[0] if price_lines else ""
    else:
        price = ""

    # ── Location ──────────────────────────────────────────────────────────
    location_el = article.select_one(".btext")
    location = location_el.get_text(strip=True) if location_el else ""

    # ── Size ──────────────────────────────────────────────────────────────
    # Size is in the first <li> of .product-classic-list that contains "кв.м."
    size = ""
    for li in article.select(".product-classic-list li"):
        text = li.get_text(strip=True)
        if "кв.м." in text:
            size = text
            break

    # ── Phone (opportunistic from card) ───────────────────────────────────
    # Some cards expose the phone number as plain text ("Тел: 0894...") or
    # as <a href="tel:...">.  Extract here to skip the detail-page request.
    phone = _extract_phone_from_card(article)

    # ── Seller Name (opportunistic from card) ─────────────────────────────
    # Some cards show the seller/agency name in a small block.
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
    """
    Try to find a phone number inside a listing card element.

    Checks:
    1. <a href="tel:..."> anchor (most reliable)
    2. Any element with class containing "phone" or "tel"
    3. Text containing "Тел" pattern (regex scan)

    Returns normalised phone string or "" if not found.
    """
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
            if len(phone) >= 9:   # sanity check: real Bulgarian numbers are 9-12 digits
                return phone

    # 3. Text scan for "Тел" prefix in the full card text
    full_text = article.get_text(" ", strip=True)
    if "тел" in full_text.lower():
        phone = _extract_phone_from_text(full_text)
        if phone:
            return phone

    return ""


def _extract_seller_name_from_card(article: BeautifulSoup) -> str:
    """
    Try to extract the seller / agency name from a listing card.

    Checks common patterns used by imoti.bg card layouts.
    Returns the name string (stripped) or "" if not found.
    """
    # Try several common CSS patterns for the seller block on listing cards.
    for selector in (
        ".product-classic-agency",
        ".agency-name",
        ".seller-name",
        "[class*='agency']",
        "[class*='seller']",
        ".block-info h3",   # may appear in card previews
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

def parse_detail_page(html: str) -> tuple[str, str]:
    """
    Parse the detail page of a listing to extract the phone number and
    the seller / agency name.

    The relevant HTML section looks like:

        <div class="block-info">
          <h3>Частно лице</h3>          ← private person, or the agency name
          <ul class="block-person-list">
            <li>
              <div class="block-person-link">
                <a href="tel:0894860795">0894860795</a>
              </div>
            </li>
          </ul>
        </div>

    Also tries:
        <div class="block-agent">
          <h3 class="block-agent-name">Агенция XYZ</h3>
          <a href="tel:...">…</a>
        </div>

    Args:
        html: Raw HTML of the detail page.

    Returns:
        Tuple (normalised_phone, seller_name):
            normalised_phone — digits-only phone string, or "" if not found.
            seller_name      — seller / agency name string, or "" if not found.
    """
    soup = BeautifulSoup(html, "html.parser")

    phone = ""
    seller_name = ""

    # ── Try primary block: div.block-info ─────────────────────────────────
    block_info = soup.select_one("div.block-info")
    if block_info:
        # Seller name from <h3> inside block-info
        h3 = block_info.select_one("h3")
        if h3:
            seller_name = h3.get_text(strip=True)

        # Phone from <a href="tel:...">
        tel_link = block_info.select_one('a[href^="tel:"]')
        if tel_link:
            raw_phone = tel_link.get("href", "").replace("tel:", "")
            phone = normalise_phone(raw_phone)

        # Fallback: text inside .block-person-link
        if not phone:
            person_link = block_info.select_one(".block-person-link")
            if person_link:
                phone = normalise_phone(person_link.get_text(strip=True))

    # ── Try alternative block: div.block-agent ────────────────────────────
    if not phone or not seller_name:
        block_agent = soup.select_one("div.block-agent")
        if block_agent:
            if not seller_name:
                name_el = block_agent.select_one("h3, .block-agent-name, [class*='name']")
                if name_el:
                    seller_name = name_el.get_text(strip=True)

            if not phone:
                tel_link = block_agent.select_one('a[href^="tel:"]')
                if tel_link:
                    raw_phone = tel_link.get("href", "").replace("tel:", "")
                    phone = normalise_phone(raw_phone)

    # ── Final fallback: any tel: link anywhere on the page ────────────────
    if not phone:
        any_tel = soup.select_one('a[href^="tel:"]')
        if any_tel:
            raw_phone = any_tel.get("href", "").replace("tel:", "")
            phone = normalise_phone(raw_phone)

    # ── Fallback seller name: "Публикувано от:" pattern ───────────────────
    if not seller_name:
        # Look for text node pattern like "Публикувано от: Агенция XYZ"
        for el in soup.select(".published-by, .posted-by, [class*='author'], [class*='contact']"):
            text = el.get_text(strip=True)
            if text:
                seller_name = text
                break

    return phone, seller_name


# ---------------------------------------------------------------------------
# Pagination helper
# ---------------------------------------------------------------------------

def _has_next_page(soup: BeautifulSoup) -> bool:
    """
    Return True if the page contains a "next page" navigation link.

    imoti.bg uses standard Bootstrap pagination with an anchor whose text
    is "»" or ">" or "следваща" (next). Also checks href for page:N pattern.
    """
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
    """
    Iterate through all rental listing pages and collect matching apartments.

    Stops automatically when:
    • A page returns no apartment listings (end of results).
    • The configured max_pages limit is reached.
    • No "next page" link is found (after page 1).

    Args:
        session: Active requests.Session.
        config:  Config object.

    Returns:
        All matching Listing objects (phone/seller_name filled where visible
        on the list page; otherwise empty — those are populated in
        enrich_listings).
    """
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
            logger.info("No listings on page %d — reached the end of results.", page_num)
            break

        # Check for a "next page" link as a secondary stop condition.
        soup = BeautifulSoup(resp.text, "html.parser")
        if not _has_next_page(soup) and page_num > 1:
            logger.info("No 'next page' link on page %d — stopping.", page_num)
            break

    logger.info("Total apartment listings collected: %d", len(all_listings))
    return all_listings


def enrich_listing(
    listing: Listing,
    session: requests.Session,
    config: Config,
    agency_phones: set[str],
    agency_names: set[str],
) -> None:
    """
    Visit the detail page for *listing* (if needed) to fill in phone and
    seller_name, then classify the listing as private or agency.

    Classification logic (in order):
    1. If the listing's Phone is in the agency_phones set → "від агенції".
    2. If the listing's Seller_Name (lowercased) contains any substring
       from agency_names → "від агенції" (catches masking agencies).
    3. Otherwise → "приватний".

    The detail page is only fetched when phone or seller_name is still empty
    after list-page extraction — this minimises unnecessary requests.

    Args:
        listing:       The Listing to enrich (modified in-place).
        session:       Active requests.Session.
        config:        Config object.
        agency_phones: Set of normalised agency phone numbers from the sheet.
        agency_names:  Set of lowercased agency names from the sheet.
    """
    needs_detail = not listing.phone or not listing.seller_name

    if needs_detail:
        logger.debug(
            "Fetching detail page for ad %s (phone=%r, seller=%r)",
            listing.ad_id, listing.phone, listing.seller_name,
        )
        resp = _polite_get(session, listing.link, config)
        if resp is None:
            logger.warning("Could not fetch detail page for ad %s.", listing.ad_id)
            listing.ad_type = "невідомо"
            return

        detail_phone, detail_seller = parse_detail_page(resp.text)

        # Only override empty values — keep list-page data if we already have it.
        if not listing.phone and detail_phone:
            listing.phone = detail_phone
            logger.debug("  Ad %s — phone from detail: %s", listing.ad_id, listing.phone)

        if not listing.seller_name and detail_seller:
            listing.seller_name = detail_seller
            logger.debug("  Ad %s — seller from detail: %s", listing.ad_id, listing.seller_name)

    if not listing.phone and not listing.seller_name:
        logger.warning(
            "Ad %s: could not find phone or seller name on list page or detail page.",
            listing.ad_id,
        )

    # ── Classification ────────────────────────────────────────────────────
    listing.ad_type = _classify_listing(
        phone=listing.phone,
        seller_name=listing.seller_name,
        agency_phones=agency_phones,
        agency_names=agency_names,
    )

    logger.debug(
        "  Ad %s → phone=%s, seller=%r, type=%s",
        listing.ad_id, listing.phone, listing.seller_name, listing.ad_type,
    )


def _classify_listing(
    phone: str,
    seller_name: str,
    agency_phones: set[str],
    agency_names: set[str],
) -> str:
    """
    Determine whether a listing is from a private person or an agency.

    Priority:
    1. Phone match against known agency phones (most reliable — the phone is
       the unique identifier that an agency cannot easily hide).
    2. Seller name substring match against known agency names (catches cases
       where an agency lists itself as "Частно лице" but uses a known name
       somewhere in the description).
    3. Default → "приватний".

    Args:
        phone:         Normalised phone string (digits only).
        seller_name:   Seller / agency name string.
        agency_phones: Set of normalised agency phone numbers.
        agency_names:  Set of lowercased agency name substrings.

    Returns:
        "від агенції" or "приватний".
    """
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
    """
    Scrape the agencies directory (https://imoti.bg/агенции/page:N) and
    return a list of AgencyRecord objects.

    Pagination stops when:
    • A page returns no agencies.
    • No "next page" link is found (after page 1).
    • The max_agency_pages limit is reached.

    Args:
        session: Active requests.Session.
        config:  Config object.

    Returns:
        List of AgencyRecord objects with name, phones, email.
    """
    all_agencies: list[AgencyRecord] = []
    seen_names: set[str] = set()   # dedup by lowercased name

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
            page_num, len(page_agencies), len(new_on_page),
        )
        all_agencies.extend(new_on_page)

        if not page_agencies:
            logger.info("No agencies on page %d — stopping.", page_num)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        if not _has_next_page(soup) and page_num > 1:
            logger.info("No 'next page' link on agency page %d — stopping.", page_num)
            break

    logger.info("Total agencies collected: %d", len(all_agencies))
    return all_agencies


def _parse_agency_page(html: str) -> list[AgencyRecord]:
    """
    Parse one agencies listing page and return AgencyRecord objects.

    imoti.bg agencies page structure (current):

        <div class="agency_section">
          <div class="agency_list">
            <div class="agency_info">
              <a href="...">Agency Name</a>
              City (optional)
              Phone number (plain text or tel: link)
            </div>
          </div>
        </div>

    Args:
        html: Raw HTML string of one agencies page.

    Returns:
        List of AgencyRecord objects.
    """
    soup = BeautifulSoup(html, "html.parser")
    records: list[AgencyRecord] = []

    # Current imoti.bg structure: div.agency_info inside div.agency_list
    containers = soup.select("div.agency_info")

    # Fallback to older structure if needed
    if not containers:
        containers = soup.select(
            ".agency-item, "
            ".agency-list-item, "
            "article.agency, "
            ".company-item"
        )

    if not containers:
        # Last fallback: look for any block that contains an agency name
        containers = [
            el.parent for el in soup.select("h3.agency-name, a.agency-name, h2.agency-name")
            if el.parent
        ]

    for container in containers:
        try:
            record = _parse_agency_container_v2(container)
        except Exception as exc:   # noqa: BLE001
            logger.debug("Skipping malformed agency container: %s", exc)
            continue
        if record is not None:
            records.append(record)

    return records


def _parse_agency_container(container: BeautifulSoup) -> Optional[AgencyRecord]:
    """
    Extract agency name, phones, and email from a single agency container element.

    Returns None if no valid name is found.
    """
    # ── Agency name ───────────────────────────────────────────────────────
    agency_name = ""
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
                break

    if not agency_name:
        return None

    # ── Phone numbers ─────────────────────────────────────────────────────
    phones: list[str] = []

    # 1. Elements with phone-related class / selector
    for phone_sel in ("span.phone", ".phone", ".phones", "[class*='phone']", "[class*='tel']"):
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
    seen: set[str] = set()
    unique_phones = []
    for p in phones:
        if p not in seen:
            seen.add(p)
            unique_phones.append(p)

    # ── Email ─────────────────────────────────────────────────────────────
    email = ""
    mailto = container.select_one('a[href^="mailto:"]')
    if mailto:
        email = mailto.get("href", "").replace("mailto:", "").strip()
    else:
        # Regex fallback for plain text email
        email_re = re.compile(r"[\w.\-+]+@[\w.\-]+\.[a-z]{2,}", re.IGNORECASE)
        full_text = container.get_text(" ", strip=True)
        em_match = email_re.search(full_text)
        if em_match:
            email = em_match.group(0)

    # ── City ────────────────────────────────────────────────────────────────
    # Try to extract city from common location patterns after phone numbers.
    # Common Bulgarian cities: София, Варна, Пловдив, Бургас, Стара Загора, etc.
    city = ""
    city_patterns = (
        r"\b(София Варна Пловдив Бургас Стара Загора Плевен Велико Търново Благоевград "
        r"Русе Габрово Казанлък Асеновград Видин Враца Монтана Кюстендил Силистра "
        r"Добрич Търговище Шумен Перник Хасково Ямбол Сливен)\b"
    )
    city_re = re.compile(city_patterns)
    
    # First try: look for city in elements with location-related classes
    for city_sel in (".city", ".location", ".address", "[class*='city']", "[class*='location']"):
        city_el = container.select_one(city_sel)
        if city_el:
            city_text = city_el.get_text(strip=True)
            city_match = city_re.search(city_text)
            if city_match:
                city = city_match.group(1)
                break
    
    # Second try: scan full text for city names after phone numbers
    if not city:
        full_text = container.get_text(" ", strip=True)
        # Find city names in the text (looking for patterns like "...phone... CityName")
        city_match = city_re.search(full_text)
        if city_match:
            city = city_match.group(1)

    return AgencyRecord(
        agency_name=agency_name,
        phones=unique_phones,
        email=email,
        city=city,
    )


def _parse_agency_container_v2(container: BeautifulSoup) -> Optional[AgencyRecord]:
    """
    Extract agency name, phones, and email from the NEW imoti.bg agency container structure.
    
    Current structure (div.agency_info):
        <div class="agency_info">
            <a>Agency Name</a>  
            City (optional)
            Phone (plain text or tel: link)
            Email (optional)
        </div>
    
    Returns None if no valid name is found.
    """
    # ── Agency name ───────────────────────────────────────────────────────
    # Try to find the agency name - usually in an <a> tag
    agency_name = ""
    
    # First try: <a> tag (most common)
    a_tag = container.select_one("a")
    if a_tag:
        agency_name = a_tag.get_text(strip=True)
    
    # If no name found in <a>, try other elements
    if not agency_name:
        for name_sel in ("h3", "h2", "span.name", ".name"):
            el = container.select_one(name_sel)
            if el:
                agency_name = el.get_text(strip=True)
                break
    
    if not agency_name:
        return None
    
    # Clean up agency name - remove extra whitespace
    agency_name = " ".join(agency_name.split())
    
    # ── Phone numbers ─────────────────────────────────────────────────────
    phones: list[str] = []
    
    # 1. <a href="tel:..."> links (most reliable)
    for tel_link in container.select('a[href^="tel:"]'):
        raw = tel_link.get("href", "").replace("tel:", "")
        normalised = normalise_phone(raw)
        if normalised and len(normalised) >= 9:
            phones.append(normalised)
    
    # 2. Plain text phone numbers in the container
    # Look for patterns like 08xx xxx xxx or +359...
    if not phones:
        full_text = container.get_text(" ", strip=True)
        for match in _PHONE_TEXT_RE.finditer(full_text):
            normalised = normalise_phone(match.group(0))
            if normalised and len(normalised) >= 9:
                phones.append(normalised)
    
    # Deduplicate while preserving order.
    seen: set[str] = set()
    unique_phones = []
    for p in phones:
        if p not in seen:
            seen.add(p)
            unique_phones.append(p)
    
    # ── Email ─────────────────────────────────────────────────────────────
    email = ""
    
    # 1. <a href="mailto:..."> links
    mailto = container.select_one('a[href^="mailto:"]')
    if mailto:
        email = mailto.get("href", "").replace("mailto:", "").strip()
    else:
        # 2. Regex fallback for plain text email
        email_re = re.compile(r"[\w.\-+]+@[\w.\-]+\.[a-z]{2,}", re.IGNORECASE)
        full_text = container.get_text(" ", strip=True)
        em_match = email_re.search(full_text)
        if em_match:
            email = em_match.group(0)
    
    # ── City ────────────────────────────────────────────────────────────────
    # Try to extract city from the container text
    # Common pattern: agency name is first, then city, then phone
    city = ""
    city_patterns = (
        r"\b(София Варна Пловдив Бургас Стара Загора Плевен Велико Търново Благоевград "
        r"Русе Габрово Казанлък Асеновград Видин Враца Монтана Кюстендил Силистра "
        r"Добрич Търговище Шумен Перник Хасково Ямбол Сливен)\b"
    )
    city_re = re.compile(city_patterns)
    
    # Get full text and look for city names
    full_text = container.get_text(" ", strip=True)
    city_match = city_re.search(full_text)
    if city_match:
        city = city_match.group(1)
    
    return AgencyRecord(
        agency_name=agency_name,
        phones=unique_phones,
        email=email,
        city=city,
    )


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
    table.add_column("#",          style="dim",     width=4)
    table.add_column("ID",         style="cyan",    no_wrap=True)
    table.add_column("Назва",      style="white")
    table.add_column("Ціна",       style="green")
    table.add_column("Місто",      style="yellow")
    table.add_column("Площа",      style="blue")
    table.add_column("Телефон",    style="magenta")
    table.add_column("Продавець",  style="white")
    table.add_column("Тип",        style="red")

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
        )

    console.print(table)


def _print_agencies_summary(agencies: list[AgencyRecord]) -> None:
    """Render a Rich table of scraped agencies to stdout."""
    table = Table(
        title="Scraped Agencies",
        show_lines=True,
        style="bold",
    )
    table.add_column("#",       style="dim",   width=4)
    table.add_column("Name",    style="cyan")
    table.add_column("Phones",  style="magenta")
    table.add_column("Email",   style="green")
    table.add_column("City",    style="yellow")

    for idx, rec in enumerate(agencies, start=1):
        table.add_row(
            str(idx),
            rec.agency_name,
            ", ".join(rec.phones),
            rec.email,
            rec.city,
        )

    console.print(table)


def export_agencies_to_csv(
    agencies: list[AgencyRecord],
    csv_path: str | Path,
) -> None:
    """
    Export agency records to a CSV file using pandas.

    Args:
        agencies:    List of AgencyRecord objects to export.
        csv_path:   Path to the output CSV file.
    """
    if not agencies:
        logger.warning("No agencies to export to CSV.")
        return

    # Convert agency records to list of dicts
    data = []
    for rec in agencies:
        # For each agency, create a row with the first phone (if any)
        # and the city
        phone = rec.phones[0] if rec.phones else ""
        data.append({
            "Agency Name": rec.agency_name,
            "Phone Number": phone,
            "City": rec.city,
        })

    # Create DataFrame with the specified columns
    df = pd.DataFrame(data, columns=["Agency Name", "Phone Number", "City"])

    # Ensure the parent directory exists
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)

    # Write to CSV with UTF-8 encoding (Bulgarian characters)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    logger.info("Exported %d agencies to CSV: %s", len(agencies), csv_path)


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
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> int:
    """
    Orchestrate the full daily scraping pipeline.

    Returns:
        Exit code (0 = success, 1 = fatal error).
    """
    args = _parse_args()

    # ── Load configuration ────────────────────────────────────────────────
    config = load_config()
    config.force           = args.force
    config.dry_run         = args.dry_run
    config.update_agencies = args.update_agencies

    _setup_logging(config)

    today = date.today().isoformat()
    logger.info(
        "=== imoti.bg rental scraper — %s (force=%s, dry_run=%s, update_agencies=%s) ===",
        today, config.force, config.dry_run, config.update_agencies,
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

    # ═════════════════════════════════════════════════════════════════════
    # PATH A: --update-agencies
    # Scrape the agency directory and upsert into the Agencies sheet.
    # Can be combined with the normal ad-scraping path.
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
        else:
            logger.warning("No agency records were scraped from the agencies directory.")

        # If the user only wanted to update agencies (no rental scraping), stop here.
        # If they also want the normal scrape, fall through.
        logger.info("Agency update complete.")

    # ═════════════════════════════════════════════════════════════════════
    # PATH B: Normal daily scraping of rental listings
    # ═════════════════════════════════════════════════════════════════════

    # ── Load already-processed IDs and agency data ────────────────────────
    try:
        processed_ids: set[str] = sheets.load_processed_ids()
        agency_phones: set[str] = sheets.load_agency_phones()
        agency_names:  set[str] = sheets.load_agency_names()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to load data from Google Sheets: %s", exc)
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
    # For listings where phone or seller_name was not found on the list page,
    # visit the detail page.  Already-extracted values are kept.
    logger.info("Enriching %d new listing(s) …", len(new_listings))
    for i, listing in enumerate(new_listings, start=1):
        logger.info(
            "  [%d/%d] Ad %s — '%s'",
            i, len(new_listings), listing.ad_id, listing.title,
        )
        try:
            enrich_listing(
                listing=listing,
                session=session,
                config=config,
                agency_phones=agency_phones,
                agency_names=agency_names,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not enrich ad %s: %s", listing.ad_id, exc)
            listing.ad_type = "невідомо"

    # ── Display summary table ─────────────────────────────────────────────
    _print_summary(new_listings, today)

    # ── Write to Google Sheets ────────────────────────────────────────────
    new_rows = [lst.as_row(today) for lst in new_listings]
    new_ids  = [lst.ad_id for lst in new_listings]

    if not config.dry_run:
        try:
            sheets.append_new_ads(new_rows)
            sheets.mark_processed(new_ids)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to write to Google Sheets: %s", exc, exc_info=True)
            return 1
    else:
        logger.info(
            "[DRY-RUN] Would write %d new ad row(s) and %d processed ID(s).",
            len(new_rows), len(new_ids),
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
