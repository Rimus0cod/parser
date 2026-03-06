#!/usr/bin/env python3
"""
scraper.py — Daily apartment-rental scraper for imoti.bg
=========================================================

Run directly:
    python scraper.py                # normal daily run
    python scraper.py --force        # re-check ALL ads, ignore Processed_IDs
    python scraper.py --dry-run      # scrape but don't write to Sheets or send email
    python scraper.py --force --dry-run

The script:
1.  Paginates through https://imoti.bg/наеми/page:N and collects listing cards.
2.  Filters for apartments ("апартамент" in title or URL slug).
3.  Skips Ad IDs already in the "Processed_IDs" Google Sheet.
4.  For each new ad, visits the detail page to extract the phone number and
    determine whether it is a private person or agency listing.
5.  Appends new rows to "New_Ads" and records Ad IDs in "Processed_IDs".
6.  Sends an HTML summary email if any new ads were found.

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
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

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
# Data model
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
    # Populated after visiting the detail page:
    phone: str = ""
    ad_type: str = ""  # "приватний" | "від агенції"
    extra: dict = field(default_factory=dict)

    def as_row(self, today: str) -> list[str]:
        """Return a flat list matching Config.new_ads_headers column order."""
        return [
            today,
            self.ad_id,
            self.title,
            self.price,
            self.location,
            self.size,
            self.link,
            self.phone,
            self.ad_type,
        ]

    def as_dict(self) -> dict:
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
            "Type": self.ad_type,
        }


# ---------------------------------------------------------------------------
# HTTP session factory
# ---------------------------------------------------------------------------

def _make_session(config: Config) -> requests.Session:
    """Create a requests.Session with realistic headers."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": config.user_agent,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "bg-BG,bg;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
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

    Args:
        session:  Active requests.Session.
        url:      URL to fetch.
        config:   Config object (for delay settings).
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

    logger.error("Giving up on %s after %d attempts.", url, retries)
    return None


# ---------------------------------------------------------------------------
# Listing-page parser
# ---------------------------------------------------------------------------

_AD_ID_RE = re.compile(r"-(\d{4,10})\.htm$", re.IGNORECASE)


def _extract_ad_id(url: str) -> str:
    """Extract the numeric Ad ID from a detail-page URL.

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


def parse_listing_page(
    html: str,
    base_url: str,
    city_filter: Optional[str] = None,
) -> list[Listing]:
    """
    Parse one results page and return a list of Listing objects.

    Only apartment-type listings are returned.  If *city_filter* is set, only
    listings whose location contains that string (case-insensitive) are kept.

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

    return Listing(
        ad_id=ad_id,
        title=title,
        price=price,
        location=location,
        size=size,
        link=link,
    )


# ---------------------------------------------------------------------------
# Detail-page parser
# ---------------------------------------------------------------------------

def parse_detail_page(html: str) -> tuple[str, bool]:
    """
    Parse the detail page of a listing to extract the phone number and
    determine whether the poster is a private person or an agency.

    The relevant HTML section looks like:

        <div class="block-info">
          <h3>Частно лице</h3>      ← or the agency name
          <ul class="block-person-list">
            <li>
              <div class="block-person-link">
                <a href="tel:0894860795">0894860795</a>
              </div>
            </li>
          </ul>
        </div>

    Args:
        html: Raw HTML of the detail page.

    Returns:
        Tuple (normalised_phone, is_agency):
            normalised_phone — digits-only phone string, or "" if not found.
            is_agency        — True if the listing is from an agency.
    """
    soup = BeautifulSoup(html, "html.parser")

    block_info = soup.select_one("div.block-info")
    if not block_info:
        return "", False

    # ── Determine poster type ─────────────────────────────────────────────
    h3 = block_info.select_one("h3")
    poster_text = h3.get_text(strip=True).lower() if h3 else ""
    # "Частно лице" → private; anything else is assumed to be an agency name.
    is_private = "частно лице" in poster_text
    is_agency = not is_private

    # ── Extract phone number ──────────────────────────────────────────────
    phone = ""
    # Primary source: <a href="tel:…">
    tel_link = block_info.select_one('a[href^="tel:"]')
    if tel_link:
        raw_phone = tel_link.get("href", "").replace("tel:", "")
        phone = normalise_phone(raw_phone)

    # Fallback: text inside .block-person-link
    if not phone:
        person_link = block_info.select_one(".block-person-link")
        if person_link:
            phone = normalise_phone(person_link.get_text(strip=True))

    return phone, is_agency


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

def _has_next_page(soup: BeautifulSoup) -> bool:
    """
    Return True if the page contains a "next page" navigation link.

    imoti.bg uses standard Bootstrap pagination with an anchor whose text
    is "»" or "следваща" (next).
    """
    # Look for <a> tags in pagination elements.
    for a in soup.select("ul.pagination a, .pagination a, a.next, li.next a"):
        text = a.get_text(strip=True).lower()
        href = a.get("href", "")
        if text in ("»", "следваща", ">", "next") or "page:" in href:
            # Specifically check it's a next-page link (not current or previous).
            if "»" in a.get_text() or "следваща" in text:
                return True
    return False


# ---------------------------------------------------------------------------
# Core scraping logic
# ---------------------------------------------------------------------------

def scrape_all_pages(
    session: requests.Session,
    config: Config,
) -> list[Listing]:
    """
    Iterate through all listing pages and collect matching apartments.

    Stops automatically when:
    • A page returns no apartment listings (end of results).
    • The configured max_pages limit is reached.

    Args:
        session: Active requests.Session.
        config:  Config object.

    Returns:
        All matching Listing objects (without phone / type — those are
        populated later in enrich_listings).
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

        # Optional: check for a "next page" link as a secondary stop condition.
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
) -> None:
    """
    Visit the detail page for *listing* and fill in phone + type fields in-place.

    Args:
        listing:       The Listing to enrich (modified in-place).
        session:       Active requests.Session.
        config:        Config object.
        agency_phones: Set of normalised agency phone numbers from the sheet.
    """
    logger.debug("Enriching ad %s — %s", listing.ad_id, listing.link)
    resp = _polite_get(session, listing.link, config)
    if resp is None:
        logger.warning("Could not fetch detail page for ad %s.", listing.ad_id)
        listing.phone = ""
        listing.ad_type = "невідомо"
        return

    phone, is_agency_from_page = parse_detail_page(resp.text)
    listing.phone = phone

    # Classification logic:
    # 1. If the detail page explicitly says "Частно лице" → private, unless the
    #    phone is in the Agencies sheet (user-maintained override).
    # 2. If the page shows an agency name → agency.
    if phone and phone in agency_phones:
        listing.ad_type = "від агенції"
    elif is_agency_from_page:
        listing.ad_type = "від агенції"
    else:
        listing.ad_type = "приватний"

    logger.debug(
        "  Ad %s → phone=%s, type=%s", listing.ad_id, listing.phone, listing.ad_type
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
    table.add_column("#", style="dim", width=4)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Назва", style="white")
    table.add_column("Ціна", style="green")
    table.add_column("Місто", style="yellow")
    table.add_column("Площа", style="blue")
    table.add_column("Телефон", style="magenta")
    table.add_column("Тип", style="red")

    for idx, lst in enumerate(listings, start=1):
        type_style = "[green]приватний[/green]" if "приватний" in lst.ad_type else "[orange1]від агенції[/orange1]"
        table.add_row(
            str(idx),
            lst.ad_id,
            lst.title,
            lst.price,
            lst.location,
            lst.size,
            lst.phone,
            type_style,
        )

    console.print(table)


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily apartment-rental scraper for imoti.bg",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py                # normal daily run
  python scraper.py --force        # re-check all ads (ignore Processed_IDs)
  python scraper.py --dry-run      # scrape but don't write to Sheets or send email
  python scraper.py --force --dry-run
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
    config.force = args.force
    config.dry_run = args.dry_run

    _setup_logging(config)

    today = date.today().isoformat()
    logger.info(
        "=== imoti.bg rental scraper — %s (force=%s, dry_run=%s) ===",
        today,
        config.force,
        config.dry_run,
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

    # ── Load already-processed IDs and agency phones ──────────────────────
    try:
        processed_ids: set[str] = sheets.load_processed_ids()
        agency_phones: set[str] = sheets.load_agency_phones()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to load data from Google Sheets: %s", exc)
        return 1

    if config.force:
        logger.info("--force flag set: ignoring %d processed IDs.", len(processed_ids))
        processed_ids = set()

    # ── Scrape listing pages ──────────────────────────────────────────────
    session = _make_session(config)
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

    # ── Enrich new listings (visit detail pages) ──────────────────────────
    logger.info("Visiting detail pages for %d new listing(s) …", len(new_listings))
    for i, listing in enumerate(new_listings, start=1):
        logger.info(
            "  [%d/%d] Enriching ad %s — '%s'",
            i,
            len(new_listings),
            listing.ad_id,
            listing.title,
        )
        try:
            enrich_listing(listing, session, config, agency_phones)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not enrich ad %s: %s", listing.ad_id, exc)
            listing.phone = ""
            listing.ad_type = "невідомо"

    # ── Display summary table ─────────────────────────────────────────────
    _print_summary(new_listings, today)

    # ── Write to Google Sheets ────────────────────────────────────────────
    new_rows = [lst.as_row(today) for lst in new_listings]
    new_ids = [lst.ad_id for lst in new_listings]

    try:
        sheets.append_new_ads(new_rows)
        sheets.mark_processed(new_ids)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to write to Google Sheets: %s", exc, exc_info=True)
        return 1

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