"""
config.py — Configuration loader for the imoti.bg rental scraper.

Reads all settings from a .env file (or environment variables) using python-dotenv.
All configuration is centralised here so that scraper.py stays clean.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env from the same directory as this file (or a parent), then fall
# back to real environment variables if any key is missing.
# ---------------------------------------------------------------------------
_ENV_FILE = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_ENV_FILE, override=False)


def _require(key: str) -> str:
    """Return the value of an env-var or abort with a clear message."""
    value = os.getenv(key, "").strip()
    if not value:
        print(
            f"[ERROR] Required environment variable '{key}' is not set.\n"
            f"        Please copy .env.example → .env and fill in all values.",
            file=sys.stderr,
        )
        sys.exit(1)
    return value


def _optional(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


# ---------------------------------------------------------------------------
# Dataclass that holds every setting used by the scraper.
# ---------------------------------------------------------------------------

@dataclass
class Config:
    # ── Google Sheets ──────────────────────────────────────────────────────
    google_sheet_id: str
    """The long ID from the Google Sheets URL (not the human-readable name)."""

    service_account_json: Path
    """Absolute (or relative) path to the service-account JSON key file."""

    sheet_name: str = "Imoti_BG_Rentals"
    """Name of the Google Spreadsheet (must match the actual file name)."""

    # ── Email / SMTP ───────────────────────────────────────────────────────
    email_from: str = ""
    email_to: str = ""
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""

    # ── Scraper behaviour ──────────────────────────────────────────────────
    base_url: str = "https://imoti.bg/наеми/page:{page}"
    """Pagination URL template.  {page} is replaced with the page number."""

    agencies_url: str = "https://imoti.bg/агенции/page:{page}"
    """Pagination URL template for the agencies listing pages."""

    max_pages: int = 30
    """Safety cap on the number of listing pages to scrape (real site is ~26)."""

    max_agency_pages: int = 15
    """Safety cap on the number of agency pages to scrape (real site is ~13)."""

    request_delay_min: float = 2.0
    """Minimum seconds to wait between HTTP requests (polite scraping)."""

    request_delay_max: float = 5.0
    """Maximum seconds to wait between HTTP requests."""

    # ── User-Agent pool ─────────────────────────────────────────────────────
    # A list of realistic browser UA strings.  One is chosen at random per run.
    user_agents: list[str] = field(
        default_factory=lambda: [
            # Chrome 124 on Windows 10
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            # Firefox 125 on Windows 10
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
                "Gecko/20100101 Firefox/125.0"
            ),
            # Chrome 124 on macOS Sonoma
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.6367.82 Safari/537.36"
            ),
            # Safari 17 on macOS
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/17.4.1 Safari/605.1.15"
            ),
            # Edge 124 on Windows 11
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
            ),
        ]
    )

    # ── Optional city filter ───────────────────────────────────────────────
    city_filter: Optional[str] = None
    """If set (e.g. 'София'), only keep listings whose location contains this
    string (case-insensitive).  Leave blank / unset to collect all cities."""

    # ── Logging ───────────────────────────────────────────────────────────
    log_file: Optional[Path] = None
    """Optional path to write log output.  stdout is always used."""

    log_level: str = "INFO"

    # ── CSV Export for Agencies ─────────────────────────────────────────────
    agencies_csv_path: Optional[Path] = None
    """Optional path to export agencies to CSV file (e.g., 'agencies.csv')."""

    # ── Runtime flags (set by CLI args) ──────────────────────────────────
    force: bool = False
    """If True, re-process all ads even if they are already in Processed_IDs."""

    dry_run: bool = False
    """If True, do not write to Google Sheets or send email."""

    update_agencies: bool = False
    """If True, scrape the agencies pages and update the Agencies sheet."""

    # ── Internal worksheet names ──────────────────────────────────────────
    ws_new_ads: str = "New_Ads"
    ws_agencies: str = "Agencies"
    ws_processed: str = "Processed_IDs"
    ws_renters: str = "Renters"

    # ── Column headers ─────────────────────────────────────────────────────
    new_ads_headers: list[str] = field(
        default_factory=lambda: [
            "Date",        # YYYY-MM-DD (today's date)
            "Ad_ID",       # Unique numeric ID from the URL
            "Title",       # Listing title (e.g. "Двустаен апартамент")
            "Price",       # Price with currency (e.g. "700 EUR/месец")
            "Location",    # City / neighbourhood
            "Size",        # Floor area in sq.m.
            "Link",        # Full URL to detail page
            "Phone",       # Normalised phone (digits only)
            "Seller_Name", # e.g. "Частно лице" or "Агенция XYZ"
            "Type",        # "приватний" | "від агенції"
        ]
    )

    agencies_headers: list[str] = field(
        default_factory=lambda: [
            "Agency_Name",  # Human-readable agency name
            "Phones",       # Comma-separated list of normalised phone numbers
            "Email",        # Optional contact email
            "City",         # Optional city (e.g., Варна, Пловдив)
        ]
    )

    renters_headers: list[str] = field(
        default_factory=lambda: [
            "Name",           # Renter's full name
            "Phone",          # Renter's contact phone
            "Email",          # Renter's email
            "City",           # Desired city
            "Apartment_Type", # e.g. "двустаен", "тристаен"
            "Max_Price",      # Maximum monthly rent
        ]
    )


def load_config() -> Config:
    """
    Build and return a Config object populated from environment variables.

    Required variables (script will exit if missing):
        GOOGLE_SHEET_ID        — the Google Spreadsheet ID
        SERVICE_ACCOUNT_JSON   — path to the service-account credentials JSON

    Optional variables (all have sensible defaults):
        SHEET_NAME             — default: "Imoti_BG_Rentals"
        EMAIL_FROM             — sender e-mail address
        EMAIL_TO               — recipient e-mail address (comma-separated ok)
        SMTP_SERVER            — default: smtp.gmail.com
        SMTP_PORT              — default: 587
        SMTP_USER              — SMTP login username
        SMTP_PASSWORD          — SMTP login password / App Password
        MAX_PAGES              — default: 30  (listing pages)
        MAX_AGENCY_PAGES       — default: 15  (agency pages)
        REQUEST_DELAY_MIN      — default: 2.0
        REQUEST_DELAY_MAX      — default: 5.0
        CITY_FILTER            — e.g. "София"  (blank = all cities)
        LOG_FILE               — path to a log file (optional)
        LOG_LEVEL              — default: INFO
        AGENCIES_CSV_PATH      — path to export agencies CSV (optional)
    """
    cfg = Config(
        google_sheet_id=_require("GOOGLE_SHEET_ID"),
        service_account_json=Path(_require("SERVICE_ACCOUNT_JSON")),
        sheet_name=_optional("SHEET_NAME", "Imoti_BG_Rentals"),
        email_from=_optional("EMAIL_FROM"),
        email_to=_optional("EMAIL_TO"),
        smtp_server=_optional("SMTP_SERVER", "smtp.gmail.com"),
        smtp_port=int(_optional("SMTP_PORT", "587")),
        smtp_user=_optional("SMTP_USER"),
        smtp_password=_optional("SMTP_PASSWORD"),
        max_pages=int(_optional("MAX_PAGES", "30")),
        max_agency_pages=int(_optional("MAX_AGENCY_PAGES", "15")),
        request_delay_min=float(_optional("REQUEST_DELAY_MIN", "2.0")),
        request_delay_max=float(_optional("REQUEST_DELAY_MAX", "5.0")),
        city_filter=_optional("CITY_FILTER") or None,
        log_file=Path(_optional("LOG_FILE")) if _optional("LOG_FILE") else None,
        log_level=_optional("LOG_LEVEL", "INFO").upper(),
        agencies_csv_path=Path(_optional("AGENCIES_CSV_PATH")) if _optional("AGENCIES_CSV_PATH") else None,
    )
    return cfg