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

    max_pages: int = 30
    """Safety cap on the number of pages to scrape (real site is ~26)."""

    request_delay_min: float = 2.0
    """Minimum seconds to wait between HTTP requests (polite scraping)."""

    request_delay_max: float = 5.0
    """Maximum seconds to wait between HTTP requests."""

    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    # ── Optional city filter ───────────────────────────────────────────────
    city_filter: Optional[str] = None
    """If set (e.g. 'София'), only keep listings whose location contains this
    string (case-insensitive).  Leave blank / unset to collect all cities."""

    # ── Logging ───────────────────────────────────────────────────────────
    log_file: Optional[Path] = None
    """Optional path to write log output.  stdout is always used."""

    log_level: str = "INFO"

    # ── Runtime flags (set by CLI args) ──────────────────────────────────
    force: bool = False
    """If True, re-process all ads even if they are already in Processed_IDs."""

    dry_run: bool = False
    """If True, do not write to Google Sheets or send email."""

    # ── Internal worksheet names ──────────────────────────────────────────
    ws_new_ads: str = "New_Ads"
    ws_agencies: str = "Agencies"
    ws_processed: str = "Processed_IDs"
    ws_renters: str = "Renters"

    # ── Column headers ─────────────────────────────────────────────────────
    new_ads_headers: list[str] = field(
        default_factory=lambda: [
            "Date",
            "Ad_ID",
            "Title",
            "Price",
            "Location",
            "Size",
            "Link",
            "Phone",
            "Type",
        ]
    )

    agencies_headers: list[str] = field(
        default_factory=lambda: [
            "Agency_Name",
            "Phones",   # comma-separated list of normalised phone numbers
            "Email",
        ]
    )
    """
    Agencies worksheet column headers.

    Column layout:
        Agency_Name  — human-readable name of the agency (e.g. "Агенция XYZ")
        Phones       — comma-separated normalised phones (e.g. "0894860795,070011777")
        Email        — optional contact email (e.g. "info@agency.com")

    Users fill this sheet manually.  The scraper reads all Phones values,
    splits by comma, normalises each, and builds a lookup set.
    """

    renters_headers: list[str] = field(
        default_factory=lambda: [
            "Name",
            "Phone",
            "Email",
            "City",
            "Apartment_Type",
            "Max_Price",
        ]
    )
    """
    Renters worksheet column headers.

    This sheet is for manual user entry only — the scraper creates it on first
    run but never reads from or writes to it during normal operation.

    Column layout:
        Name            — full name of the potential renter (e.g. "Іван Петров")
        Phone           — normalised phone, single or comma-separated multiples
        Email           — contact email (e.g. "ivan@example.com")
        City            — desired city (e.g. "София")
        Apartment_Type  — desired type(s), comma-separated (e.g. "1-room,2-room")
        Max_Price       — maximum budget (e.g. "700 EUR")
    """


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
        MAX_PAGES              — default: 30
        REQUEST_DELAY_MIN      — default: 2.0
        REQUEST_DELAY_MAX      — default: 5.0
        CITY_FILTER            — e.g. "София"  (blank = all cities)
        LOG_FILE               — path to a log file (optional)
        LOG_LEVEL              — default: INFO
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
        request_delay_min=float(_optional("REQUEST_DELAY_MIN", "2.0")),
        request_delay_max=float(_optional("REQUEST_DELAY_MAX", "5.0")),
        city_filter=_optional("CITY_FILTER") or None,
        log_file=Path(_optional("LOG_FILE")) if _optional("LOG_FILE") else None,
        log_level=_optional("LOG_LEVEL", "INFO").upper(),
    )
    return cfg
