"""
sheets.py — Google Sheets integration for the imoti.bg rental scraper.

Uses gspread + google-auth (service-account credentials).

Responsibilities
────────────────
• Open (or create) the target spreadsheet and its three worksheets.
• Read the set of already-processed Ad IDs from "Processed_IDs".
• Read the set of known-agency phone numbers from "Agencies".
• Append new ad rows to "New_Ads".
• Append new Ad IDs to "Processed_IDs".
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from config import Config

logger = logging.getLogger(__name__)

# OAuth2 scopes required for full read/write access to Sheets (and Drive so we
# can open the file by ID even if the service account doesn't own it).
_SCOPES: list[str] = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


# ---------------------------------------------------------------------------
# Helper: ensure a worksheet exists, create it if not
# ---------------------------------------------------------------------------

def _ensure_worksheet(
    spreadsheet: gspread.Spreadsheet,
    title: str,
    header_row: list[str] | None = None,
) -> gspread.Worksheet:
    """
    Return the worksheet with *title*, creating it (and writing headers) if it
    doesn't exist yet.

    Args:
        spreadsheet:  An open gspread.Spreadsheet object.
        title:        Worksheet tab name.
        header_row:   If provided and the sheet is newly created, this row is
                      written as the first row.

    Returns:
        The gspread.Worksheet object.
    """
    try:
        ws = spreadsheet.worksheet(title)
        logger.debug("Opened existing worksheet '%s'.", title)
        return ws
    except gspread.WorksheetNotFound:
        logger.info("Worksheet '%s' not found — creating it.", title)
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=20)
        if header_row:
            ws.append_row(header_row, value_input_option="USER_ENTERED")
        logger.debug("Created worksheet '%s' with headers: %s", title, header_row)
        return ws


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class SheetsClient:
    """
    Wrapper around a gspread connection that exposes only the operations the
    scraper needs.

    Usage
    ─────
    client = SheetsClient(config)
    processed_ids = client.load_processed_ids()
    agency_phones = client.load_agency_phones()
    client.append_new_ads(rows)
    client.mark_processed(ad_ids)
    """

    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._spreadsheet: gspread.Spreadsheet | None = None
        self._ws_new_ads: gspread.Worksheet | None = None
        self._ws_agencies: gspread.Worksheet | None = None
        self._ws_processed: gspread.Worksheet | None = None

    # ── Connection ──────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Authenticate with the Google API and open all three worksheets,
        creating them if necessary.

        Raises:
            FileNotFoundError: if the service-account JSON file does not exist.
            gspread.exceptions.SpreadsheetNotFound: if the sheet ID is wrong
                or the service account hasn't been granted access.
        """
        json_path: Path = self._cfg.service_account_json
        if not json_path.exists():
            raise FileNotFoundError(
                f"Service-account JSON not found: {json_path}\n"
                "Download it from Google Cloud Console and set SERVICE_ACCOUNT_JSON "
                "in your .env file."
            )

        logger.info("Authenticating with Google Sheets API …")
        creds = Credentials.from_service_account_file(str(json_path), scopes=_SCOPES)
        gc = gspread.authorize(creds)

        logger.info("Opening spreadsheet ID '%s' …", self._cfg.google_sheet_id)
        self._spreadsheet = gc.open_by_key(self._cfg.google_sheet_id)

        # Ensure all three worksheets exist.
        self._ws_new_ads = _ensure_worksheet(
            self._spreadsheet,
            self._cfg.ws_new_ads,
            header_row=self._cfg.new_ads_headers,
        )
        self._ws_agencies = _ensure_worksheet(
            self._spreadsheet,
            self._cfg.ws_agencies,
            header_row=["Phone"],
        )
        self._ws_processed = _ensure_worksheet(
            self._spreadsheet,
            self._cfg.ws_processed,
            header_row=["Ad_ID"],
        )
        logger.info("Google Sheets connection established.")

    # ── Read helpers ────────────────────────────────────────────────────────

    def load_processed_ids(self) -> set[str]:
        """
        Return the set of Ad_IDs already stored in the "Processed_IDs" sheet.

        The first row is treated as a header and skipped.
        """
        self._require_connection()
        assert self._ws_processed is not None

        all_values: list[list[str]] = self._ws_processed.get_all_values()
        # Skip header row; strip whitespace; ignore blank cells.
        ids = {row[0].strip() for row in all_values[1:] if row and row[0].strip()}
        logger.info("Loaded %d processed Ad IDs from sheet.", len(ids))
        return ids

    def load_agency_phones(self) -> set[str]:
        """
        Return the set of normalised phone numbers stored in "Agencies".

        Phones are normalised (digits only) so they can be compared reliably.
        The first row is treated as a header and skipped.
        """
        self._require_connection()
        assert self._ws_agencies is not None

        all_values: list[list[str]] = self._ws_agencies.get_all_values()
        phones: set[str] = set()
        for row in all_values[1:]:
            if row and row[0].strip():
                normalised = _normalise_phone(row[0])
                if normalised:
                    phones.add(normalised)
        logger.info("Loaded %d agency phone numbers from sheet.", len(phones))
        return phones

    # ── Write helpers ───────────────────────────────────────────────────────

    def append_new_ads(self, rows: list[list[Any]]) -> None:
        """
        Append *rows* to the "New_Ads" worksheet.

        Each element in *rows* must have values in the same column order as
        Config.new_ads_headers:
            [Date, Ad_ID, Title, Price, Location, Size, Link, Phone, Type]

        Args:
            rows: List of row-lists to append.
        """
        if not rows:
            logger.debug("append_new_ads called with empty list — nothing to do.")
            return

        self._require_connection()
        assert self._ws_new_ads is not None

        if self._cfg.dry_run:
            logger.info("[DRY-RUN] Would append %d row(s) to '%s'.", len(rows), self._cfg.ws_new_ads)
            for row in rows:
                logger.debug("  DRY-RUN row: %s", row)
            return

        logger.info("Appending %d new ad row(s) to '%s' …", len(rows), self._cfg.ws_new_ads)
        # Append one-by-one to respect API quotas and give better error context.
        for row in rows:
            self._ws_new_ads.append_row(row, value_input_option="USER_ENTERED")
        logger.info("Done appending rows.")

    def mark_processed(self, ad_ids: list[str]) -> None:
        """
        Append *ad_ids* to the "Processed_IDs" worksheet.

        Args:
            ad_ids: List of Ad ID strings to record.
        """
        if not ad_ids:
            return

        self._require_connection()
        assert self._ws_processed is not None

        if self._cfg.dry_run:
            logger.info("[DRY-RUN] Would mark %d ID(s) as processed.", len(ad_ids))
            return

        logger.info("Marking %d Ad ID(s) as processed …", len(ad_ids))
        for ad_id in ad_ids:
            self._ws_processed.append_row([ad_id], value_input_option="USER_ENTERED")

    # ── Private ─────────────────────────────────────────────────────────────

    def _require_connection(self) -> None:
        if self._spreadsheet is None:
            raise RuntimeError(
                "SheetsClient.connect() must be called before any read/write operation."
            )


# ---------------------------------------------------------------------------
# Standalone utility — also used in scraper.py
# ---------------------------------------------------------------------------

def _normalise_phone(raw: str) -> str:
    """
    Strip all non-digit characters and return the cleaned phone string.

    Examples:
        "+359 89 486 0795"  →  "35989486­0795"  (international)
        "0894-860-795"      →  "0894860795"
        "0894860795"        →  "0894860795"
    """
    return "".join(ch for ch in raw if ch.isdigit())


# Make normalise_phone importable by scraper.py without a circular import.
normalise_phone = _normalise_phone
