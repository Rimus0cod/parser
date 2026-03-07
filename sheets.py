"""
sheets.py — Google Sheets integration for the imoti.bg rental scraper.

Uses gspread + google-auth (service-account credentials).

Responsibilities
────────────────
• Open (or create) the target spreadsheet and its four worksheets.
• Read the set of already-processed Ad IDs from "Processed_IDs".
• Read agency data (phone sets + name sets) from "Agencies".
• Append new ad rows to "New_Ads".
• Append new Ad IDs to "Processed_IDs".
• Upsert agency rows (merge phones, update email) in "Agencies".
• Create the "Renters" sheet if missing (manual-only — script never writes to it).

Agencies sheet layout (multi-column, enhanced):
    Agency_Name  |  Phones                       |  Email
    Агенция XYZ  |  0894860795,070011777          |  info@agency.com
    ...

Phones are stored as a comma-separated list (normalised, digits only).
When updating, new phones are merged (unique union) and Email is updated
only if the existing value is empty or if a new one is found.
"""

from __future__ import annotations

import logging
import time
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
# Helper: normalise a phone number to digits-only string
# ---------------------------------------------------------------------------

def normalise_phone(raw: str) -> str:
    """
    Normalise a Bulgarian phone number to a consistent format.
    
    Handles:
    - +359 format (international)
    - 00 359 format
    - 0 prefix (Bulgarian)
    - Spaces, dashes, parentheses removal
    
    Examples:
        "+359 89 486 0795"  → "0894860795"
        "00359894860795"   → "0894860795"
        "0894-860-795"     → "0894860795"
        "0888492790"       → "0888492790"
        "359896380248"     → "0896380248"
    """
    # First, extract only digits
    digits = "".join(ch for ch in raw if ch.isdigit())
    
    if not digits:
        return ""
    
    # Handle Bulgarian phone formats
    # If starts with 00359 or +359, remove the country code
    if digits.startswith("00359"):
        digits = digits[5:]  # Remove 00359
    elif digits.startswith("359") and len(digits) > 9:
        digits = digits[3:]  # Remove 359, keep 89...
    elif digits.startswith("+359") and len(digits) > 10:
        digits = digits[4:]  # Remove +359, keep 89...
    
    # If it starts with 0, it's already in correct format
    # If it doesn't start with 0 but has 9 digits, it might be missing the 0
    if not digits.startswith("0") and len(digits) == 9:
        # Check if it looks like a mobile (starts with 8 or 9)
        if digits[0] in ("8", "9"):
            digits = "0" + digits
    
    return digits


# Make importable as `from sheets import normalise_phone`
# (used in scraper.py).


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
    client.connect()
    processed_ids  = client.load_processed_ids()
    agency_phones  = client.load_agency_phones()
    agency_names   = client.load_agency_names()
    client.append_new_ads(rows)
    client.mark_processed(ad_ids)
    client.upsert_agencies(agency_rows)
    """

    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._spreadsheet: gspread.Spreadsheet | None = None
        self._ws_new_ads: gspread.Worksheet | None = None
        self._ws_agencies: gspread.Worksheet | None = None
        self._ws_processed: gspread.Worksheet | None = None
        self._ws_renters: gspread.Worksheet | None = None

    # ── Connection ──────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Authenticate with the Google API and open all four worksheets,
        creating them if necessary.

        Worksheet layout created on first run:
            New_Ads       — Date, Ad_ID, Title, Price, Location, Size, Link,
                            Phone, Seller_Name, Type
            Agencies      — Agency_Name, Phones, Email
            Processed_IDs — Ad_ID
            Renters       — Name, Phone, Email, City, Apartment_Type, Max_Price
                            (manual-only — script never writes here)

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

        # ── Ensure all four worksheets exist ──────────────────────────────
        self._ws_new_ads = _ensure_worksheet(
            self._spreadsheet,
            self._cfg.ws_new_ads,
            header_row=self._cfg.new_ads_headers,
        )
        self._ws_agencies = _ensure_worksheet(
            self._spreadsheet,
            self._cfg.ws_agencies,
            header_row=self._cfg.agencies_headers,
        )
        self._ws_processed = _ensure_worksheet(
            self._spreadsheet,
            self._cfg.ws_processed,
            header_row=["Ad_ID"],
        )
        # Renters: created if missing but NEVER written to by this script.
        self._ws_renters = _ensure_worksheet(
            self._spreadsheet,
            self._cfg.ws_renters,
            header_row=self._cfg.renters_headers,
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

        The Agencies sheet now uses multi-column layout:
            Agency_Name | Phones (comma-separated) | Email

        Each cell in the "Phones" column may contain multiple phones separated
        by commas.  All are normalised (digits only) and returned as a flat set.

        The first row is treated as a header and skipped.
        """
        self._require_connection()
        assert self._ws_agencies is not None

        all_values: list[list[str]] = self._ws_agencies.get_all_values()
        phones: set[str] = set()

        for row in all_values[1:]:
            if len(row) < 2:
                continue
            # Column index 1 = "Phones" (comma-separated)
            raw_phones_cell = row[1].strip()
            if not raw_phones_cell:
                continue
            for raw_phone in raw_phones_cell.split(","):
                normalised = normalise_phone(raw_phone.strip())
                if normalised:
                    phones.add(normalised)

        logger.info("Loaded %d agency phone numbers from sheet.", len(phones))
        return phones

    def load_agency_names(self) -> set[str]:
        """
        Return the set of agency names (lowercased) from the "Agencies" sheet.

        Used for substring-matching against Seller_Name to detect agencies
        that list themselves as private individuals.

        The first row is treated as a header and skipped.
        """
        self._require_connection()
        assert self._ws_agencies is not None

        all_values: list[list[str]] = self._ws_agencies.get_all_values()
        names: set[str] = set()

        for row in all_values[1:]:
            if row and row[0].strip():
                names.add(row[0].strip().lower())

        logger.info("Loaded %d agency names from sheet.", len(names))
        return names

    def load_agencies_full(self) -> list[dict[str, str]]:
        """
        Return all rows from the Agencies sheet as a list of dicts.

        Each dict has keys: 'Agency_Name', 'Phones', 'Email'.
        Used internally when upserting new agency data.
        """
        self._require_connection()
        assert self._ws_agencies is not None

        all_values: list[list[str]] = self._ws_agencies.get_all_values()
        result: list[dict[str, str]] = []

        for row in all_values[1:]:
            # Pad the row to at least 3 columns.
            while len(row) < 3:
                row.append("")
            result.append({
                "Agency_Name": row[0].strip(),
                "Phones":      row[1].strip(),
                "Email":       row[2].strip(),
            })

        return result

    # ── Write helpers ───────────────────────────────────────────────────────

    def append_new_ads(self, rows: list[list[Any]]) -> None:
        """
        Append *rows* to the "New_Ads" worksheet.

        Each element in *rows* must have values in the same column order as
        Config.new_ads_headers:
            [Date, Ad_ID, Title, Price, Location, Size, Link, Phone, Seller_Name, Type]

        Args:
            rows: List of row-lists to append.
        """
        if not rows:
            logger.debug("append_new_ads called with empty list — nothing to do.")
            return

        self._require_connection()
        assert self._ws_new_ads is not None

        if self._cfg.dry_run:
            logger.info(
                "[DRY-RUN] Would append %d row(s) to '%s'.",
                len(rows),
                self._cfg.ws_new_ads,
            )
            for row in rows:
                logger.debug("  DRY-RUN row: %s", row)
            return

        logger.info("Appending %d new ad row(s) to '%s' …", len(rows), self._cfg.ws_new_ads)
        # Use batch append to reduce API calls and handle quota errors with retry.
        self._batch_append_with_retry(self._ws_new_ads, rows)
        logger.info("Done appending rows.")

    def _batch_append_with_retry(
        self,
        worksheet: gspread.Worksheet,
        rows: list[list[Any]],
        max_retries: int = 5,
        base_delay: float = 10.0,
    ) -> None:
        """
        Append rows to a worksheet using batch operation with retry logic for quota errors.

        Args:
            worksheet: The gspread Worksheet to append to.
            rows: List of rows to append.
            max_retries: Maximum number of retry attempts on 429 errors.
            base_delay: Base delay in seconds for exponential backoff.
        """
        for attempt in range(max_retries):
            try:
                worksheet.append_rows(rows, value_input_option="USER_ENTERED")
                return
            except Exception as exc:
                error_str = str(exc)
                # Check for quota exceeded error (429)
                if "429" in error_str or "quota" in error_str.lower():
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(
                            "Quota exceeded (attempt %d/%d). Waiting %.1f seconds before retry...",
                            attempt + 1, max_retries, delay,
                        )
                        time.sleep(delay)
                        continue
                # Re-raise if not a quota error or retries exhausted
                raise

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
        # Use batch append to reduce API calls and handle quota errors with retry.
        rows = [[ad_id] for ad_id in ad_ids]
        self._batch_append_with_retry(self._ws_processed, rows)

    def upsert_agencies(self, scraped: list[dict[str, str]]) -> None:
        """
        Merge *scraped* agency data into the "Agencies" sheet.

        For each scraped agency:
        • If Agency_Name (case-insensitive) already exists in the sheet →
          merge its phone list (unique union of existing + new, normalised)
          and update Email only if the existing value is empty.
        • If it does not exist → append a new row.

        Args:
            scraped: List of dicts with keys 'Agency_Name', 'Phones', 'Email'.
                     'Phones' should already be a comma-separated, normalised
                     string (e.g. "0894860795,070011777").
        """
        if not scraped:
            logger.debug("upsert_agencies: nothing to upsert.")
            return

        self._require_connection()
        assert self._ws_agencies is not None

        if self._cfg.dry_run:
            logger.info(
                "[DRY-RUN] Would upsert %d agency record(s).", len(scraped)
            )
            return

        # Load the current sheet state into a mutable list.
        existing_rows: list[dict[str, str]] = self.load_agencies_full()

        # Build a lookup: lowercased name → row index in existing_rows
        # We'll rebuild the entire sheet to handle updates cleanly.
        name_to_idx: dict[str, int] = {
            row["Agency_Name"].lower(): i
            for i, row in enumerate(existing_rows)
            if row["Agency_Name"]
        }

        new_rows_added = 0
        rows_updated = 0

        for scraped_agency in scraped:
            name = scraped_agency.get("Agency_Name", "").strip()
            if not name:
                continue

            new_phones_raw = scraped_agency.get("Phones", "")
            new_phones: set[str] = {
                p for p in (normalise_phone(x.strip()) for x in new_phones_raw.split(","))
                if p
            }
            new_email = scraped_agency.get("Email", "").strip()

            name_lower = name.lower()
            if name_lower in name_to_idx:
                # ── Update existing row ──────────────────────────────────
                idx = name_to_idx[name_lower]
                existing = existing_rows[idx]

                # Merge phone sets (keep unique, normalised).
                existing_phones: set[str] = {
                    p for p in (normalise_phone(x.strip()) for x in existing["Phones"].split(","))
                    if p
                }
                merged_phones = existing_phones | new_phones
                existing_rows[idx]["Phones"] = ",".join(sorted(merged_phones))

                # Update email only if currently empty.
                if not existing["Email"] and new_email:
                    existing_rows[idx]["Email"] = new_email

                rows_updated += 1
                logger.debug("Updated existing agency: '%s'", name)
            else:
                # ── Append new row ───────────────────────────────────────
                existing_rows.append({
                    "Agency_Name": name,
                    "Phones":      ",".join(sorted(new_phones)),
                    "Email":       new_email,
                })
                name_to_idx[name_lower] = len(existing_rows) - 1
                new_rows_added += 1
                logger.debug("New agency added: '%s'", name)

        # Rewrite the entire Agencies sheet with the merged data.
        # gspread: clear everything except headers, then write all rows at once.
        logger.info(
            "Writing Agencies sheet: %d new, %d updated, %d total rows.",
            new_rows_added,
            rows_updated,
            len(existing_rows),
        )

        # Build the full matrix including headers.
        matrix: list[list[str]] = [self._cfg.agencies_headers]
        for row in existing_rows:
            matrix.append([
                row["Agency_Name"],
                row["Phones"],
                row["Email"],
            ])

        # Resize sheet if needed, then write in one batch call.
        total_rows_needed = len(matrix) + 10  # headroom
        if self._ws_agencies.row_count < total_rows_needed:
            self._ws_agencies.resize(rows=total_rows_needed)

        # Clear the sheet and rewrite (batch-friendly).
        self._ws_agencies.clear()
        self._ws_agencies.update(
            values=matrix,
            range_name="A1",
            value_input_option="USER_ENTERED",
        )

        logger.info("Agencies sheet updated successfully.")

    # ── Private ─────────────────────────────────────────────────────────────

    def _require_connection(self) -> None:
        if self._spreadsheet is None:
            raise RuntimeError(
                "SheetsClient.connect() must be called before any read/write operation."
            )