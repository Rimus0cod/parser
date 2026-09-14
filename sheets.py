from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Optional

import gspread
from google.oauth2.service_account import Credentials

from config import Config

logger = logging.getLogger("imoti_scraper")


# ---------------------------------------------------------------------------
# Helper: normalise a phone number to digits-only string
# ---------------------------------------------------------------------------


def normalise_phone(raw: str) -> str:
    """
    Improved phone number normalization for Bulgarian numbers.
    Uses the enhanced logic from utils.py
    """
    from utils import normalize_phone_number

    return normalize_phone_number(raw)


def _ensure_worksheet(
    spreadsheet: gspread.Spreadsheet,
    title: str,
    headers: list[str],
) -> gspread.Worksheet:
    """Get or create worksheet with specified headers."""
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        logger.info("Creating new worksheet '%s' …", title)
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(headers))
        # Add headers
        if headers:
            ws.append_row(headers, value_input_option="USER_ENTERED")
        logger.debug("Created worksheet '%s' with headers: %s", title, headers)
    return ws


def _column_label(idx: int) -> str:
    """Convert 1-based column index to A1 column label."""
    if idx < 1:
        raise ValueError("Column index must >= 1")
    label = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        label = chr(65 + rem) + label
    return label


def _column_index_or_none(header: list[str], column_name: str) -> Optional[int]:
    """Find the 0-based index of a column in a header row, or None if not found."""
    try:
        return header.index(column_name)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class SheetsClient:
    def __init__(
        self,
        sheet_id: str,
        service_account_file: Path,
        sheet_name: str = "Imoti_BG_Rentals",
        ws_new_ads: str = "New_Ads",
        ws_agencies: str = "Agencies",
        ws_processed: str = "Processed_IDs",
        ws_renters: str = "Renters",
        new_ads_headers: list[str] = None,
        agencies_headers: list[str] = None,
        renters_headers: list[str] = None,
    ) -> None:
        self._sheet_id = sheet_id
        self._service_account_file = service_account_file
        self._sheet_name = sheet_name
        self._ws_new_ads_name = ws_new_ads
        self._ws_agencies_name = ws_agencies
        self._ws_processed_name = ws_processed
        self._ws_renters_name = ws_renters
        self._new_ads_headers = new_ads_headers or [
            "Date",
            "Ad_ID",
            "Title",
            "Price",
            "Location",
            "Size",
            "Link",
            "Phone",
            "Seller_Name",
            "Type",
            "Contact_Name",
            "Contact_Email",
        ]
        self._agencies_headers = agencies_headers or [
            "Agency_Name",
            "Phones",
            "City",
            "Email",
            "Contact_Name",
        ]
        self._renters_headers = renters_headers or [
            "Name",
            "Phone",
            "Email",
            "City",
            "Apartment_Type",
            "Max_Price",
        ]

        self._sheets_client: gspread.Client | None = None
        self._spreadsheet: gspread.Spreadsheet | None = None
        self._ws_new_ads: gspread.Worksheet | None = None
        self._ws_agencies: gspread.Worksheet | None = None
        self._ws_processed: gspread.Worksheet | None = None
        self._ws_renters: gspread.Worksheet | None = None

    def connect(self) -> None:
        """Establish connection to Google Sheets API."""
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ]
        credentials = Credentials.from_service_account_file(
            self._service_account_file, scopes=scopes
        )
        self._sheets_client = gspread.authorize(credentials)
        self._spreadsheet = self._sheets_client.open_by_key(self._sheet_id)

        # Open or create worksheets
        self._ws_new_ads = _ensure_worksheet(
            self._spreadsheet, self._ws_new_ads_name, self._new_ads_headers
        )
        self._ws_agencies = _ensure_worksheet(
            self._spreadsheet, self._ws_agencies_name, self._agencies_headers
        )
        self._ws_processed = _ensure_worksheet(
            self._spreadsheet, self._ws_processed_name, ["Ad_ID"]
        )
        self._ws_renters = _ensure_worksheet(
            self._spreadsheet, self._ws_renters_name, self._renters_headers
        )

        # Ensure New_Ads has both Contact_Name and Contact_Email columns
        self._ensure_new_ads_columns()

        logger.info(
            "Connected to Google Sheet '%s' (ID: %s). Worksheets ready.",
            self._sheet_name,
            self._sheet_id,
        )

    def _ensure_new_ads_columns(self) -> None:
        """Ensure the New_Ads sheet has Contact_Name and Contact_Email columns."""
        assert self._ws_new_ads is not None

        header = self._ws_new_ads.row_values(1)
        if not header:
            # If there's no header row, initialize with the expected headers
            self._ws_new_ads.update(
                range_name="A1",
                values=[self._new_ads_headers],
                value_input_option="USER_ENTERED",
            )
            logger.info("New_Ads header initialized with %d columns.", len(self._new_ads_headers))
            return

        needed = [col for col in ("Contact_Name", "Contact_Email") if col not in header]
        if needed:
            # Add missing columns
            for col in needed:
                header.append(col)
            self._ws_new_ads.update(
                range_name="A1",
                values=[header],
                value_input_option="USER_ENTERED",
            )
            logger.info("Added columns to New_Ads: %s", needed)

    def load_processed_ids(self) -> set[str]:
        """Load all processed ad IDs from the Processed_IDs sheet."""
        self._require_connection()
        assert self._ws_processed is not None

        values = self._ws_processed.col_values(1)  # First column contains IDs
        # Skip header if present
        return {row.strip() for row in values[1:] if row.strip()}

    def load_agency_phones(self) -> set[str]:
        """Load agency phones from the Agencies sheet."""
        self._require_connection()
        assert self._ws_agencies is not None

        values = self._ws_agencies.get_all_values()
        if len(values) <= 1:  # Only header row exists
            return set()

        header = values[0]
        phones_idx = _column_index_or_none(header, "Phones")
        if phones_idx is None:
            return set()

        phones_set = set()
        for row in values[1:]:
            if phones_idx < len(row):
                phones_str = row[phones_idx].strip()
                if phones_str:
                    # Split comma-separated phones
                    for phone in phones_str.split(","):
                        phone = phone.strip()
                        if phone:
                            phones_set.add(phone)

        return phones_set

    def load_agency_names(self) -> set[str]:
        """Load agency names from the Agencies sheet."""
        self._require_connection()
        assert self._ws_agencies is not None

        values = self._ws_agencies.get_all_values()
        if len(values) <= 1:  # Only header row exists
            return set()

        header = values[0]
        name_idx = _column_index_or_none(header, "Agency_Name")
        if name_idx is None:
            return set()

        names_set = set()
        for row in values[1:]:
            if name_idx < len(row):
                name = row[name_idx].strip()
                if name:
                    names_set.add(name)

        return names_set

    def load_agency_contact_map(self) -> dict[str, dict[str, str]]:
        """
        Load agency contact mapping (keyed by agency name or phone) to contact details.
        """
        self._require_connection()
        assert self._ws_agencies is not None

        values = self._ws_agencies.get_all_values()
        if len(values) <= 1:  # Only header row exists
            return {}

        header = values[0]
        name_idx = _column_index_or_none(header, "Agency_Name")
        phones_idx = _column_index_or_none(header, "Phones")
        email_idx = _column_index_or_none(header, "Email")
        contact_name_idx = _column_index_or_none(header, "Contact_Name")

        result = {}
        for row in values[1:]:
            agency_info = {
                "contact_name": row[contact_name_idx].strip()
                if contact_name_idx is not None and contact_name_idx < len(row)
                else "",
                "email": row[email_idx].strip()
                if email_idx is not None and email_idx < len(row)
                else "",
                "phone": row[phones_idx].strip()
                if phones_idx is not None and phones_idx < len(row)
                else "",
            }

            # Add mapping by agency name
            if name_idx is not None and name_idx < len(row):
                name = row[name_idx].strip()
                if name:
                    result[name.lower()] = agency_info

            # Add mappings by phone numbers
            if phones_idx is not None and phones_idx < len(row):
                phones_str = row[phones_idx].strip()
                if phones_str:
                    for phone in phones_str.split(","):
                        phone = phone.strip()
                        if phone:
                            result[phone] = agency_info

        return result

    def load_new_ads_for_backfill(self) -> list[dict[str, Any]]:
        """
        Load rows from New_Ads sheet where Contact_Name or Contact_Email are missing.
        Returns a list of dictionaries with row information.
        """
        self._require_connection()
        assert self._ws_new_ads is not None

        values = self._ws_new_ads.get_all_values()
        if len(values) <= 1:  # Only header row exists
            return []

        header = values[0]

        # Check for required columns
        required = {
            "row_number": _column_index_or_none(header, "Row_Number"),  # We'll use row index
            "ad_id": _column_index_or_none(header, "Ad_ID"),
            "link": _column_index_or_none(header, "Link"),
            "phone": _column_index_or_none(header, "Phone"),
            "seller_name": _column_index_or_none(header, "Seller_Name"),
            "ad_type": _column_index_or_none(header, "Type"),
            "contact_name": _column_index_or_none(header, "Contact_Name"),
            "contact_email": _column_index_or_none(header, "Contact_Email"),
        }

        missing_required = [k for k, v in required.items() if v is None]
        if missing_required:
            raise RuntimeError(
                f"New_Ads is missing required columns for backfill: {missing_required}"
            )

        rows: list[dict[str, Any]] = []
        for offset, row in enumerate(values[1:], start=2):

            def cell(col_name: str) -> str:
                idx = required[col_name]
                assert idx is not None
                return row[idx].strip() if idx < len(row) else ""

            # Only include rows that need backfill
            contact_name = cell("contact_name")
            contact_email = cell("contact_email")

            if not contact_name or contact_name == "-" or not contact_email or contact_email == "-":
                rows.append(
                    {
                        "row_number": offset,
                        "ad_id": cell("ad_id"),
                        "link": cell("link"),
                        "phone": cell("phone"),
                        "seller_name": cell("seller_name"),
                        "ad_type": cell("ad_type"),
                        "contact_name": contact_name,
                        "contact_email": contact_email,
                    }
                )

        return rows

    def load_agencies_full(self) -> list[dict[str, str]]:
        """
        Load all agencies as a list of dictionaries (for upsert operation).
        """
        self._require_connection()
        assert self._ws_agencies is not None

        all_values = self._ws_agencies.get_all_values()
        if len(all_values) <= 1:  # Only header row exists
            return []

        # Build index mapping header names to column indices
        header = all_values[0]
        idx = {name: i for i, name in enumerate(header)}

        # Check for legacy format (Agency Name, Phone Number, City)
        has_city_col = "City" in idx
        result: list[dict[str, str]] = []

        for row in all_values[1:]:

            def val(column: str) -> str:
                col_idx = idx.get(column)
                if col_idx is None or col_idx >= len(row):
                    return ""
                return row[col_idx].strip()

            # Handle legacy format columns
            legacy_email = row[2].strip() if (not has_city_col and len(row) > 2) else ""
            legacy_contact = row[3].strip() if (not has_city_col and len(row) > 3) else ""

            result.append(
                {
                    "Agency_Name": val("Agency_Name") or (row[0].strip() if len(row) > 0 else ""),
                    "Phones": val("Phones") or (row[1].strip() if len(row) > 1 else ""),
                    "City": val("City"),
                    "Email": val("Email") or legacy_email,
                    "Contact_Name": val("Contact_Name") or legacy_contact,
                }
            )

        return result

    def append_new_ads(self, listings) -> None:
        """
        Append new listings to the New_Ads worksheet.
        """
        from scraper import Listing  # Import here to avoid circular import

        self._require_connection()
        assert self._ws_new_ads is not None

        if not listings:
            logger.debug("append_new_ads: no listings to append.")
            return

        # Convert listings to rows
        today = time.strftime("%Y-%m-%d")
        normalised_rows = []
        for listing in listings:
            if isinstance(listing, dict):
                # If it's already a dictionary
                row = [
                    today,  # Date
                    listing.get("ad_id", ""),
                    listing.get("title", ""),
                    listing.get("price", ""),
                    listing.get("location", ""),
                    listing.get("size", ""),
                    listing.get("link", ""),
                    listing.get("phone", ""),
                    listing.get("seller_name", ""),
                    listing.get("ad_type", ""),
                    listing.get("contact_name", "-"),
                    listing.get("contact_email", "-"),
                ]
            else:
                # If it's a Listing object
                row = [
                    today,  # Date
                    listing.ad_id,
                    listing.title,
                    listing.price,
                    listing.location,
                    listing.size,
                    listing.link,
                    listing.phone,
                    listing.seller_name,
                    listing.ad_type,
                    listing.contact_name,
                    listing.contact_email,
                ]
            normalised_rows.append(row)

        if not normalised_rows:
            return

        logger.info(
            "Appending %d new ad row(s) to '%s' …", len(normalised_rows), self._ws_new_ads_name
        )
        # Use batch append to reduce API calls and handle quota errors with retry.
        self._batch_append_with_retry(self._ws_new_ads, normalised_rows)
        logger.info("Done appending rows.")

    def _batch_append_with_retry(
        self,
        worksheet: gspread.Worksheet,
        rows: list[list[str]],
        max_retries: int = 5,
        base_delay: float = 10.0,
    ) -> None:
        for attempt in range(max_retries):
            try:
                worksheet.append_rows(rows, value_input_option="USER_ENTERED")
                return
            except gspread.exceptions.APIError as e:
                error_str = str(e).lower()
                # Check for quota exceeded error (429)
                if "429" in error_str or "quota" in error_str.lower():
                    if attempt < max_retries - 1:
                        delay = base_delay * (2**attempt)  # Exponential backoff
                        logger.warning(
                            "Quota exceeded (attempt %d/%d). Waiting %.1f seconds before retry...",
                            attempt + 1,
                            max_retries,
                            delay,
                        )
                        time.sleep(delay)
                        continue
                raise

    def mark_processed(self, ad_ids: list[str]) -> None:
        if not ad_ids:
            return

        self._require_connection()
        assert self._ws_processed is not None

        # Prepare rows to append (each ID in its own row)
        rows_to_append = [[ad_id] for ad_id in ad_ids]

        logger.info(
            "Marking %d ad ID(s) as processed in '%s' …", len(ad_ids), self._ws_processed_name
        )
        self._ws_processed.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        logger.info("Done marking processed.")

    def update_new_ads_contacts(self, listings) -> None:
        """
        Update Contact_Name and Contact_Email columns in New_Ads for specific rows.
        """
        self._require_connection()
        assert self._ws_new_ads is not None

        if not listings:
            return

        # Get current header to find column indices
        header = self._ws_new_ads.row_values(1)
        try:
            ad_id_idx = header.index("Ad_ID") + 1  # Convert to 1-based index
            contact_name_idx = header.index("Contact_Name") + 1
            contact_email_idx = header.index("Contact_Email") + 1
        except ValueError as exc:
            raise RuntimeError(
                "New_Ads must contain Contact_Name and Contact_Email columns."
            ) from exc

        # Find the rows that need updating by searching for the Ad_IDs
        all_values = self._ws_new_ads.get_all_values()
        ranges = []

        # Skip header row, start from index 1
        for row_idx, row in enumerate(all_values[1:], start=2):  # Start from row 2 (after header)
            if ad_id_idx <= len(row) and row[ad_id_idx - 1]:  # Adjust for 0-based indexing
                ad_id = row[ad_id_idx - 1].strip()

                # Find the corresponding listing
                target_listing = None
                for listing in listings:
                    if isinstance(listing, dict):
                        if listing.get("ad_id") == ad_id:
                            target_listing = listing
                            break
                    else:
                        if listing.ad_id == ad_id:
                            target_listing = listing
                            break

                if target_listing:
                    # Create range updates for this row
                    contact_name_val = (
                        target_listing.contact_name
                        if hasattr(target_listing, "contact_name")
                        else target_listing.get("contact_name", "-")
                    )
                    contact_email_val = (
                        target_listing.contact_email
                        if hasattr(target_listing, "contact_email")
                        else target_listing.get("contact_email", "-")
                    )

                    ranges.append(
                        {
                            "range": f"{_column_label(contact_name_idx)}{row_idx}",
                            "values": [[contact_name_val]],
                        }
                    )
                    ranges.append(
                        {
                            "range": f"{_column_label(contact_email_idx)}{row_idx}",
                            "values": [[contact_email_val]],
                        }
                    )

        if not ranges:
            return

        logger.info("Updating New_Ads contacts for %d listing(s).", len(listings))
        for range_item in ranges:
            self._ws_new_ads.update(
                range_item["range"], range_item["values"], value_input_option="USER_ENTERED"
            )
        logger.info("Contact updates completed.")

    def upsert_agencies(self, scraped: list[dict[str, str]]) -> None:
        if not scraped:
            logger.debug("upsert_agencies: nothing to upsert.")
            return

        self._require_connection()
        assert self._ws_agencies is not None

        logger.info("Upserting %d agency record(s).", len(scraped))
        for rec in scraped:
            logger.debug("  Upserting agency: %s", rec)

        # Load the current sheet state into a mutable list.
        existing_rows: list[dict[str, str]] = self.load_agencies_full()

        # Create a lookup map for quick access
        name_to_idx: dict[str, int] = {}
        for idx, row in enumerate(existing_rows):
            name_lower = row.get("Agency_Name", "").lower()
            if name_lower:
                name_to_idx[name_lower] = idx

        # Track changes
        rows_updated = 0
        new_rows_added = 0

        for scraped_agency in scraped:
            name = scraped_agency.get("Agency_Name", "").strip()
            if not name:
                logger.warning("Skipping agency with empty name: %s", scraped_agency)
                continue

            name_lower = name.lower()

            # ── Parse incoming phones ─────────────────────────────────────
            new_phones_raw = scraped_agency.get("Phones", "")
            new_phones: set[str] = {
                p for p in (normalise_phone(x.strip()) for x in new_phones_raw.split(",")) if p
            }

            new_city = scraped_agency.get("City", "").strip()
            new_email = scraped_agency.get("Email", "").strip()
            new_contact_name = scraped_agency.get("Contact_Name", "").strip()

            name_lower = name.lower()
            if name_lower in name_to_idx:
                # ── Update existing row ───────────────────────────────────
                idx = name_to_idx[name_lower]
                existing = existing_rows[idx]

                # Merge phone sets (unique union, normalised).
                existing_phones: set[str] = {
                    p
                    for p in (normalise_phone(x.strip()) for x in existing["Phones"].split(","))
                    if p
                }
                merged_phones = existing_phones | new_phones
                existing_rows[idx]["Phones"] = ",".join(sorted(merged_phones))

                # Update Email only if the existing value is absent / sentinel.
                existing_email = existing.get("Email", "").strip()
                if (not existing_email or existing_email == "-") and new_email and new_email != "-":
                    existing_rows[idx]["Email"] = new_email

                # Update Contact_Name only if the existing value is absent / sentinel.
                existing_cn = existing.get("Contact_Name", "").strip()
                if (
                    (not existing_cn or existing_cn == "-")
                    and new_contact_name
                    and new_contact_name != "-"
                ):
                    existing_rows[idx]["Contact_Name"] = new_contact_name

                rows_updated += 1
                logger.debug("Updated existing agency: '%s'", name)

            else:
                # ── Append new row ────────────────────────────────────────
                existing_rows.append(
                    {
                        "Agency_Name": name,
                        "Phones": ",".join(sorted(new_phones)),
                        "City": new_city,
                        "Email": new_email,
                        "Contact_Name": new_contact_name,
                    }
                )
                name_to_idx[name_lower] = len(existing_rows) - 1
                new_rows_added += 1
                logger.debug("New agency added: '%s'", name)

        logger.info(
            "Agencies upsert complete: %d updated, %d added, total %d rows in sheet.",
            rows_updated,
            new_rows_added,
            len(existing_rows),
        )

        # Column order must match self._agencies_headers:
        #   Agency_Name | Phones | City | Email | Contact_Name
        matrix: list[list[str]] = [self._agencies_headers]
        for row in existing_rows:
            matrix.append(
                [
                    row.get("Agency_Name", ""),
                    row.get("Phones", ""),
                    row.get("City", ""),
                    row.get("Email", ""),
                    row.get("Contact_Name", ""),
                ]
            )

        # Clear all old content first, then rewrite from scratch.
        self._ws_agencies.clear()
        self._ws_agencies.update(
            range_name=f"A1:E{len(matrix)}",
            values=matrix,
            value_input_option="USER_ENTERED",
        )
        logger.info("Agencies sheet rewritten with %d rows.", len(matrix))

        logger.info("Agencies sheet updated successfully.")

    def _require_connection(self) -> None:
        if self._spreadsheet is None:
            raise RuntimeError("SheetsClient not connected. Call connect() first.")


# End of sheets.py
