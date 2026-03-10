
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

def _ensure_worksheet(
    spreadsheet: gspread.Spreadsheet,
    title: str,
    header_row: list[str] | None = None,
) -> gspread.Worksheet:
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


def _column_label(idx: int) -> str:
    """Convert 1-based column index to A1 column label."""
    if idx < 1:
        raise ValueError("Column index must be >= 1")
    label = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        label = chr(65 + rem) + label
    return label


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class SheetsClient:
    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._spreadsheet: gspread.Spreadsheet | None = None
        self._ws_new_ads: gspread.Worksheet | None = None
        self._ws_agencies: gspread.Worksheet | None = None
        self._ws_processed: gspread.Worksheet | None = None
        self._ws_renters: gspread.Worksheet | None = None

    # ── Connection ──────────────────────────────────────────────────────────

    def connect(self) -> None:
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
        self._ensure_new_ads_columns()
        logger.info("Google Sheets connection established.")

    def _ensure_new_ads_columns(self) -> None:
        assert self._ws_new_ads is not None

        header = [h.strip() for h in self._ws_new_ads.row_values(1)]
        if not header:
            self._ws_new_ads.update(
                range_name="A1",
                values=[self._cfg.new_ads_headers],
                value_input_option="USER_ENTERED",
            )
            logger.info("New_Ads header initialized with %d columns.", len(self._cfg.new_ads_headers))
            return

        needed = [col for col in ("Contact_Name", "Contact_Email") if col not in header]
        if not needed:
            return

        start_col = len(header) + 1
        end_col = start_col + len(needed) - 1
        rng = f"{_column_label(start_col)}1:{_column_label(end_col)}1"
        self._ws_new_ads.update(
            range_name=rng,
            values=[needed],
            value_input_option="USER_ENTERED",
        )
        logger.info("New_Ads header migrated: added columns %s", ", ".join(needed))

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
        self._require_connection()
        assert self._ws_agencies is not None

        all_values: list[list[str]] = self._ws_agencies.get_all_values()
        names: set[str] = set()

        for row in all_values[1:]:
            if row and row[0].strip():
                names.add(row[0].strip().lower())

        logger.info("Loaded %d agency names from sheet.", len(names))
        return names

    def load_agency_contact_map(self) -> dict[str, dict[str, str]]:
        agencies = self.load_agencies_full()
        out: dict[str, dict[str, str]] = {}
        for row in agencies:
            name = row.get("Agency_Name", "").strip().lower()
            if not name:
                continue
            contact_name = row.get("Contact_Name", "").strip() or "-"
            contact_email = row.get("Email", "").strip() or "-"
            out[name] = {
                "contact_name": contact_name if contact_name else "-",
                "contact_email": contact_email if contact_email else "-",
            }
        logger.info("Loaded %d agency contact records from sheet.", len(out))
        return out

    def load_new_ads_for_backfill(self) -> list[dict[str, Any]]:
        self._require_connection()
        assert self._ws_new_ads is not None

        values = self._ws_new_ads.get_all_values()
        if not values:
            return []

        header = [h.strip() for h in values[0]]
        index: dict[str, int] = {name: idx for idx, name in enumerate(header)}

        required = {
            "ad_id": index.get("Ad_ID"),
            "link": index.get("Link"),
            "seller_name": index.get("Seller_Name"),
            "ad_type": index.get("Type"),
            "phone": index.get("Phone"),
            "contact_name": index.get("Contact_Name"),
            "contact_email": index.get("Contact_Email"),
        }
        missing_required = [k for k, v in required.items() if v is None]
        if missing_required:
            raise RuntimeError(f"New_Ads is missing required columns for backfill: {missing_required}")

        rows: list[dict[str, Any]] = []
        for offset, row in enumerate(values[1:], start=2):
            def cell(col_name: str) -> str:
                idx = required[col_name]
                assert idx is not None
                if idx >= len(row):
                    return ""
                return row[idx].strip()

            rows.append(
                {
                    "row_number": offset,
                    "ad_id": cell("ad_id"),
                    "link": cell("link"),
                    "seller_name": cell("seller_name"),
                    "ad_type": cell("ad_type"),
                    "phone": cell("phone"),
                    "contact_name": cell("contact_name"),
                    "contact_email": cell("contact_email"),
                }
            )

        logger.info("Loaded %d New_Ads row(s) for backfill scan.", len(rows))
        return rows

    def load_agencies_full(self) -> list[dict[str, str]]:
        self._require_connection()
        assert self._ws_agencies is not None

        all_values: list[list[str]] = self._ws_agencies.get_all_values()
        if not all_values:
            return []

        header = [h.strip() for h in all_values[0]]
        idx = {name: i for i, name in enumerate(header)}

        has_city_col = "City" in idx
        result: list[dict[str, str]] = []

        for row in all_values[1:]:
            def val(column: str) -> str:
                col_idx = idx.get(column)
                if col_idx is None or col_idx >= len(row):
                    return ""
                return row[col_idx].strip()

            # Legacy sheet fallback:
            # old layout was Agency_Name | Phones | Email | Contact_Name (no City).
            legacy_email = row[2].strip() if (not has_city_col and len(row) > 2) else ""
            legacy_contact = row[3].strip() if (not has_city_col and len(row) > 3) else ""

            result.append({
                "Agency_Name":  val("Agency_Name") or (row[0].strip() if len(row) > 0 else ""),
                "Phones":       val("Phones") or (row[1].strip() if len(row) > 1 else ""),
                "City":         val("City"),
                "Email":        val("Email") or legacy_email,
                "Contact_Name": val("Contact_Name") or legacy_contact,
            })

        return result

    # ── Write helpers ───────────────────────────────────────────────────────

    def append_new_ads(self, rows: list[list[Any]]) -> None:
        if not rows:
            logger.debug("append_new_ads called with empty list — nothing to do.")
            return

        self._require_connection()
        assert self._ws_new_ads is not None
        expected_len = len(self._cfg.new_ads_headers)
        normalised_rows: list[list[Any]] = []
        for row in rows:
            row_copy = list(row)
            if len(row_copy) < expected_len:
                row_copy.extend(["-"] * (expected_len - len(row_copy)))
                logger.debug(
                    "append_new_ads: padded row to %d columns (Ad_ID=%s).",
                    expected_len,
                    row_copy[1] if len(row_copy) > 1 else "?",
                )
            elif len(row_copy) > expected_len:
                logger.warning(
                    "append_new_ads: trimming row from %d to %d columns (Ad_ID=%s).",
                    len(row_copy),
                    expected_len,
                    row_copy[1] if len(row_copy) > 1 else "?",
                )
                row_copy = row_copy[:expected_len]
            normalised_rows.append(row_copy)

        if self._cfg.dry_run:
            logger.info(
                "[DRY-RUN] Would append %d row(s) to '%s'.",
                len(normalised_rows),
                self._cfg.ws_new_ads,
            )
            for row in normalised_rows:
                logger.debug("  DRY-RUN row: %s", row)
            return

        logger.info("Appending %d new ad row(s) to '%s' …", len(normalised_rows), self._cfg.ws_new_ads)
        # Use batch append to reduce API calls and handle quota errors with retry.
        self._batch_append_with_retry(self._ws_new_ads, normalised_rows)
        logger.info("Done appending rows.")

    def _batch_append_with_retry(
        self,
        worksheet: gspread.Worksheet,
        rows: list[list[Any]],
        max_retries: int = 5,
        base_delay: float = 10.0,
    ) -> None:
        
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

    def update_new_ads_contacts(
        self,
        updates: list[dict[str, Any]],
        batch_size: int = 200,
    ) -> None:
        
        if not updates:
            return

        self._require_connection()
        assert self._ws_new_ads is not None

        header = [h.strip() for h in self._ws_new_ads.row_values(1)]
        try:
            contact_name_idx = header.index("Contact_Name") + 1
            contact_email_idx = header.index("Contact_Email") + 1
        except ValueError as exc:
            raise RuntimeError("New_Ads must contain Contact_Name and Contact_Email columns.") from exc

        ranges: list[dict[str, Any]] = []
        for item in updates:
            row_number = int(item["row_number"])
            left = _column_label(contact_name_idx)
            right = _column_label(contact_email_idx)
            ranges.append(
                {
                    "range": f"{left}{row_number}:{right}{row_number}",
                    "values": [[item.get("contact_name", "-"), item.get("contact_email", "-")]],
                }
            )

        if self._cfg.dry_run:
            logger.info("[DRY-RUN] Would update contact columns for %d New_Ads row(s).", len(ranges))
            return

        for start in range(0, len(ranges), batch_size):
            chunk = ranges[start:start + batch_size]
            self._ws_new_ads.batch_update(chunk, value_input_option="USER_ENTERED")
        logger.info("Updated New_Ads contacts for %d row(s).", len(ranges))

    def upsert_agencies(self, scraped: list[dict[str, str]]) -> None:
        
        if not scraped:
            logger.debug("upsert_agencies: nothing to upsert.")
            return

        self._require_connection()
        assert self._ws_agencies is not None

        if self._cfg.dry_run:
            logger.info(
                "[DRY-RUN] Would upsert %d agency record(s).", len(scraped)
            )
            for rec in scraped:
                logger.debug("  DRY-RUN agency: %s", rec)
            return

        # Load the current sheet state into a mutable list.
        existing_rows: list[dict[str, str]] = self.load_agencies_full()

        # Build a lookup: lowercased name → row index in existing_rows.
        # We rebuild the entire sheet to handle in-place updates cleanly.
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

            # ── Parse incoming phones ─────────────────────────────────────
            new_phones_raw = scraped_agency.get("Phones", "")
            new_phones: set[str] = {
                p for p in (normalise_phone(x.strip()) for x in new_phones_raw.split(","))
                if p
            }

            new_city         = scraped_agency.get("City", "").strip()
            new_email        = scraped_agency.get("Email", "").strip()
            new_contact_name = scraped_agency.get("Contact_Name", "").strip()

            name_lower = name.lower()

            if name_lower in name_to_idx:
                # ── Update existing row ───────────────────────────────────
                idx = name_to_idx[name_lower]
                existing = existing_rows[idx]

                # Merge phone sets (unique union, normalised).
                existing_phones: set[str] = {
                    p for p in (normalise_phone(x.strip()) for x in existing["Phones"].split(","))
                    if p
                }
                merged_phones = existing_phones | new_phones
                existing_rows[idx]["Phones"] = ",".join(sorted(merged_phones))

                existing_city = existing.get("City", "").strip()
                if (not existing_city or existing_city == "-") and new_city and new_city != "-":
                    existing_rows[idx]["City"] = new_city

                # Update Email only if the existing value is absent / sentinel.
                existing_email = existing.get("Email", "").strip()
                if (not existing_email or existing_email == "-") and new_email and new_email != "-":
                    existing_rows[idx]["Email"] = new_email

                # Update Contact_Name only if the existing value is absent / sentinel.
                existing_cn = existing.get("Contact_Name", "").strip()
                if (not existing_cn or existing_cn == "-") and new_contact_name and new_contact_name != "-":
                    existing_rows[idx]["Contact_Name"] = new_contact_name

                rows_updated += 1
                logger.debug("Updated existing agency: '%s'", name)

            else:
                # ── Append new row ────────────────────────────────────────
                existing_rows.append({
                    "Agency_Name":  name,
                    "Phones":       ",".join(sorted(new_phones)),
                    "City":         new_city,
                    "Email":        new_email,
                    "Contact_Name": new_contact_name,
                })
                name_to_idx[name_lower] = len(existing_rows) - 1
                new_rows_added += 1
                logger.debug("New agency added: '%s'", name)

        # ── Rewrite the entire Agencies sheet with the merged data ────────
        logger.info(
            "Writing Agencies sheet: %d new, %d updated, %d total rows.",
            new_rows_added,
            rows_updated,
            len(existing_rows),
        )

        
        # Column order must match Config.agencies_headers:
        #   Agency_Name | Phones | City | Email | Contact_Name
        matrix: list[list[str]] = [self._cfg.agencies_headers]
        for row in existing_rows:
            matrix.append([
                row.get("Agency_Name",  ""),
                row.get("Phones",       ""),
                row.get("City",         ""),
                row.get("Email",        ""),
                row.get("Contact_Name", ""),
            ])

        # Resize sheet if needed to fit all rows, then batch-write.
        total_rows_needed = len(matrix) + 10   # small headroom
        if self._ws_agencies.row_count < total_rows_needed:
            self._ws_agencies.resize(rows=total_rows_needed)

        # Clear and rewrite atomically.
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