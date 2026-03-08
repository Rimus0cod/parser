"""
MySQL persistence layer for imoti scraper.

Stores:
- listings (New_Ads payload)
- agencies
- processed_ids
"""

from __future__ import annotations

import logging
from typing import Any

from config import Config

logger = logging.getLogger(__name__)


class MySQLStore:
    def __init__(self, config: Config) -> None:
        self._cfg = config
        self._conn = None

    def connect(self) -> None:
        if not self._cfg.mysql_enabled:
            return

        try:
            import mysql.connector  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "mysql-connector-python is not installed. "
                "Run: pip install mysql-connector-python"
            ) from exc

        self._conn = mysql.connector.connect(
            host=self._cfg.mysql_host,
            port=self._cfg.mysql_port,
            user=self._cfg.mysql_user,
            password=self._cfg.mysql_password,
            database=self._cfg.mysql_database,
            autocommit=True,
            charset="utf8mb4",
            use_unicode=True,
        )
        self._ensure_schema()
        logger.info("MySQL connected (%s:%s/%s)", self._cfg.mysql_host, self._cfg.mysql_port, self._cfg.mysql_database)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass

    def _ensure_schema(self) -> None:
        assert self._conn is not None
        cur = self._conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_ids (
                ad_id VARCHAR(32) PRIMARY KEY,
                processed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS agencies (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                agency_name VARCHAR(255) NOT NULL,
                phones TEXT,
                city VARCHAR(128),
                email VARCHAR(255),
                contact_name VARCHAR(255),
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_agency_name (agency_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS listings (
                ad_id VARCHAR(32) PRIMARY KEY,
                date_seen DATE NULL,
                title TEXT,
                price VARCHAR(128),
                location VARCHAR(255),
                size VARCHAR(64),
                link TEXT,
                phone VARCHAR(64),
                seller_name VARCHAR(255),
                ad_type VARCHAR(64),
                contact_name VARCHAR(255),
                contact_email VARCHAR(255),
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_ad_type (ad_type),
                INDEX idx_seller_name (seller_name(191))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cur.close()

    def _ensure_conn(self) -> None:
        if self._conn is None:
            raise RuntimeError("MySQLStore.connect() must be called first.")

    def load_processed_ids(self) -> set[str]:
        self._ensure_conn()
        cur = self._conn.cursor()
        cur.execute("SELECT ad_id FROM processed_ids")
        out = {row[0].strip() for row in cur.fetchall() if row and row[0]}
        cur.close()
        return out

    def mark_processed(self, ad_ids: list[str]) -> None:
        if not ad_ids:
            return
        self._ensure_conn()
        cur = self._conn.cursor()
        cur.executemany(
            """
            INSERT INTO processed_ids (ad_id)
            VALUES (%s)
            ON DUPLICATE KEY UPDATE processed_at = CURRENT_TIMESTAMP
            """,
            [(x,) for x in ad_ids],
        )
        cur.close()

    def load_agency_phones(self) -> set[str]:
        self._ensure_conn()
        cur = self._conn.cursor()
        cur.execute("SELECT phones FROM agencies")
        phones: set[str] = set()
        for (raw,) in cur.fetchall():
            if not raw:
                continue
            for ph in str(raw).split(","):
                ph = ph.strip()
                if ph:
                    phones.add(ph)
        cur.close()
        return phones

    def load_agency_names(self) -> set[str]:
        self._ensure_conn()
        cur = self._conn.cursor()
        cur.execute("SELECT agency_name FROM agencies")
        out = {str(row[0]).strip().lower() for row in cur.fetchall() if row and row[0]}
        cur.close()
        return out

    def load_agency_contact_map(self) -> dict[str, dict[str, str]]:
        self._ensure_conn()
        cur = self._conn.cursor()
        cur.execute("SELECT agency_name, email, contact_name FROM agencies")
        out: dict[str, dict[str, str]] = {}
        for row in cur.fetchall():
            name = str(row[0] or "").strip().lower()
            if not name:
                continue
            out[name] = {
                "contact_email": str(row[1] or "-").strip() or "-",
                "contact_name": str(row[2] or "-").strip() or "-",
            }
        cur.close()
        return out

    def upsert_agencies(self, rows: list[dict[str, str]]) -> None:
        if not rows:
            return
        self._ensure_conn()

        cur = self._conn.cursor()
        cur.execute("SELECT agency_name, phones, city, email, contact_name FROM agencies")
        existing: dict[str, dict[str, str]] = {}
        for row in cur.fetchall():
            key = str(row[0] or "").strip().lower()
            if not key:
                continue
            existing[key] = {
                "agency_name": str(row[0] or "").strip(),
                "phones": str(row[1] or "").strip(),
                "city": str(row[2] or "").strip(),
                "email": str(row[3] or "").strip(),
                "contact_name": str(row[4] or "").strip(),
            }

        payload = []
        for src in rows:
            name = str(src.get("Agency_Name", "")).strip()
            if not name:
                continue
            key = name.lower()

            old = existing.get(key, {})
            old_phones = {x.strip() for x in old.get("phones", "").split(",") if x.strip()}
            new_phones = {x.strip() for x in str(src.get("Phones", "")).split(",") if x.strip()}
            merged_phones = ",".join(sorted(old_phones | new_phones))

            city_old = old.get("city", "").strip()
            city_new = str(src.get("City", "")).strip()
            city = city_old or city_new

            email_old = old.get("email", "").strip()
            email_new = str(src.get("Email", "")).strip()
            email = email_old if email_old and email_old != "-" else (email_new or "-")

            cn_old = old.get("contact_name", "").strip()
            cn_new = str(src.get("Contact_Name", "")).strip()
            contact_name = cn_old if cn_old and cn_old != "-" else (cn_new or "-")

            payload.append((name, merged_phones, city, email, contact_name))

        if payload:
            cur.executemany(
                """
                INSERT INTO agencies (agency_name, phones, city, email, contact_name)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    phones = VALUES(phones),
                    city = VALUES(city),
                    email = VALUES(email),
                    contact_name = VALUES(contact_name)
                """,
                payload,
            )

        cur.close()

    def upsert_listings(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self._ensure_conn()

        payload = []
        for r in rows:
            payload.append(
                (
                    str(r.get("Ad_ID", "")),
                    str(r.get("Date", "")) or None,
                    str(r.get("Title", "")),
                    str(r.get("Price", "")),
                    str(r.get("Location", "")),
                    str(r.get("Size", "")),
                    str(r.get("Link", "")),
                    str(r.get("Phone", "")),
                    str(r.get("Seller_Name", "")),
                    str(r.get("Type", "")),
                    str(r.get("Contact_Name", "-")) or "-",
                    str(r.get("Contact_Email", "-")) or "-",
                )
            )

        cur = self._conn.cursor()
        cur.executemany(
            """
            INSERT INTO listings (
                ad_id, date_seen, title, price, location, size, link,
                phone, seller_name, ad_type, contact_name, contact_email
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                date_seen = VALUES(date_seen),
                title = VALUES(title),
                price = VALUES(price),
                location = VALUES(location),
                size = VALUES(size),
                link = VALUES(link),
                phone = VALUES(phone),
                seller_name = VALUES(seller_name),
                ad_type = VALUES(ad_type),
                contact_name = VALUES(contact_name),
                contact_email = VALUES(contact_email)
            """,
            payload,
        )
        cur.close()

    def upsert_from_new_ads_sheet_rows(self, rows: list[dict[str, Any]]) -> None:
        """Persist rows loaded from SheetsClient.load_new_ads_for_backfill()."""
        shaped = []
        for r in rows:
            shaped.append(
                {
                    "Date": "",
                    "Ad_ID": r.get("ad_id", ""),
                    "Title": "",
                    "Price": "",
                    "Location": "",
                    "Size": "",
                    "Link": r.get("link", ""),
                    "Phone": r.get("phone", ""),
                    "Seller_Name": r.get("seller_name", ""),
                    "Type": r.get("ad_type", ""),
                    "Contact_Name": r.get("contact_name", "-") or "-",
                    "Contact_Email": r.get("contact_email", "-") or "-",
                }
            )
        self.upsert_listings(shaped)
