from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("imoti_scraper")


class MySQLStore:
    """
    MySQL storage for scraped data (listings, agencies, processed IDs).
    """

    def __init__(self, config) -> None:
        self._cfg = config

        # Import MySQL connector
        try:
            import mysql.connector  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "mysql-connector-python is not installed. Run: pip install mysql-connector-python"
            ) from exc

        self._conn = mysql.connector.connect(
            host=self._cfg.mysql_host,
            port=self._cfg.mysql_port,
            user=self._cfg.mysql_user,
            password=self._cfg.mysql_password,
            database=self._cfg.mysql_database,
            charset="utf8mb4",
            use_unicode=True,
        )
        self._ensure_schema()
        logger.info(
            "MySQL connected (%s:%s/%s)",
            self._cfg.mysql_host,
            self._cfg.mysql_port,
            self._cfg.mysql_database,
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _ensure_schema(self) -> None:
        cursor = self._conn.cursor()
        try:
            # Listings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS listings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date DATE NOT NULL,
                    ad_id VARCHAR(20) NOT NULL UNIQUE,
                    title TEXT,
                    price VARCHAR(100),
                    location VARCHAR(200),
                    size VARCHAR(50),
                    link TEXT,
                    phone VARCHAR(20),
                    seller_name VARCHAR(200),
                    ad_type VARCHAR(50),
                    contact_name VARCHAR(200),
                    contact_email VARCHAR(200),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_ad_id (ad_id),
                    INDEX idx_date (date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            # Agencies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS agencies (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    agency_name VARCHAR(200) NOT NULL,
                    phones TEXT,
                    city VARCHAR(100),
                    email VARCHAR(200),
                    contact_name VARCHAR(200),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_agency_name (agency_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            # Processed IDs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_ids (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ad_id VARCHAR(20) NOT NULL UNIQUE,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_ad_id (ad_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)

            self._conn.commit()
            logger.info("MySQL schema verified/created.")
        finally:
            cursor.close()

    def store_listings(self, listings) -> None:
        """
        Store listings in the database.
        """
        if not listings:
            return

        cursor = self._conn.cursor()
        try:
            for listing in listings:
                # Handle both dictionary and Listing object formats
                if hasattr(listing, "ad_id"):
                    # It's a Listing object
                    values = (
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
                    )
                else:
                    # It's a dictionary
                    values = (
                        listing.get("ad_id", ""),
                        listing.get("title", ""),
                        listing.get("price", ""),
                        listing.get("location", ""),
                        listing.get("size", ""),
                        listing.get("link", ""),
                        listing.get("phone", ""),
                        listing.get("seller_name", ""),
                        listing.get("ad_type", ""),
                        listing.get("contact_name", ""),
                        listing.get("contact_email", ""),
                    )

                cursor.execute(
                    """
                    INSERT INTO listings
                    (ad_id, title, price, location, size, link, phone, seller_name, ad_type, contact_name, contact_email)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
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
                    values,
                )

            self._conn.commit()
            logger.info(f"Stored {len(listings)} listings in MySQL")
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Failed to store listings in MySQL: {e}")
            raise
        finally:
            cursor.close()

    def upsert_agencies(self, agencies: list[dict[str, str]]) -> None:
        """
        Insert or update agencies in the database.
        """
        if not agencies:
            return

        cursor = self._conn.cursor()
        try:
            for agency in agencies:
                cursor.execute(
                    """
                    INSERT INTO agencies (agency_name, phones, city, email, contact_name)
                    VALUES (%(Agency_Name)s, %(Phones)s, %(City)s, %(Email)s, %(Contact_Name)s)
                    ON DUPLICATE KEY UPDATE
                        phones = COALESCE(NULLIF(%(Phones)s, ''), phones),
                        city = COALESCE(NULLIF(%(City)s, ''), city),
                        email = COALESCE(NULLIF(%(Email)s, ''), email),
                        contact_name = COALESCE(NULLIF(%(Contact_Name)s, ''), contact_name)
                """,
                    agency,
                )

            self._conn.commit()
            logger.info(f"Upserted {len(agencies)} agencies in MySQL")
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Failed to upsert agencies in MySQL: {e}")
            raise
        finally:
            cursor.close()

    def load_processed_ids(self) -> set[str]:
        """
        Load all processed ad IDs from the database.
        """
        cursor = self._conn.cursor()
        try:
            cursor.execute("SELECT ad_id FROM processed_ids")
            rows = cursor.fetchall()
            return {row[0] for row in rows}
        finally:
            cursor.close()

    def load_agency_phones(self) -> set[str]:
        """
        Load all agency phone numbers from the database.
        """
        cursor = self._conn.cursor()
        try:
            cursor.execute("SELECT phones FROM agencies WHERE phones IS NOT NULL AND phones != ''")
            rows = cursor.fetchall()
            phones_set = set()
            for row in rows:
                phones_str = row[0]
                if phones_str:
                    # Split comma-separated phones
                    for phone in phones_str.split(","):
                        phone = phone.strip()
                        if phone:
                            phones_set.add(phone)
            return phones_set
        finally:
            cursor.close()

    def load_agency_names(self) -> set[str]:
        """
        Load all agency names from the database.
        """
        cursor = self._conn.cursor()
        try:
            cursor.execute("SELECT agency_name FROM agencies WHERE agency_name IS NOT NULL")
            rows = cursor.fetchall()
            return {row[0].strip() for row in rows if row[0].strip()}
        finally:
            cursor.close()

    def load_agency_contact_map(self) -> dict[str, dict[str, str]]:
        """
        Load agency contact mapping from the database.
        """
        cursor = self._conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT agency_name, phones, email, contact_name FROM agencies")
            rows = cursor.fetchall()

            result = {}
            for row in rows:
                agency_info = {
                    "contact_name": row["contact_name"] or "",
                    "email": row["email"] or "",
                    "phone": row["phones"] or "",
                }

                # Map by agency name
                if row["agency_name"]:
                    result[row["agency_name"].lower()] = agency_info

                # Map by phone numbers
                if row["phones"]:
                    for phone in row["phones"].split(","):
                        phone = phone.strip()
                        if phone:
                            result[phone] = agency_info

            return result
        finally:
            cursor.close()

    def mark_processed(self, ad_ids: list[str]) -> None:
        """
        Mark ad IDs as processed in the database.
        """
        if not ad_ids:
            return

        cursor = self._conn.cursor()
        try:
            # Use multi-row insert to efficiently add multiple IDs
            values = [(ad_id,) for ad_id in ad_ids]
            cursor.executemany("INSERT IGNORE INTO processed_ids (ad_id) VALUES (%s)", values)
            self._conn.commit()
            logger.info(f"Marked {len(ad_ids)} ads as processed in MySQL")
        except Exception as e:
            self._conn.rollback()
            logger.error(f"Failed to mark processed IDs in MySQL: {e}")
            raise
        finally:
            cursor.close()
