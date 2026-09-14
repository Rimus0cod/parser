from __future__ import annotations

import logging
import smtplib
from datetime import date
from email.message import EmailMessage
from typing import Any, List

from config import Config

logger = logging.getLogger("imoti_scraper")


# Email templates
_HTML_HEAD = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Нові оголошення за {today}</title>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
    </style>
</head>
<body>
    <h2>Нові оголошення на imoti.bg — {count} намери</h2>
    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>Заголовок</th>
                <th>Цена</th>
                <th>Местоположение</th>
                <th>Размер</th>
                <th>Линк</th>
            </tr>
        </thead>
        <tbody>
"""

_HTML_ROW = """            <tr>
                <td>{id}</td>
                <td>{title}</td>
                <td>{price}</td>
                <td>{location}</td>
                <td>{size}</td>
                <td><a href="{link}">Виж</a></td>
            </tr>"""

_HTML_FOOT = """        </tbody>
    </table>
</body>
</html>"""


def build_html_body(listings, today: str) -> str:
    """Build HTML email body from listings."""
    from scraper import Listing  # Local import to avoid circular dependency

    rows_html = ""
    for idx, listing in enumerate(listings, start=1):
        if isinstance(listing, Listing):
            # It's a Listing object
            rows_html += _HTML_ROW.format(
                id=listing.ad_id,
                title=_escape(listing.title),
                price=_escape(listing.price),
                location=_escape(listing.location),
                size=_escape(listing.size),
                link=_escape_attr(listing.link),
            )
        else:
            # It's a dictionary
            rows_html += _HTML_ROW.format(
                id=_escape(str(listing.get("Ad_ID", ""))),
                title=_escape(str(listing.get("Title", ""))),
                price=_escape(str(listing.get("Price", ""))),
                location=_escape(str(listing.get("Location", ""))),
                size=_escape(str(listing.get("Size", ""))),
                link=_escape_attr(str(listing.get("Link", "#"))),
            )

    html = _HTML_HEAD.format(today=_escape(today), count=len(listings)) + rows_html + _HTML_FOOT
    return html


def build_plain_text(listings, today: str) -> str:
    """Build plain text email body from listings."""
    from scraper import Listing  # Local import to avoid circular dependency

    lines = [
        f"Нови оголошения за наем на имоти — {today}",
        f"Брой нови оголошения: {len(listings)}",
        "=" * 50,
        "",
    ]

    for idx, listing in enumerate(listings, start=1):
        if isinstance(listing, Listing):
            # It's a Listing object
            lines.extend(
                [
                    f"{idx}. {listing.title}",
                    f"   Цена: {listing.price}",
                    f"   Локация: {listing.location}",
                    f"   Размер: {listing.size}",
                    f"   Линк: {listing.link}",
                    "",
                ]
            )
        else:
            # It's a dictionary
            lines.extend(
                [
                    f"{idx}. {listing.get('Title', '')}",
                    f"   Цена: {listing.get('Price', '')}",
                    f"   Локация: {listing.get('Location', '')}",
                    f"   Размер: {listing.get('Size', '')}",
                    f"   Линк: {listing.get('Link', '')}",
                    "",
                ]
            )

    return "\n".join(lines)


def send_email(
    from_addr: str,
    to_addrs: List[str],
    subject: str,
    body: str,
    listings=None,
) -> None:
    """Send email notification."""
    if not listings:
        listings = []

    # Get SMTP configuration from environment
    import os

    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")

    if not from_addr or not to_addrs:
        logger.warning("Email settings incomplete — skipping email notification.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)

    # Set plain text body
    msg.set_content(body)

    # Add HTML version if we have listings
    if listings:
        html_body = build_html_body(listings, date.today().isoformat())
        msg.add_alternative(html_body, subtype="html")

    logger.info(
        "Sending email via %s:%d (user=%s) → %s …",
        smtp_server,
        smtp_port,
        smtp_user,
        to_addrs,
    )

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        logger.info("Email sent successfully.")
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send email: %s", exc)


# Internal helpers
def _escape(text: str) -> str:
    """Minimal HTML escaping to prevent XSS / broken markup in the email body."""
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


def _escape_attr(url: str) -> str:
    """Escape quotes in HTML attributes."""
    return url.replace('"', "%22")
