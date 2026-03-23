from __future__ import annotations

import logging
import smtplib
from datetime import date
from email.message import EmailMessage
from typing import Any

from config import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTML template helpers
# ---------------------------------------------------------------------------
_HTML_HEAD = """\
<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {{
      font-family: Arial, Helvetica, sans-serif;
      font-size: 14px;
      color: #333;
      background: #f7f7f7;
      margin: 0;
      padding: 20px;
    }}
    .container {{
      max-width: 1040px;
      margin: 0 auto;
      background: #fff;
      border-radius: 8px;
      padding: 24px;
      box-shadow: 0 2px 8px rgba(0,0,0,.12);
    }}
    h1 {{
      color: #2c6fad;
      font-size: 20px;
      margin-top: 0;
    }}
    p.subtitle {{
      color: #666;
      margin-bottom: 20px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    thead tr {{
      background: #2c6fad;
      color: #fff;
    }}
    thead th {{
      padding: 10px 12px;
      text-align: left;
      white-space: nowrap;
    }}
    tbody tr:nth-child(even) {{
      background: #f2f6fb;
    }}
    tbody tr:hover {{
      background: #ddeeff;
    }}
    tbody td {{
      padding: 8px 12px;
      border-bottom: 1px solid #e0e0e0;
      vertical-align: top;
    }}
    .badge-private {{
      display: inline-block;
      background: #27ae60;
      color: #fff;
      border-radius: 4px;
      padding: 2px 8px;
      font-size: 11px;
      white-space: nowrap;
    }}
    .badge-agency {{
      display: inline-block;
      background: #e67e22;
      color: #fff;
      border-radius: 4px;
      padding: 2px 8px;
      font-size: 11px;
      white-space: nowrap;
    }}
    .badge-unknown {{
      display: inline-block;
      background: #95a5a6;
      color: #fff;
      border-radius: 4px;
      padding: 2px 8px;
      font-size: 11px;
      white-space: nowrap;
    }}
    a {{
      color: #2c6fad;
      text-decoration: none;
    }}
    a:hover {{
      text-decoration: underline;
    }}
    .footer {{
      margin-top: 24px;
      font-size: 12px;
      color: #aaa;
      text-align: center;
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Нові оголошення про оренду квартир на imoti.bg</h1>
    <p class="subtitle">
      Дата: <strong>{today}</strong> &nbsp;|&nbsp;
      Знайдено: <strong>{count}</strong> нових оголошень
    </p>
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Назва</th>
          <th>Ціна</th>
          <th>Місто</th>
          <th>Площа</th>
          <th>Телефон</th>
          <th>Продавець</th>
          <th>Тип</th>
          <th>Посилання</th>
        </tr>
      </thead>
      <tbody>
"""

# One row template.  Uses str.format() — all user content MUST be HTML-escaped.
_HTML_ROW = """\
        <tr>
          <td>{idx}</td>
          <td>{title}</td>
          <td><strong>{price}</strong></td>
          <td>{location}</td>
          <td>{size}</td>
          <td>{phone}</td>
          <td>{seller_name}</td>
          <td>{badge}</td>
          <td><a href="{link}" target="_blank">Переглянути</a></td>
        </tr>
"""

_HTML_FOOT = """\
      </tbody>
    </table>
    <div class="footer">
      Це автоматично згенерований лист від imoti.bg scraper bot.
    </div>
  </div>
</body>
</html>
"""


def _make_badge(ad_type: str) -> str:
    """Return an HTML badge element for the given listing type."""
    ad_type_lower = ad_type.lower()
    if "агенці" in ad_type_lower:
        return '<span class="badge-agency">від агенції</span>'
    if "приватний" in ad_type_lower:
        return '<span class="badge-private">приватний</span>'
    return '<span class="badge-unknown">невідомо</span>'


def build_html_body(ads: list[dict[str, Any]], today: str) -> str:
    
    rows_html = ""
    for idx, ad in enumerate(ads, start=1):
        rows_html += _HTML_ROW.format(
            idx=idx,
            title=_escape(str(ad.get("Title", ""))),
            price=_escape(str(ad.get("Price", ""))),
            location=_escape(str(ad.get("Location", ""))),
            size=_escape(str(ad.get("Size", ""))),
            phone=_escape(str(ad.get("Phone", ""))),
            seller_name=_escape(str(ad.get("Seller_Name", ""))),
            badge=_make_badge(str(ad.get("Type", ""))),
            link=_escape_attr(str(ad.get("Link", "#"))),
        )

    html = (
        _HTML_HEAD.format(today=_escape(today), count=len(ads))
        + rows_html
        + _HTML_FOOT
    )
    return html


def build_plain_text(ads: list[dict[str, Any]], today: str) -> str:
    
    lines = [
        f"Нові оголошення про оренду квартир на imoti.bg — {today}",
        f"Знайдено: {len(ads)} нових оголошень",
        "=" * 60,
        "",
    ]
    for idx, ad in enumerate(ads, start=1):
        lines.append(f"{idx}. {ad.get('Title', '')} — {ad.get('Price', '')}")
        lines.append(f"   Місто: {ad.get('Location', '')} | Площа: {ad.get('Size', '')}")
        lines.append(f"   Телефон: {ad.get('Phone', '')} | Продавець: {ad.get('Seller_Name', '')}")
        lines.append(f"   Тип: {ad.get('Type', '')}")
        lines.append(f"   Посилання: {ad.get('Link', '')}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------

def send_email(
    config: Config,
    ads: list[dict[str, Any]],
    today: str | None = None,
) -> None:
    
    if not ads:
        logger.debug("No new ads — skipping email.")
        return

    if not config.email_to:
        logger.warning("EMAIL_TO is not set — skipping email notification.")
        return

    if config.dry_run:
        logger.info(
            "[DRY-RUN] Would send email to %s with %d ad(s).",
            config.email_to, len(ads),
        )
        return

    if today is None:
        today = date.today().isoformat()

    subject = f"Нові оголошення про оренду квартир на imoti.bg — {today}"

    logger.info("Building email for %d ad(s) …", len(ads))
    html_body = build_html_body(ads, today)
    text_body = build_plain_text(ads, today)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = config.email_from or config.smtp_user
    msg["To"]      = config.email_to

    # Set plain text as the primary content, then attach HTML as an alternative.
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    logger.info(
        "Sending email via %s:%d (user=%s) → %s …",
        config.smtp_server,
        config.smtp_port,
        config.smtp_user,
        config.email_to,
    )

    try:
        with smtplib.SMTP(config.smtp_server, config.smtp_port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            if config.smtp_user and config.smtp_password:
                smtp.login(config.smtp_user, config.smtp_password)
            smtp.send_message(msg)
        logger.info("Email sent successfully.")
    except smtplib.SMTPAuthenticationError as exc:
        logger.error(
            "SMTP authentication failed: %s\n"
            "Check SMTP_USER / SMTP_PASSWORD in your .env file.\n"
            "For Gmail, you need an App Password — see README.md.",
            exc,
        )
        raise
    except smtplib.SMTPException as exc:
        logger.error("Failed to send email: %s", exc)
        raise


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _escape(text: str) -> str:
    """Minimal HTML escaping to prevent XSS / broken markup in the email body."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
    )


def _escape_attr(url: str) -> str:
    
    return url.replace('"', "%22")