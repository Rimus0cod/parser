# imoti.bg Rental Scraper Bot

A production-ready Python 3.11+ daily scraper that monitors apartment rental
listings on **imoti.bg**, stores new ads in Google Sheets, and sends a
beautifully formatted HTML email digest.

---

## Project Structure

```
imoti_scraper/
├── scraper.py        ← Entry point — run this
├── config.py         ← Configuration (loaded from .env)
├── sheets.py         ← Google Sheets read/write client
├── email_sender.py   ← SMTP email builder and sender
├── .env              ← Your secrets (create from .env.example)
├── .env.example      ← Template — copy and fill in values
├── requirements.txt  ← Python dependencies
└── README.md         ← This file
```

---

## Features

- Scrapes **all pages** of `https://imoti.bg/наеми/` (up to ~26 pages).
- Filters for **apartment** listings only (`апартамент`, `едностаен`, `двустаен`, etc.).
- **Optimised requests**: tries to extract phone + seller name from the listing card first
  to avoid unnecessary detail-page fetches.
- Visits the **detail page** only when phone or seller name is not visible on the card.
- Extracts `Seller_Name` (e.g. `Частно лице` or `Агенція XYZ`) from detail pages.
- **Enhanced classification** — classifies each listing as **"приватний"** (private) or
  **"від агенції"** (agency) using two independent checks:
  1. Phone number match against the `Agencies` sheet (exact match).
  2. Seller name **substring match** against known agency names — catches masking agencies
     that list themselves as private individuals but use a recognisable name.
- Stores results in **four Google Sheets tabs**: `New_Ads`, `Agencies`, `Processed_IDs`,
  `Renters`.
- **`--update-agencies` flag**: scrapes `https://imoti.bg/агенції/` (up to ~13 pages)
  and upserts the results into the `Agencies` sheet — useful for keeping the agency
  database fresh.  Run periodically (e.g. weekly) to improve agency detection.
- Sends a **styled HTML email** with a table of all new listings (includes `Seller_Name`
  column).
- **User-Agent rotation**: chooses a random browser UA from a pool of 5 on every run
  (and on every retry) to reduce fingerprinting risk.
- Polite scraping: **random 2–5 s delay** between requests + retry with backoff.
- Full **logging** with coloured Rich output + optional log file.
- CLI flags: `--force`, `--dry-run`, `--update-agencies`.
- Optional **city filter** via `.env`.

---

## Prerequisites

- Python **3.11** or newer
- A Google account with access to Google Cloud Console
- A Gmail account with an **App Password** (or any other SMTP provider)

---

## Installation

```bash
# 1. Clone or copy the project folder
cd imoti_scraper

# 2. Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate      # Linux / macOS
# .venv\Scripts\activate       # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Google Service Account Setup

### Step 1 — Create a Google Cloud project

1. Go to [https://console.cloud.google.com/](https://console.cloud.google.com/).
2. Create a new project (e.g. `imoti-scraper`).
3. Enable the **Google Sheets API** and **Google Drive API**:
   - Navigation menu → **APIs & Services** → **Library**.
   - Search for "Google Sheets API" → **Enable**.
   - Search for "Google Drive API" → **Enable**.

### Step 2 — Create a Service Account

1. Navigation menu → **IAM & Admin** → **Service Accounts**.
2. Click **Create Service Account**.
   - Name: `imoti-scraper-bot`
   - Click **Create and Continue** → **Done**.
3. Click on the newly created service account.
4. Go to the **Keys** tab → **Add Key** → **Create new key** → **JSON**.
5. The JSON file is downloaded automatically. Save it somewhere safe, e.g.:
   ```
   /home/youruser/secrets/imoti-bot-service-account.json
   ```

### Step 3 — Create and share the Google Spreadsheet

1. Go to [https://sheets.google.com](https://sheets.google.com) and create a new spreadsheet.
2. Name it exactly: **`Imoti_BG_Rentals`** (or whatever you set in `SHEET_NAME`).
3. Copy the **Spreadsheet ID** from the URL:
   ```
   https://docs.google.com/spreadsheets/d/THIS_IS_THE_ID/edit
   ```
4. Share the spreadsheet with the service account's email address (shown in the JSON
   under `"client_email"`):
   - Click **Share** (top-right) → paste the email → role: **Editor** → **Send**.

### Step 4 — Configure .env

```bash
cp .env.example .env
```

Edit `.env` and set at minimum:
```
GOOGLE_SHEET_ID=<paste the spreadsheet ID>
SERVICE_ACCOUNT_JSON=/home/youruser/secrets/imoti-bot-service-account.json
```

---

## Gmail App Password Setup

Gmail requires an **App Password** when 2-Step Verification is enabled.

1. Ensure **2-Step Verification** is ON:
   [https://myaccount.google.com/security](https://myaccount.google.com/security)

2. Go to **App Passwords**:
   [https://myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)

3. Select app: **Mail** / Select device: **Other (custom name)** → type `imoti-scraper`.

4. Click **Generate** — you get a 16-character password (shown once).

5. Add to `.env`:
   ```
   EMAIL_FROM=your.email@gmail.com
   EMAIL_TO=recipient@example.com
   SMTP_USER=your.email@gmail.com
   SMTP_PASSWORD=abcd efgh ijkl mnop
   ```

---

## Configuration Reference

All options are set in `.env` (see `.env.example` for the full annotated list):

| Variable | Required | Default | Description |
|---|---|---|---|
| `GOOGLE_SHEET_ID` | **Yes** | — | Spreadsheet ID from the URL |
| `SERVICE_ACCOUNT_JSON` | **Yes** | — | Path to service-account JSON |
| `SHEET_NAME` | No | `Imoti_BG_Rentals` | Name of the Google Spreadsheet |
| `EMAIL_FROM` | No | — | Sender email address |
| `EMAIL_TO` | No | — | Recipient(s), comma-separated |
| `SMTP_SERVER` | No | `smtp.gmail.com` | SMTP host |
| `SMTP_PORT` | No | `587` | SMTP port (TLS) |
| `SMTP_USER` | No | — | SMTP login username |
| `SMTP_PASSWORD` | No | — | SMTP password / App Password |
| `MAX_PAGES` | No | `30` | Max rental listing pages to scrape |
| `MAX_AGENCY_PAGES` | No | `15` | Max agency directory pages (for `--update-agencies`) |
| `REQUEST_DELAY_MIN` | No | `2.0` | Min delay between requests (s) |
| `REQUEST_DELAY_MAX` | No | `5.0` | Max delay between requests (s) |
| `CITY_FILTER` | No | *(all cities)* | City substring filter (e.g. `София`) |
| `LOG_LEVEL` | No | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_FILE` | No | *(console only)* | Path to a log file |

---

## Running the Scraper

```bash
# Activate the venv first
source .venv/bin/activate

# Normal daily run
python scraper.py

# Re-process ALL ads (useful for initial setup / testing)
python scraper.py --force

# Scrape but don't write to Sheets or send email
python scraper.py --dry-run

# Combine: re-check everything but don't save anything
python scraper.py --force --dry-run

# Update the Agencies sheet from imoti.bg agency directory
python scraper.py --update-agencies

# Preview agency update without saving
python scraper.py --update-agencies --dry-run

# Update agencies AND run the normal daily scrape in one go
python scraper.py --update-agencies --force
```

---

## Google Sheets Layout

The bot automatically creates four worksheet tabs on first run:

### `New_Ads`

| Date | Ad_ID | Title | Price | Location | Size | Link | Phone | Seller_Name | Type |
|---|---|---|---|---|---|---|---|---|---|
| 2025-06-01 | 513894 | Двустаен апартамент | 700 EUR/месец | София, Бункера | 60 кв.м. | https://… | 0894860795 | Частно лице | приватний |
| 2025-06-01 | 513895 | Тристаен апартамент | 950 EUR/месец | София, Лозенец | 90 кв.м. | https://… | 0884123456 | Агенция XYZ | від агенції |

**New column: `Seller_Name`**
Extracted from the detail page (or list page when visible).  Contains the
seller's self-declared name — e.g. `Частно лице` for a private person, or
the agency's full name.  Empty if the site did not expose this information.

### `Agencies` (enhanced multi-column)

| Agency_Name | Phones | Email |
|---|---|---|
| Агенция XYZ | 0894860795,070011777 | info@agency.com |
| Имот Плюс | 0888123456 | |

- **`Agency_Name`** — human-readable agency name.
- **`Phones`** — comma-separated list of normalised phone numbers (digits only).
  When `--update-agencies` is run, new phones are merged into existing rows
  (unique union) — no duplicates.
- **`Email`** — optional contact email.  Updated if the existing value is empty.

> **Tip:** You can manually add or edit rows in this sheet at any time.
> The script respects manually added entries on every run.

### `Processed_IDs`

| Ad_ID |
|---|
| 513894 |
| 513895 |

Auto-maintained list of Ad IDs already seen.  The bot skips any ad whose ID
is in this list (unless `--force` is used).

### `Renters` (manual-only)

| Name | Phone | Email | City | Apartment_Type | Max_Price |
|---|---|---|---|---|---|
| *(your data)* | | | | | |

This tab is created automatically on first run but the script **never writes
to it**.  Use it to track potential renters manually.

---

## Agency Detection & Masking

The classification logic uses two independent checks to detect agencies:

### Check 1 — Phone match
If a listing's extracted phone number exactly matches any phone stored in
the `Agencies` sheet → `від агенції`.

This is the most reliable signal: an agency's phone number is their real
identifier and is hard to fake.

### Check 2 — Seller name substring match
If a listing's `Seller_Name` (lowercased) contains any agency name
(lowercased) from the `Agencies` sheet as a substring → `від агенції`.

This catches **masking agencies** — agencies that list themselves as
`Частно лице` (private person) on the listing page but use a recognisable
agency name somewhere in their profile.

**Example:**
If the `Agencies` sheet contains `агенция xyz`, and a listing has
`Seller_Name = "Агенция XYZ - Иван Иванов"` → matched → `від агенції`.

### Keeping the agency database fresh

Run `--update-agencies` periodically (e.g. weekly or monthly) to import
new agencies from the imoti.bg directory:

```bash
python scraper.py --update-agencies
```

This scrapes `https://imoti.bg/агенції/page:N` (all pages, up to
`MAX_AGENCY_PAGES`) and upserts the results into the `Agencies` sheet.
New agencies are added; existing agencies get their phone lists merged.

---

## Scheduling with cron (Linux / Ubuntu)

Run once per day at 08:00, and refresh agencies every Sunday at 07:00:

```bash
# Edit crontab
crontab -e
```

Add:
```cron
# Daily rental scrape at 08:00
0 8 * * * /home/youruser/imoti_scraper/.venv/bin/python /home/youruser/imoti_scraper/scraper.py >> /home/youruser/imoti_scraper/cron.log 2>&1

# Weekly agency refresh on Sunday at 07:00
0 7 * * 0 /home/youruser/imoti_scraper/.venv/bin/python /home/youruser/imoti_scraper/scraper.py --update-agencies >> /home/youruser/imoti_scraper/cron.log 2>&1
```

View logs:
```bash
tail -f /home/youruser/imoti_scraper/cron.log
```

---

## Optional: systemd Service (Linux)

Create `/etc/systemd/system/imoti-scraper.service`:

```ini
[Unit]
Description=imoti.bg Rental Scraper Bot
After=network-online.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/home/youruser/imoti_scraper
ExecStart=/home/youruser/imoti_scraper/.venv/bin/python scraper.py
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable imoti-scraper
sudo systemctl start imoti-scraper
sudo journalctl -u imoti-scraper -f   # follow logs
```

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `SERVICE_ACCOUNT_JSON not found` | Check the path in `.env`. Use an absolute path. |
| `SpreadsheetNotFound` | Make sure you shared the sheet with the service account email. |
| `SMTP authentication failed` | Use a Gmail App Password, not your account password. |
| `503 / 429 from imoti.bg` | Increase `REQUEST_DELAY_MIN` / `REQUEST_DELAY_MAX` in `.env`. |
| Phone not found on detail page | The site may render the phone via JavaScript. The scraper stores an empty phone in that case and logs a warning. |
| `Seller_Name` always empty | The site structure may have changed. Run with `LOG_LEVEL=DEBUG` to inspect the parsed HTML and update the CSS selectors in `scraper.py:parse_detail_page`. |
| Agency not detected (masking) | Run `--update-agencies` to refresh the Agencies sheet. Also check that the agency's name in the sheet matches how it appears in listings. |
| City filter not working | Make sure `CITY_FILTER` matches the Bulgarian city name exactly as it appears in the `Location` column (e.g. `София`, not `Sofia`). |

---

## Security Notes

- **Never commit `.env`** to version control — add it to `.gitignore`.
- Store the service-account JSON **outside** the project directory if possible.
- The Gmail App Password grants **send-only** access — it cannot read your inbox.
- The service account has **Editor** access to the specific spreadsheet only.

---

## License

MIT — do whatever you like, at your own risk.