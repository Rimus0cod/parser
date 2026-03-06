# imoti.bg Rental Scraper Bot

A production-ready Python 3.11+ daily scraper that monitors apartment rental listings on **imoti.bg**, stores new ads in Google Sheets, and sends a beautifully formatted HTML email digest.

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
- Visits each **detail page** to extract the contact phone number and seller name.
- **Three-tier agency classification** (see section below).
- Stores results in **four Google Sheets tabs**: `New_Ads`, `Agencies`, `Processed_IDs`, `Renters`.
- Sends a **styled HTML email** with a table of all new listings.
- Polite scraping: **random 2–5 s delay** between requests.
- Full **logging** with coloured Rich output + optional file.
- CLI flags: `--force`, `--dry-run`.
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
4. Share the spreadsheet with the service account's email address (shown in the JSON under `"client_email"`):
   - Click **Share** (top-right) → paste the email → role: **Editor** → **Send**.

### Step 4 — Configure .env

```bash
cp .env.example .env
```

Edit `.env` and set:
```
GOOGLE_SHEET_ID=<paste the spreadsheet ID>
SERVICE_ACCOUNT_JSON=/home/youruser/secrets/imoti-bot-service-account.json
```

---

## Gmail App Password Setup

Gmail requires an **App Password** when 2-Step Verification is enabled (which is mandatory for this use case — never use your main account password).

1. Make sure **2-Step Verification** is ON on your Google account:
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

All options are set in `.env` (see `.env.example` for the full list):

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
| `MAX_PAGES` | No | `30` | Max pages to scrape |
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
```

---

## Google Sheets Layout

The bot automatically creates **four** worksheet tabs on first run:

### `New_Ads`

Auto-populated by the scraper. **Do not edit manually** — rows are appended.

| Date | Ad_ID | Title | Price | Location | Size | Link | Phone | Type |
|---|---|---|---|---|---|---|---|---|
| 2025-06-01 | 513894 | Двустаен апартамент | 700 EUR\месец | София, Бункера | 60 кв.м. | https://… | 0894860795 | приватний |

---

### `Agencies`

**Manually maintained** list of known real-estate agencies.  
The scraper reads this sheet to build its agency phone lookup set.

| Agency_Name | Phones | Email |
|---|---|---|
| Хоби Имоти ЕООД | 0894860795,070011777 | office@hoboimoti.bg |
| Имоти България | 070011777 | office@imoti.bg |
| Агенция XYZ | 0888123456 | |

**Column details:**

- **`Agency_Name`** — free-text name for your reference (any string).
- **`Phones`** — **comma-separated** list of normalised phone numbers.
  - Digits only, no spaces or dashes (e.g. `0894860795,070011777`).
  - The scraper splits by comma, normalises each fragment, and loads all
    into its lookup set.
  - International format is also fine — `+359 88 486 0795` normalises to
    `359884860795`. Use the same format the site shows in ads.
  - If an agency uses 3 numbers, put all three in one row:  
    `0888111222,0888333444,070011777`
- **`Email`** — optional; not used by the scraper (informational only).

> **Backwards compatibility:** If a row has only one column with a phone number
> (old single-column layout), the scraper falls back to reading column A.

---

### `Processed_IDs`

Auto-maintained by the scraper. One column: `Ad_ID`.

Every Ad ID is appended here after processing.  On subsequent runs, any ID
already in this sheet is skipped (unless `--force` is used).

**Do not delete rows** unless you intentionally want an ad re-processed.

---

### `Renters`

**Manually maintained** registry of potential tenants you are tracking.  
The scraper **creates this sheet** on first run but **never reads from or writes to it**.

| Name | Phone | Email | City | Apartment_Type | Max_Price |
|---|---|---|---|---|---|
| Іван Петров | 0898765432 | ivan@example.com | София | 1-room,2-room | 700 EUR |
| Maria Schmidt | 0877123456 | | Пловдив | 2-room | 500 EUR |

**Column meanings:**
- `Name` — full name of the renter.
- `Phone` — normalised phone(s), comma-separated if multiple contacts.
- `Email` — contact email.
- `City` — desired city.
- `Apartment_Type` — comma-separated desired types (e.g. `1-room,2-room`).
- `Max_Price` — budget ceiling (e.g. `700 EUR`, `1500 BGN`).

This sheet is for **your own reference only** — for example, to match new
listings in `New_Ads` against what your renters are looking for.

---

## Agency Classification Logic

The scraper uses **three-tier classification**, applied in priority order:

```
┌─────┬────────────────────────────────────────────────────────────────┐
│ Tier│ Condition                              → Result                │
├─────┼────────────────────────────────────────────────────────────────┤
│  1  │ Ad phone is in the Agencies sheet      → "від агенції"         │
│     │ (user-maintained multi-phone list)                             │
│     │ Strongest signal — explicit user override.                     │
├─────┼────────────────────────────────────────────────────────────────┤
│  2  │ Detail page <h3> label is NOT          → "від агенції"         │
│     │ "Частно лице" (an agency name is       (page shows the         │
│     │ shown by the site itself)               agency directly)       │
├─────┼────────────────────────────────────────────────────────────────┤
│  3  │ Seller name contains a keyword         → "від агенції"         │
│     │ (masking detection):                                           │
│     │   агенция, агенція, agency, агенц,                             │
│     │   имоти, realty, estate, еоод, оод, ад                        │
│     │ Catches agencies that post under a                             │
│     │ person's name but reveal their company                         │
│     │ suffix in the seller field.                                    │
├─────┼────────────────────────────────────────────────────────────────┤
│  4  │ None of the above                      → "приватний"           │
└─────┴────────────────────────────────────────────────────────────────┘
```

**When the phone is hidden (JavaScript-protected):**
- Phone is stored as `""` in `New_Ads`.
- Classification still proceeds via Tiers 2–4 using the seller name.
- A `WARNING` log line is emitted so you can review these ads manually.

**When you find an agency not caught automatically:**
- Add its phone number(s) to the `Agencies` sheet — that is the most reliable fix.
- Alternatively, open an issue or add its name keyword to `AGENCY_NAME_KEYWORDS`
  in `scraper.py`.

---

## Scheduling with cron (Linux / Ubuntu)

Run once per day at 08:00:

```bash
crontab -e
```

Add the following line (adjust paths):

```cron
0 8 * * * /home/youruser/imoti_scraper/.venv/bin/python /home/youruser/imoti_scraper/scraper.py >> /home/youruser/imoti_scraper/cron.log 2>&1
```

View recent log output:
```bash
tail -f /home/youruser/imoti_scraper/cron.log
```

---

## Optional: Background Scheduling with APScheduler

If you prefer to keep the script running as a daemon instead of using cron,
uncomment `apscheduler` in `requirements.txt` and replace the `__main__` block
at the bottom of `scraper.py`:

```python
from apscheduler.schedulers.blocking import BlockingScheduler

if __name__ == "__main__":
    import sys
    # Run once immediately on startup, then daily at 08:00 Sofia time.
    main()
    scheduler = BlockingScheduler(timezone="Europe/Sofia")
    scheduler.add_job(main, "cron", hour=8, minute=0)
    print("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
```

Then run:
```bash
python scraper.py    # runs indefinitely, fires daily at 08:00
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
sudo journalctl -u imoti-scraper -f
```

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `SERVICE_ACCOUNT_JSON not found` | Check the path in `.env`. Use an absolute path. |
| `SpreadsheetNotFound` | Make sure you shared the sheet with the service account email. |
| `SMTP authentication failed` | Use a Gmail App Password, not your account password. |
| `503 / 429 from imoti.bg` | Increase `REQUEST_DELAY_MIN` / `REQUEST_DELAY_MAX` in `.env`. |
| Phone not found on detail page | The number may be JS-protected. A WARNING is logged; classification still works via seller name. |
| Agency classified as "приватний" | Add the agency's phone(s) to the `Agencies` sheet. Or check if its name contains a recognised keyword. |
| City filter not working | `CITY_FILTER` must be a Bulgarian substring matching the `Location` column (e.g. `София`, not `Sofia`). |
| Want to re-process old ads | Run `python scraper.py --force`. |
| Want to test without side effects | Run `python scraper.py --dry-run`. |

---

## Security Notes

- **Never commit `.env`** to version control. Add it to `.gitignore`.
- Store the service-account JSON **outside** the project directory if possible.
- The Gmail App Password grants access to **send email only**.

---

## License

MIT — do whatever you like, at your own risk.
