# Real Estate SaaS Core

Production-oriented platform for asynchronous real-estate scraping with API, worker, UI, MySQL, and Redis.

## Overview

`Real Estate SaaS Core` is an on-premise scraping platform designed for collecting, storing, and reviewing rental listings from multiple sources.

Current runtime includes:

- `api` on `FastAPI`
- `scraper` as a background async worker
- `streamlit_ui` for internal operators
- `mysql` for persistent storage
- `redis` for worker state and operational flags

The project is designed to run primarily through Docker Compose and includes:

- structured logging
- health checks
- background scraping
- MySQL schema bootstrap
- Streamlit authentication
- integration hooks for webhooks / CRM delivery

## Key Features

- Async multi-site scraper built on `httpx`, `asyncio`, `BeautifulSoup`
- REST API for health checks, leads, agencies, and manual scrape triggering
- Streamlit dashboard for operators
- MySQL persistence with typed repository layer
- Redis-backed worker status and run metadata
- Dockerized deployment with health checks and restart policies
- Optional Sentry integration
- Optional webhook, AmoCRM, and Bitrix24 integrations

## Architecture

```text
                    +-------------------+
                    |   Streamlit UI    |
                    |      :8501        |
                    +---------+---------+
                              |
                              v
                    +-------------------+
                    |    FastAPI API    |
                    |      :8000        |
                    +----+----------+---+
                         |          |
                         v          v
                 +-----------+   +--------+
                 |  MySQL 8  |   | Redis  |
                 +-----------+   +--------+
                         ^
                         |
                    +----+----+
                    | Scraper |
                    | Worker  |
                    +---------+
```

## Runtime Stack

- Python `3.11`
- FastAPI
- Streamlit
- MySQL `8`
- Redis `7`
- Docker Compose
- Poetry for local dependency management

## Project Layout

```text
app/
  api/main.py                FastAPI entrypoint and routes
  core/config.py             typed application settings
  core/logging.py            structured logging and optional Sentry setup
  db/mysql.py                MySQL pool and schema initialization
  models/schemas.py          API response models
  services/async_scraper.py  async scraping engine
  services/repository.py     database access layer
  scraper_worker.py          long-running worker loop
  ui/streamlit_app.py        operator dashboard
  ui/users.yaml              Streamlit auth users
  ui/generate_password_hashes.py
integrations/
  amocrm.py                  optional AmoCRM integration
  bitrix24.py                optional Bitrix24 integration
  webhooks.py                optional webhook delivery
Dockerfile
docker-compose.yml
.env.example
Makefile
requirements.txt
pyproject.toml
utils.py
```

## Supported Sources

Built-in source configuration is defined in `app/core/config.py` and can be overridden via `SCRAPER_SITES`.

Default built-in sources:

- `imoti.bg`
- `alo.bg`
- `dom.ria.com`
- `olx.ua`
- `lun.ua`

For controlled production rollouts it is recommended to limit active sources explicitly through `.env`, for example:

```env
SCRAPER_SITES=["imoti.bg","alo.bg"]
```

## Requirements

### For Docker deployment

- Docker `24+`
- Docker Compose `v2+`

### For local development

- Python `3.11.x`
- Poetry `1.8+`
- MySQL `8`
- Redis `7`

## Quick Start

### 1. Prepare environment

```bash
cp .env.example .env
```

Update at minimum:

- `MYSQL_PASSWORD`
- `MYSQL_ROOT_PASSWORD`
- `STREAMLIT_COOKIE_KEY`
- `STREAMLIT_JWT_SECRET`

Recommended:

- set `APP_ENV=prod`
- keep `LOG_FORMAT=json`
- enable `SENTRY_DSN` if you use Sentry
- restrict `SCRAPER_SITES` to the sources you actually want to run

### 2. Prepare Streamlit users

Generate a password hash:

```bash
python -m app.ui.generate_password_hashes "your-strong-password"
```

Put the resulting hash into [`app/ui/users.yaml`](/home/diff/Parser_prod/app/ui/users.yaml).

Example:

```yaml
credentials:
  usernames:
    admin:
      email: "ops@example.com"
      name: "Operations"
      password: "$2b$12$replace_with_generated_hash"
```

### 3. Build and start services

```bash
docker compose up -d --build
```

Or via `Makefile`:

```bash
make up-build
```

### 4. Check health

```bash
curl http://localhost:8000/health
curl http://localhost:8501/_stcore/health
docker compose ps
```

### 5. Open the platform

- UI: `http://localhost:8501`
- API docs: `http://localhost:8000/docs`
- API health: `http://localhost:8000/health`

## First Production Run

### Trigger one scrape manually

```bash
curl -X POST http://localhost:8000/trigger-scrape
```

### Watch worker logs

```bash
docker compose logs -f scraper
```

### Check worker state

```bash
docker compose exec redis redis-cli MGET \
  scrape:worker_status \
  scrape:last_status \
  scrape:last_total_scraped \
  scrape:last_written
```

### Verify data in MySQL

```bash
docker compose exec mysql sh -lc '
mysql -uroot -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" -e "
SELECT source_site, COUNT(*) AS total
FROM listings
GROUP BY source_site
ORDER BY total DESC;
"'
```

## Environment Variables

Main configuration lives in [`.env.example`](/home/diff/Parser_prod/.env.example).

### Core

- `APP_NAME` application name
- `APP_ENV` environment name, usually `prod` or `dev`
- `APP_DEBUG` debug mode

### MySQL

- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `MYSQL_ROOT_PASSWORD`

### Redis

- `REDIS_HOST`
- `REDIS_PORT`
- `REDIS_DB`

### Scraper runtime

- `SCRAPE_TIMEOUT_SECONDS`
- `SCRAPE_INTERVAL_SECONDS`
- `SCRAPE_CONCURRENCY`
- `SCRAPE_RETRY_COUNT`
- `SCRAPE_BACKOFF_BASE_SECONDS`
- `SCRAPE_BACKOFF_CAP_SECONDS`
- `SCRAPE_DELAY_MIN_SECONDS`
- `SCRAPE_DELAY_MAX_SECONDS`
- `SCRAPE_FOLLOW_REDIRECTS`
- `SCRAPE_VERIFY_SSL`
- `SCRAPE_DETAIL_PAGES`
- `HTTP_MAX_CONNECTIONS`
- `HTTP_MAX_KEEPALIVE_CONNECTIONS`
- `CITY_FILTER`
- `SCRAPER_SITES`

### Proxy rotation

- `PROXY_ENABLED`
- `PROXY_LIST`
- `PROXY_ROTATION_STRATEGY`
- `PROXY_MAX_RETRIES`

### Logging and observability

- `LOG_LEVEL`
- `LOG_FORMAT`
- `LOG_TO_FILE`
- `LOG_DIR`
- `SENTRY_DSN`
- `SENTRY_ENVIRONMENT`
- `SENTRY_TRACES_SAMPLE_RATE`

### Streamlit auth

- `STREAMLIT_COOKIE_NAME`
- `STREAMLIT_COOKIE_KEY`
- `STREAMLIT_COOKIE_EXPIRY_DAYS`
- `STREAMLIT_JWT_SECRET`
- `STREAMLIT_USERS_YAML_PATH`

### Integrations

- `WEBHOOKS_*`
- `AMOCRM_*`
- `BITRIX24_*`

### Legacy compatibility

These variables are still present for backward compatibility and migration, but they are not part of the main `api + scraper + streamlit` runtime path:

- `GOOGLE_SHEET_ID`
- `SERVICE_ACCOUNT_JSON`
- `SHEET_NAME`
- `EMAIL_*`
- `SMTP_*`
- `MAX_PAGES`
- `REQUEST_DELAY_*`
- `LOG_FILE`
- `AGENCIES_CSV_PATH`
- `MYSQL_ENABLED`
- `TELEGRAM_*`

## Services

### `mysql`

- Stores scraped data
- Initializes schema on app startup
- Uses named Docker volume `mysql_data`

### `redis`

- Stores worker runtime state
- Used by health checks and operational visibility
- Uses named Docker volume `redis_data`

### `api`

- Exposes REST endpoints
- Can trigger background scrape jobs
- Bootstraps schema on startup

### `scraper`

- Runs scheduled scraping loop
- Writes run status into Redis
- Writes listings to MySQL

### `streamlit_ui`

- Displays recent leads and statistics
- Requires valid user definitions in `app/ui/users.yaml`

## API

### `GET /health`

Returns service health summary.

### `GET /leads?limit=100`

Returns recent leads.

### `GET /agencies?limit=100`

Returns recent agencies if present in storage.

### `POST /trigger-scrape`

Queues a background scrape task in the API process.

## Local Development

Install dependencies:

```bash
poetry install --with dev
```

Run API:

```bash
poetry run uvicorn app.api.main:app --reload --host 0.0.0.0 --port 8000
```

Run worker:

```bash
poetry run python -m app.scraper_worker
```

Run UI:

```bash
poetry run streamlit run app/ui/streamlit_app.py
```

Static checks:

```bash
make lint
make format
make type-check
```

## Operations

### Common commands

```bash
make help
make up
make up-build
make down
make restart
make logs
make logs-api
make logs-scraper
make logs-ui
make ps
```

### Database maintenance

Backup:

```bash
make db-backup
```

Restore:

```bash
make db-restore FILE=backups/backup_YYYYMMDD_HHMMSS.sql
```

Reset:

```bash
make db-reset
```

### Manual operational checks

API health:

```bash
make api-health
```

Trigger scrape:

```bash
make trigger-scrape
```

Container shells:

```bash
make shell-api
make shell-scraper
make shell-mysql
make shell-redis
```

## Logs and Observability

By default the platform uses structured logging.

Recommended production setup:

- `LOG_FORMAT=json`
- `LOG_LEVEL=INFO`
- `LOG_TO_FILE=false` unless you explicitly need file logs
- configure `SENTRY_DSN` for error aggregation

Useful runtime signals:

- API health via `/health`
- Streamlit health via `/_stcore/health`
- Redis keys:
  - `scrape:worker_status`
  - `scrape:last_status`
  - `scrape:last_total_scraped`
  - `scrape:last_written`
  - `scrape:last_error`

## Production Deployment Notes

### Minimum checklist

- Replace every placeholder secret in `.env`
- Use a dedicated database user instead of `root`
- Restrict externally exposed ports
- Put the stack behind reverse proxy / VPN if needed
- Enable backups for MySQL volume
- Set up Sentry or central log shipping
- Restrict `SCRAPER_SITES` to validated sources
- Verify one manual scrape before enabling long-running worker mode

### Recommended deployment model

- `docker compose up -d --build`
- persistent Docker volumes for MySQL and Redis
- external monitoring for container health
- periodic DB backups
- CI pipeline for lint, type-check, and image build

## Known Source-Specific Notes

- Some sites expose masked or bot-protected contact data.
- `alo.bg` may hide phone numbers behind anti-bot flow; in such cases the system should keep `phone` empty instead of storing fake data.
- Source markup changes can break extraction logic, so each enabled source should be validated after deployment.

## Troubleshooting

### `api` or `streamlit_ui` is unhealthy

Check:

```bash
docker compose ps
docker compose logs --tail=100 api
docker compose logs --tail=100 streamlit_ui
```

### Worker runs but no rows appear in MySQL

Check:

```bash
docker compose logs -f scraper
docker compose exec redis redis-cli MGET scrape:last_status scrape:last_total_scraped scrape:last_written
```

If `HTTP 200` exists but `last_total_scraped=0`, the source likely changed markup and needs parser updates.

### MySQL auth errors

Make sure `.env` values match the initialized volume.

If credentials changed after first startup, either:

- create the app user manually in existing MySQL
- or recreate volumes for a clean bootstrap

### Port `3306` already in use

Another local MySQL instance is already bound to the host.

Options:

- stop local MySQL
- or change published port in `docker-compose.yml`

### Full reset

```bash
docker compose down -v
docker compose up -d --build
```

## Security Notes

- Never commit `.env`
- Never keep real service-account JSON in the repository
- Rotate compromised secrets immediately
- Use hashed passwords only in `app/ui/users.yaml`
- Prefer non-root DB access for runtime services
- Keep Docker images and base packages updated

## License

See [LICENSE](/home/diff/Parser_prod/LICENSE).
