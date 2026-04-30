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

- Async multi-site scraper built on HTTP and Playwright sessions with adaptive selectors and mode escalation
- REST API for health checks, leads, agencies, and manual scrape triggering
- Streamlit dashboard for operators
- MySQL persistence with typed repository layer
- Composite listing identity by `source_site + ad_id` with normalized price and area fields
- Manual-review status and stored field-level extraction issues for parser quality checks
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
  scraping/                  production scraping engine, fetchers, profiles, validation
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

If `SCRAPER_SITES` is left empty, the built-in defaults are used.

For controlled production rollouts, `SCRAPER_SITES` should be a JSON array of full site objects, for example:

```env
SCRAPER_SITES=[{"name":"imoti.bg","base_url":"https://imoti.bg/наеми/page:{page}","max_pages":5,"selectors":{"card":"article, li, section, div","title":"h4 a[href*='/наеми/'], h3 a[href*='/наеми/'], a[href*='/наеми/']","link":"h4 a[href*='/наеми/'], h3 a[href*='/наеми/'], a[href*='/наеми/']","seller":"[class*='agency'], [class*='broker'], [class*='owner']"},"selector_version":"v2","timeout":30.0,"concurrency":4,"enabled":true,"verify_ssl":true,"detail_pages_enabled":true,"mode_order":["http","dynamic","stealth"],"listing_path_keywords":["/наеми/"],"allowed_domains":["imoti.bg"]}]
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
- Browser dependencies for Playwright-backed dynamic/browser mode

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
- keep browser and AI fallback modes disabled until you validate target sites

### 2. Prepare Streamlit users

Generate a password hash:

```bash
python -m app.ui.generate_password_hashes "your-strong-password"
```

Put the resulting hash into [app/ui/users.yaml](/Users/diff/code/parser/app/ui/users.yaml).
Do not keep real operator emails or passwords in the repository version of this file.

Example:

```yaml
credentials:
  usernames:
    admin:
      email: "ops@example.com"
      name: "Operations"
      password: "$2b$12$replace_with_generated_hash"
```

### 3. Install Playwright browser runtime for local development

If you plan to use `dynamic` / `browser` mode locally, install the browser runtime once:

```bash
poetry run playwright install chromium
```

Inside Docker this step is already baked into the image build.

### 4. Build and start services

```bash
docker compose up -d --build
```

Or via `Makefile`:

```bash
make up-build
```

### 5. Check health

```bash
curl http://localhost:8000/health
curl http://localhost:8000/readyz
curl http://localhost:8501/_stcore/health
docker compose ps
```

### 6. Open the platform

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

Main configuration lives in [.env.example](/Users/diff/code/parser/.env.example).

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
- `BROWSER_STRATEGY_ENABLED` / legacy alias `SCRAPLING_DYNAMIC_ENABLED`
- `AI_STRATEGY_ENABLED` / legacy alias `SCRAPLING_STEALTH_ENABLED`
- `BROWSER_CONCURRENCY` / legacy alias `SCRAPLING_DYNAMIC_CONCURRENCY`
- `AI_STRATEGY_CONCURRENCY` / legacy alias `SCRAPLING_STEALTH_CONCURRENCY`
- `SCRAPLING_HTTP_IMPERSONATE`
- `SCRAPLING_HTTP3`
- `SCRAPLING_DISABLE_RESOURCES`
- `SCRAPLING_DISABLE_ADS`
- `SCRAPLING_BLOCK_WEBRTC`
- `SCRAPLING_NETWORK_IDLE`
- `SCRAPLING_SOLVE_CLOUDFLARE`
- `SCRAPLING_STEALTH_HUMANIZE`
- `SCRAPLING_WAIT_SELECTOR_TIMEOUT_MS`
- `SCRAPLING_BLOCKED_MARKERS`
- `SCRAPLING_STORAGE_TABLE`
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
- Must include Playwright browser runtime because `POST /trigger-scrape` can escalate into `dynamic` / `browser`

### `scraper`

- Runs scheduled scraping loop
- Writes run status into Redis
- Writes listings to MySQL
- Uses `http -> browser -> ai` escalation when a source is blocked, JS-rendered, or fails required selector checks

### `streamlit_ui`

- Displays recent leads and statistics
- Requires valid user definitions in `app/ui/users.yaml`

## API

### `GET /health`

Returns a liveness response for the API process.

### `GET /readyz`

Returns dependency readiness for Redis and MySQL and responds with `503` when the stack is not ready.

### `GET /leads?limit=100`

Returns recent leads.

### `GET /leads/review?limit=100`

Returns listings quarantined for manual review because parser validation found field-level warnings.

### `GET /leads/issues?limit=250`

Returns stored extraction issues for the current parser version.

### `GET /agencies?limit=100`

Returns recent agencies if present in storage.

### `POST /trigger-scrape`

Queues a background scrape task in the API process.
Returns `busy` when another scrape run is already active.

## Local Development

Install dependencies:

```bash
poetry install --with dev
poetry run playwright install chromium
```

Run checks:

```bash
make check
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
- Structured logs from `scraping_engine` and `scraping_strategies` include `site`, `mode`, `attempt`, and `blocked_reason`
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
- Keep browser and AI fallback modes off until HTTP mode is validated for each enabled source
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
- Browser-backed modes need enough shared memory; the provided Compose file sets `shm_size: 1gb` for `api` and `scraper`.

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

If logs show `blocked_reason` or repeated fallback to `dynamic` / `browser` / `ai`, validate:

- browser runtime was installed successfully in the image
- `BROWSER_STRATEGY_ENABLED=true` or `AI_STRATEGY_ENABLED=true` for the affected site
- target site still matches the configured selectors in `app/core/config.py`

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

See [LICENSE](/Users/diff/code/parser/LICENSE).
