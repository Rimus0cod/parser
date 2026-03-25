# Real Estate SaaS Core (On-Premise) / Ядро SaaS для недвижимости

Production-ready Phase 1 foundation for a commercial on-premise platform for real estate agencies in Ukraine and Bulgaria.

---

## EN: Overview

### Stack
- **Scraper**: Async `httpx` + `asyncio` + `BeautifulSoup`
- **API**: `FastAPI`
- **UI**: `Streamlit` + `streamlit-authenticator` (multi-user auth with signed cookie token/JWT-style session)
- **Data**: `MySQL 8`
- **Queue/State**: `Redis 7`
- **Dependency manager**: `Poetry`
- **Runtime**: Docker Compose

### Services (`docker-compose.yml`)
- `mysql`
- `redis`
- `api` (FastAPI: `:8000`)
- `scraper` (background async worker)
- `streamlit_ui` (`:8501`)

### Implemented API
- `GET /health`
- `GET /leads?limit=100`
- `GET /agencies?limit=100`
- `POST /trigger-scrape`

### Project Structure
```text
app/
  api/main.py                # FastAPI app and routes
  core/config.py             # Typed settings (pydantic-settings)
  db/mysql.py                # MySQL pool + schema init
  models/schemas.py          # Pydantic API schemas
  services/async_scraper.py  # Async scraper logic (httpx + asyncio)
  services/repository.py     # Typed DB access layer
  scraper_worker.py          # Background scraper loop service
  ui/streamlit_app.py        # Streamlit UI with multi-user auth
  ui/users.yaml              # Auth users config
Dockerfile
docker-compose.yml
pyproject.toml
.env.example
```

### Quick Start
1. Copy env template:
   - `cp .env.example .env` (Linux/macOS)
   - `copy .env.example .env` (Windows)
2. Update secrets in `.env`:
   - `MYSQL_PASSWORD`, `MYSQL_ROOT_PASSWORD`
   - `STREAMLIT_COOKIE_KEY`, `STREAMLIT_JWT_SECRET`
3. Start platform:
   - `docker compose up -d --build`
4. Open:
   - UI: `http://localhost:8501`
   - API docs: `http://localhost:8000/docs`

### Local (without Docker)
```bash
poetry install
poetry run uvicorn app.api.main:app --reload
poetry run python -m app.scraper_worker
poetry run streamlit run app/ui/streamlit_app.py
```

### Security Notes
- Replace all default secrets from `.env.example`.
- Store real credentials only in `.env` (never commit it).
- Use strong hashed passwords in `app/ui/users.yaml`.
- Use network firewall rules for on-prem deployments.

---

## RU: Обзор

### Стек
- **Парсер**: асинхронный `httpx` + `asyncio` + `BeautifulSoup`
- **API**: `FastAPI`
- **UI**: `Streamlit` + `streamlit-authenticator` (мультипользовательская аутентификация с подписанной cookie-сессией/JWT-стилем)
- **Хранилище**: `MySQL 8`
- **Кэш/очередь состояний**: `Redis 7`
- **Менеджер зависимостей**: `Poetry`
- **Запуск**: Docker Compose

### Сервисы (`docker-compose.yml`)
- `mysql`
- `redis`
- `api` (FastAPI: `:8000`)
- `scraper` (фоновой async worker)
- `streamlit_ui` (`:8501`)

### API (реализовано)
- `GET /health`
- `GET /leads?limit=100`
- `GET /agencies?limit=100`
- `POST /trigger-scrape`

### Быстрый старт
1. Создайте `.env` из шаблона:
   - `cp .env.example .env` / `copy .env.example .env`
2. Обновите секреты:
   - `MYSQL_PASSWORD`, `MYSQL_ROOT_PASSWORD`
   - `STREAMLIT_COOKIE_KEY`, `STREAMLIT_JWT_SECRET`
3. Запуск:
   - `docker compose up -d --build`
4. Доступ:
   - UI: `http://localhost:8501`
   - Документация API: `http://localhost:8000/docs`

### Локальный запуск без Docker
```bash
poetry install
poetry run uvicorn app.api.main:app --reload
poetry run python -m app.scraper_worker
poetry run streamlit run app/ui/streamlit_app.py
```

### Важно по безопасности
- Не используйте значения по умолчанию из `.env.example` в проде.
- Не коммитьте `.env` в репозиторий.
- Используйте только сильные хешированные пароли в `app/ui/users.yaml`.
- Ограничьте доступ к сервисам через корпоративный firewall/VPN.
