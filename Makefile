# =============================================================================
# Real Estate SaaS Core - Makefile
# =============================================================================
# Упрощённые команды для управления проектом
#
# Использование:
#   make help           - показать все доступные команды
#   make setup          - первоначальная настройка проекта
#   make up             - запустить все сервисы
#   make down           - остановить все сервисы
#   make logs           - показать логи всех сервисов
# =============================================================================

.PHONY: help
help: ## Показать это сообщение помощи
	@echo "==================================================================="
	@echo "Real Estate SaaS Core - Available Commands"
	@echo "==================================================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo "==================================================================="

.PHONY: setup
setup: ## Первоначальная настройка проекта
	@echo "🚀 Setting up Real Estate SaaS Core..."
	@if [ ! -f .env ]; then \
		echo "📝 Creating .env from .env.example..."; \
		cp .env.example .env; \
		echo "⚠️  Please edit .env and fill in your secrets!"; \
	else \
		echo "✅ .env already exists"; \
	fi
	@echo "🔐 Generating secure secrets..."
	@echo "MYSQL_PASSWORD=$$(openssl rand -hex 16)" >> .env.tmp
	@echo "MYSQL_ROOT_PASSWORD=$$(openssl rand -hex 16)" >> .env.tmp
	@echo "STREAMLIT_COOKIE_KEY=$$(openssl rand -hex 32)" >> .env.tmp
	@echo "STREAMLIT_JWT_SECRET=$$(openssl rand -hex 32)" >> .env.tmp
	@echo ""
	@echo "✅ Setup complete! Generated secrets saved to .env.tmp"
	@echo "📋 Copy these values to your .env file"
	@echo ""

.PHONY: secrets
secrets: ## Сгенерировать новые секретные ключи
	@echo "🔐 Generating new secrets..."
	@echo "MYSQL_PASSWORD=$$(openssl rand -hex 16)"
	@echo "MYSQL_ROOT_PASSWORD=$$(openssl rand -hex 16)"
	@echo "STREAMLIT_COOKIE_KEY=$$(openssl rand -hex 32)"
	@echo "STREAMLIT_JWT_SECRET=$$(openssl rand -hex 32)"

.PHONY: install
install: ## Установить зависимости через Poetry
	@echo "📦 Installing dependencies with Poetry..."
	poetry install

.PHONY: install-dev
install-dev: ## Установить зависимости включая dev
	@echo "📦 Installing all dependencies (including dev)..."
	poetry install --with dev

.PHONY: update
update: ## Обновить зависимости
	@echo "🔄 Updating dependencies..."
	poetry update

.PHONY: build
build: ## Собрать Docker образы
	@echo "🏗️  Building Docker images..."
	docker compose build

.PHONY: up
up: ## Запустить все сервисы
	@echo "🚀 Starting all services..."
	docker compose up -d
	@echo "✅ Services started!"
	@echo "📊 Dashboard: http://localhost:8501"
	@echo "🔌 API: http://localhost:8000/docs"

.PHONY: up-build
up-build: ## Пересобрать и запустить все сервисы
	@echo "🏗️  Building and starting all services..."
	docker compose up -d --build
	@echo "✅ Services started!"
	@echo "📊 Dashboard: http://localhost:8501"
	@echo "🔌 API: http://localhost:8000/docs"

.PHONY: down
down: ## Остановить все сервисы
	@echo "🛑 Stopping all services..."
	docker compose down

.PHONY: down-volumes
down-volumes: ## Остановить сервисы и удалить volumes (⚠️ удалит данные!)
	@echo "⚠️  WARNING: This will delete all data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker compose down -v; \
		echo "✅ Services stopped and volumes removed"; \
	else \
		echo "❌ Cancelled"; \
	fi

.PHONY: restart
restart: ## Перезапустить все сервисы
	@echo "🔄 Restarting all services..."
	docker compose restart

.PHONY: restart-scraper
restart-scraper: ## Перезапустить только scraper worker
	@echo "🔄 Restarting scraper worker..."
	docker compose restart scraper

.PHONY: restart-api
restart-api: ## Перезапустить только API
	@echo "🔄 Restarting API..."
	docker compose restart api

.PHONY: restart-ui
restart-ui: ## Перезапустить только Streamlit UI
	@echo "🔄 Restarting Streamlit UI..."
	docker compose restart streamlit_ui

.PHONY: logs
logs: ## Показать логи всех сервисов
	docker compose logs -f

.PHONY: logs-scraper
logs-scraper: ## Показать логи scraper worker
	docker compose logs -f scraper

.PHONY: logs-api
logs-api: ## Показать логи API
	docker compose logs -f api

.PHONY: logs-ui
logs-ui: ## Показать логи Streamlit UI
	docker compose logs -f streamlit_ui

.PHONY: logs-mysql
logs-mysql: ## Показать логи MySQL
	docker compose logs -f mysql

.PHONY: logs-redis
logs-redis: ## Показать логи Redis
	docker compose logs -f redis

.PHONY: ps
ps: ## Показать статус всех сервисов
	docker compose ps

.PHONY: shell-api
shell-api: ## Открыть shell в контейнере API
	docker compose exec api /bin/bash

.PHONY: shell-scraper
shell-scraper: ## Открыть shell в контейнере scraper
	docker compose exec scraper /bin/bash

.PHONY: shell-mysql
shell-mysql: ## Открыть MySQL shell
	docker compose exec mysql mysql -u root -p

.PHONY: shell-redis
shell-redis: ## Открыть Redis CLI
	docker compose exec redis redis-cli

.PHONY: lint
lint: ## Проверить код с помощью ruff
	@echo "🔍 Linting code..."
	poetry run ruff check app/ integrations/ utils.py

.PHONY: lint-fix
lint-fix: ## Автоматически исправить проблемы линтера
	@echo "🔧 Fixing linting issues..."
	poetry run ruff check --fix app/ integrations/ utils.py

.PHONY: format
format: ## Форматировать код с помощью ruff
	@echo "✨ Formatting code..."
	poetry run ruff format app/ integrations/ utils.py

.PHONY: type-check
type-check: ## Проверить типы с помощью mypy
	@echo "🔍 Type checking..."
	poetry run mypy app/ integrations/

.PHONY: check
check: lint type-check ## Запустить все проверки (lint + type-check)

.PHONY: clean
clean: ## Очистить временные файлы
	@echo "🧹 Cleaning temporary files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf htmlcov/ .coverage dist/ handoff/ parser.log 2>/dev/null || true
	@echo "✅ Cleanup complete"

.PHONY: db-backup
db-backup: ## Создать backup базы данных MySQL
	@echo "💾 Creating database backup..."
	@mkdir -p backups
	docker compose exec -T mysql mysqldump -u root -p$${MYSQL_ROOT_PASSWORD} imoti > backups/backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "✅ Backup created in backups/"

.PHONY: db-restore
db-restore: ## Восстановить базу данных из backup (использование: make db-restore FILE=backup.sql)
	@if [ -z "$(FILE)" ]; then \
		echo "❌ Error: Please specify FILE=backup.sql"; \
		exit 1; \
	fi
	@echo "📥 Restoring database from $(FILE)..."
	docker compose exec -T mysql mysql -u root -p$${MYSQL_ROOT_PASSWORD} imoti < $(FILE)
	@echo "✅ Database restored"

.PHONY: db-reset
db-reset: ## Сбросить базу данных (⚠️ удалит все данные!)
	@echo "⚠️  WARNING: This will delete all database data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker compose exec mysql mysql -u root -p$${MYSQL_ROOT_PASSWORD} -e "DROP DATABASE IF EXISTS imoti; CREATE DATABASE imoti;"; \
		echo "✅ Database reset complete"; \
	else \
		echo "❌ Cancelled"; \
	fi

.PHONY: trigger-scrape
trigger-scrape: ## Запустить scraping вручную через API
	@echo "🔄 Triggering manual scrape..."
	curl -X POST http://localhost:8000/trigger-scrape
	@echo ""

.PHONY: api-health
api-health: ## Проверить health API
	@echo "🏥 Checking API health..."
	curl http://localhost:8000/health
	@echo ""

.PHONY: api-docs
api-docs: ## Открыть API документацию в браузере
	@echo "📖 Opening API documentation..."
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8000/docs || \
	command -v open >/dev/null 2>&1 && open http://localhost:8000/docs || \
	echo "Please open http://localhost:8000/docs in your browser"

.PHONY: dashboard
dashboard: ## Открыть dashboard в браузере
	@echo "📊 Opening dashboard..."
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8501 || \
	command -v open >/dev/null 2>&1 && open http://localhost:8501 || \
	echo "Please open http://localhost:8501 in your browser"

.PHONY: dev-api
dev-api: ## Запустить API локально (без Docker)
	@echo "🚀 Starting API in development mode..."
	poetry run uvicorn app.api.main:app --reload --host 0.0.0.0 --port 8000

.PHONY: dev-scraper
dev-scraper: ## Запустить scraper локально (без Docker)
	@echo "🚀 Starting scraper in development mode..."
	poetry run python -m app.scraper_worker

.PHONY: dev-ui
dev-ui: ## Запустить Streamlit UI локально (без Docker)
	@echo "🚀 Starting Streamlit UI in development mode..."
	poetry run streamlit run app/ui/streamlit_app.py

.PHONY: prod-deploy
prod-deploy: ## Развернуть в production режиме
	@echo "🚀 Deploying to production..."
	@if [ ! -f .env ]; then \
		echo "❌ Error: .env file not found!"; \
		exit 1; \
	fi
	docker compose -f docker-compose.yml up -d --build
	@echo "✅ Production deployment complete!"

.PHONY: monitor
monitor: ## Показать мониторинг ресурсов контейнеров
	docker stats

.PHONY: prune
prune: ## Очистить неиспользуемые Docker ресурсы
	@echo "🧹 Pruning unused Docker resources..."
	docker system prune -f
	@echo "✅ Prune complete"

.PHONY: version
version: ## Показать версии всех компонентов
	@echo "==================================================================="
	@echo "Real Estate SaaS Core - Version Information"
	@echo "==================================================================="
	@echo "Python version:"
	@python --version 2>/dev/null || echo "  Not installed"
	@echo ""
	@echo "Poetry version:"
	@poetry --version 2>/dev/null || echo "  Not installed"
	@echo ""
	@echo "Docker version:"
	@docker --version 2>/dev/null || echo "  Not installed"
	@echo ""
	@echo "Docker Compose version:"
	@docker compose version 2>/dev/null || echo "  Not installed"
	@echo "==================================================================="

# Default target
.DEFAULT_GOAL := help
