from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Iterable

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from config import Config, load_config
from mysql_store import MySQLStore
from scraper import ParserRunResult, _setup_logging, run_parser_once


def _parse_allowed_chat_ids(cfg: Config) -> set[str]:
    out: set[str] = set()
    if cfg.telegram_chat_id:
        out.add(str(cfg.telegram_chat_id).strip())
    raw = (cfg.telegram_allowed_chat_ids or "").strip()
    if not raw:
        return out
    for token in raw.split(","):
        chat_id = token.strip()
        if chat_id:
            out.add(chat_id)
    return out


def _format_listing_line(row: dict) -> str:
    return (
        f"{row.get('date_seen') or '-'} | {row.get('location') or '-'} | "
        f"{row.get('title') or '-'} | {row.get('price') or '-'} | "
        f"{row.get('phone') or '-'} | {row.get('link') or '-'}"
    )


def _chunked(items: list[str], chunk_size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _week_bounds(week_offset: int = 0) -> tuple[date, date]:
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_start = week_start + timedelta(days=7 * week_offset)
    week_end = week_start + timedelta(days=6)
    return week_start, week_end


def _parse_history_week_args(args: list[str]) -> tuple[str | None, int]:
    city: str | None = None
    week_offset = 0
    if not args:
        return city, week_offset
    if len(args) == 1:
        try:
            week_offset = int(args[0])
        except ValueError:
            city = args[0]
        return city, week_offset
    city = args[0]
    try:
        week_offset = int(args[1])
    except ValueError:
        week_offset = 0
    return city, week_offset


async def _send_history(
    update: Update,
    rows: list[dict],
    cfg: Config,
    title: str,
) -> None:
    if not update.effective_chat:
        return
    if not rows:
        await update.effective_chat.send_message(f"{title}\nНічого не знайдено.")
        return

    lines = [_format_listing_line(r) for r in rows]
    chunk_size = max(1, cfg.telegram_history_chunk_size)
    await update.effective_chat.send_message(f"{title}\nЗнайдено: {len(rows)}")
    for chunk in _chunked(lines, chunk_size):
        await update.effective_chat.send_message("\n".join(chunk), disable_web_page_preview=True)


def _get_store(cfg: Config) -> MySQLStore | None:
    if not cfg.mysql_enabled:
        return None
    store = MySQLStore(cfg)

    return store


def _summarize_run(result: ParserRunResult, preview_count: int) -> str:
    header = (
        f"Парсер завершено.\n"
        f"Дата: {result.today}\n"
        f"Усього зібрано: {result.total_scraped}\n"
        f"Нових: {result.new_count}"
    )
    if result.exit_code != 0:
        return f"Помилка запуску парсера: {result.message}"
    listings = result.new_listings or []
    if not listings:
        return f"{header}\nНових оголошень немає."
    preview = listings[: max(1, preview_count)]
    preview_lines = [
        f"{idx}. {lst.title} | {lst.location} | {lst.price} | {lst.link}"
        for idx, lst in enumerate(preview, start=1)
    ]
    return f"{header}\n\nПерші {len(preview_lines)} результати:\n" + "\n".join(preview_lines)


async def _run_parser_and_send(chat_id: str, app: Application, cfg: Config) -> None:
    result = await asyncio.to_thread(run_parser_once, cfg)
    text = _summarize_run(result, cfg.telegram_startup_preview_count)
    await app.bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)


def _is_chat_allowed(update: Update, allowed_chats: set[str]) -> bool:
    chat = update.effective_chat
    return bool(chat and str(chat.id) in allowed_chats)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    allowed_chats: set[str] = context.application.bot_data["allowed_chats"]
    if not _is_chat_allowed(update, allowed_chats):
        return
    help_text = (
        "Доступні команди:\n"
        "/run - запустити парсер вручну\n"
        "/history_city <місто> - повна історія по місту\n"
        "/history_days <дні> [місто] - історія за N днів\n"
        "/history_week [місто] [зсув_тижня] - історія за тиждень (0 поточний, -1 попередній)\n"
    )
    await update.effective_chat.send_message(help_text)


async def run_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    allowed_chats: set[str] = context.application.bot_data["allowed_chats"]
    run_lock: asyncio.Lock = context.application.bot_data["run_lock"]
    if not _is_chat_allowed(update, allowed_chats):
        return
    if run_lock.locked():
        await update.effective_chat.send_message("Парсер вже виконується. Спробуйте трохи пізніше.")
        return
    async with run_lock:
        await update.effective_chat.send_message("Запускаю парсер...")
        result = await asyncio.to_thread(run_parser_once, cfg)
        await update.effective_chat.send_message(
            _summarize_run(result, cfg.telegram_startup_preview_count),
            disable_web_page_preview=True,
        )


async def history_city_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    allowed_chats: set[str] = context.application.bot_data["allowed_chats"]
    if not _is_chat_allowed(update, allowed_chats):
        return
    if not context.args:
        await update.effective_chat.send_message("Використання: /history_city <місто>")
        return
    city = " ".join(context.args).strip()
    store = _get_store(cfg)
    if store is None:
        await update.effective_chat.send_message("MySQL вимкнено. Увімкніть MYSQL_ENABLED=true.")
        return
    try:
        rows = store.query_listings(city=city)
    finally:
        store.close()
    await _send_history(update, rows, cfg, f"Історія для міста: {city}")


async def history_days_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    allowed_chats: set[str] = context.application.bot_data["allowed_chats"]
    if not _is_chat_allowed(update, allowed_chats):
        return
    if not context.args:
        await update.effective_chat.send_message("Використання: /history_days <дні> [місто]")
        return
    try:
        days = int(context.args[0])
    except ValueError:
        await update.effective_chat.send_message("Параметр <дні> має бути числом.")
        return
    city = " ".join(context.args[1:]).strip() if len(context.args) > 1 else None
    days = max(1, days)
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    store = _get_store(cfg)
    if store is None:
        await update.effective_chat.send_message("MySQL вимкнено. Увімкніть MYSQL_ENABLED=true.")
        return
    try:
        rows = store.query_listings(city=city, start_date=start_date, end_date=end_date)
    finally:
        store.close()
    title = f"Історія за останні {days} днів"
    if city:
        title += f" (місто: {city})"
    await _send_history(update, rows, cfg, title)


async def history_week_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg: Config = context.application.bot_data["cfg"]
    allowed_chats: set[str] = context.application.bot_data["allowed_chats"]
    if not _is_chat_allowed(update, allowed_chats):
        return
    city, week_offset = _parse_history_week_args(context.args)
    week_start, week_end = _week_bounds(week_offset)
    store = _get_store(cfg)
    if store is None:
        await update.effective_chat.send_message("MySQL вимкнено. Увімкніть MYSQL_ENABLED=true.")
        return
    try:
        rows = store.query_listings(city=city, start_date=week_start, end_date=week_end)
    finally:
        store.close()
    title = f"Історія за тиждень {week_start} .. {week_end}"
    if city:
        title += f" (місто: {city})"
    await _send_history(update, rows, cfg, title)


async def on_startup(app: Application) -> None:
    cfg: Config = app.bot_data["cfg"]
    run_lock: asyncio.Lock = app.bot_data["run_lock"]
    if not cfg.telegram_chat_id:
        return
    if run_lock.locked():
        return
    async with run_lock:
        await _run_parser_and_send(cfg.telegram_chat_id, app, cfg)


def main() -> None:
    cfg = load_config()
    _setup_logging(cfg)
    if not cfg.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required to run telegram_bot.py")

    allowed_chats = _parse_allowed_chat_ids(cfg)
    app = Application.builder().token(cfg.telegram_bot_token).post_init(on_startup).build()
    app.bot_data["cfg"] = cfg
    app.bot_data["allowed_chats"] = allowed_chats
    app.bot_data["run_lock"] = asyncio.Lock()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", start_command))
    app.add_handler(CommandHandler("run", run_command))
    app.add_handler(CommandHandler("history_city", history_city_command))
    app.add_handler(CommandHandler("history_days", history_days_command))
    app.add_handler(CommandHandler("history_week", history_week_command))

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
