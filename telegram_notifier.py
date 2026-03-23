import logging
import requests

logger = logging.getLogger("telegram_notifier")

def send_telegram_lead(config, listing) -> bool:
    """Send a private listing lead to a Telegram chat."""
    if not config.telegram_bot_token or not config.telegram_chat_id:
        return False
        
    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
    
    text = (
        f"🚨 <b>Новий приватний лід!</b>\n\n"
        f"🏠 <b>{listing.title}</b>\n"
        f"💰 {listing.price}\n"
        f"📍 {listing.location}\n"
        f"📏 {listing.size}\n\n"
        f"📞 <b>{listing.phone}</b> ({listing.seller_name})\n\n"
        f"🔗 <a href='{listing.link}'>Переглянути оголошення</a>"
    )
    
    payload = {
        "chat_id": config.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        # Avoid crashing if the bot is blocked/not configured right
        if not response.ok:
            logger.error("Telegram API error: %s", response.text)
            return False
        logger.info("Successfully sent lead %s to Telegram.", listing.ad_id)
        return True
    except Exception as e:
        logger.error("Failed to send lead %s to Telegram: %s", listing.ad_id, e)
        return False
