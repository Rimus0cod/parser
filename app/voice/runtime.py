from __future__ import annotations

from redis import Redis

from app.core.config import get_settings
from app.core.logging import get_logger
from app.voice.media import SpeechRecognitionManager
from app.voice.prompts import ensure_prompt_assets
from app.voice.service import VoiceService
from app.voice.session import VoiceSessionStore

logger = get_logger("voice.runtime")

_redis_client: Redis | None = None
_session_store: VoiceSessionStore | None = None
_voice_service: VoiceService | None = None
_speech_manager: SpeechRecognitionManager | None = None


def get_voice_session_store() -> VoiceSessionStore:
    global _redis_client, _session_store

    if _session_store is None:
        settings = get_settings()
        if _redis_client is None:
            _redis_client = Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                decode_responses=True,
            )
        _session_store = VoiceSessionStore(_redis_client)
    return _session_store


def get_voice_service() -> VoiceService:
    global _voice_service

    if _voice_service is None:
        _voice_service = VoiceService(get_voice_session_store())
    return _voice_service


def get_speech_recognition_manager() -> SpeechRecognitionManager:
    global _speech_manager

    if _speech_manager is None:
        _speech_manager = SpeechRecognitionManager(get_voice_session_store(), get_settings())
    return _speech_manager


def prepare_voice_runtime() -> None:
    settings = get_settings()
    if not settings.voice_enabled:
        return
    ensure_prompt_assets(settings, logger)
