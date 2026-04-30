from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from app.voice.extractors import QUESTION_SEQUENCE

try:
    from google.cloud import texttospeech
except ImportError:  # pragma: no cover - optional dependency path
    texttospeech = None  # type: ignore[assignment]


SCRIPT_NAME = "bg_listing_v1"
INTRO_AUDIO_FILENAME = "intro.mp3"
THANKS_AUDIO_FILENAME = "thanks.mp3"
STREAM_NAME = "listing_voice_stream"
PROMPT_CACHE_DIR = Path(__file__).resolve().parents[2] / "tts_cache"

PROMPT_MANIFEST: dict[str, str] = {
    INTRO_AUDIO_FILENAME: (
        "Здравейте, аз съм AI асистент и се интересувам от апартамента ви. "
        "Можете ли да ми кажете повече детайли?"
    ),
    THANKS_AUDIO_FILENAME: "Благодаря ви! Това е всичко от мен. Приятен ден.",
    **{question.audio_file: question.text for question in QUESTION_SEQUENCE},
}


def ensure_prompt_directory() -> Path:
    PROMPT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return PROMPT_CACHE_DIR


def prompt_path(filename: str) -> Path:
    return ensure_prompt_directory() / Path(filename).name


def prompt_public_url(base_url: str, filename: str) -> str:
    return f"{base_url.rstrip('/')}/voice/tts/{Path(filename).name}"


def ensure_prompt_assets(settings: Any, logger: Any) -> None:
    cache_dir = ensure_prompt_directory()
    missing = [filename for filename in PROMPT_MANIFEST if not (cache_dir / filename).exists()]
    if not missing:
        return

    credentials_path = settings.google_application_credentials or os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", ""
    )
    if credentials_path:
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", credentials_path)

    if texttospeech is None:
        logger.warning(
            "Voice prompt generation skipped because google-cloud-texttospeech is not installed.",
            missing_files=missing,
        )
        return

    if not credentials_path:
        logger.warning(
            "Voice prompt generation skipped because GOOGLE_APPLICATION_CREDENTIALS is not configured.",
            missing_files=missing,
        )
        return

    client = texttospeech.TextToSpeechClient()
    voice = texttospeech.VoiceSelectionParams(language_code="bg-BG", name="bg-BG-Standard-A")
    audio_config = texttospeech.AudioConfig(audio_encoding=texttospeech.AudioEncoding.MP3)

    for filename in missing:
        synthesis_input = texttospeech.SynthesisInput(text=PROMPT_MANIFEST[filename])
        response = client.synthesize_speech(
            input=synthesis_input,
            voice=voice,
            audio_config=audio_config,
        )
        prompt_path(filename).write_bytes(response.audio_content)
        logger.info("Generated missing voice prompt", filename=filename)
