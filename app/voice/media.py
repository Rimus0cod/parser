from __future__ import annotations

import asyncio
import base64
import json
import os
import queue
import threading
from typing import Any

from app.core.logging import get_logger

try:
    from google.cloud import speech
except ImportError:  # pragma: no cover - optional dependency path
    speech = None

logger = get_logger("voice.media")


class SpeechRecognitionManager:
    def __init__(self, session_store: Any, settings: Any) -> None:
        self._session_store = session_store
        self._settings = settings
        self._queues: dict[str, queue.Queue[bytes | None]] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._stream_to_call_sid: dict[str, str] = {}
        self._lock = threading.Lock()

        credentials_path = settings.google_application_credentials or os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS", ""
        )
        if credentials_path:
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", credentials_path)

    def ensure_stream(self, call_sid: str) -> None:
        with self._lock:
            if call_sid in self._queues:
                return
            audio_queue: queue.Queue[bytes | None] = queue.Queue()
            thread = threading.Thread(
                target=self._streaming_recognize,
                args=(call_sid, audio_queue),
                daemon=True,
            )
            self._queues[call_sid] = audio_queue
            self._threads[call_sid] = thread
            thread.start()

    def push_audio(self, call_sid: str, payload: bytes) -> None:
        self.ensure_stream(call_sid)
        self._queues[call_sid].put(payload)

    def stop_stream(self, call_sid: str) -> None:
        with self._lock:
            audio_queue = self._queues.pop(call_sid, None)
            self._threads.pop(call_sid, None)
            self._stream_to_call_sid = {
                stream_sid: known_call_sid
                for stream_sid, known_call_sid in self._stream_to_call_sid.items()
                if known_call_sid != call_sid
            }
        if audio_queue is not None:
            audio_queue.put(None)

    def register_stream(self, call_sid: str, stream_sid: str | None) -> None:
        if not stream_sid:
            return
        with self._lock:
            self._stream_to_call_sid[stream_sid] = call_sid

    def call_sid_for_stream(self, stream_sid: str | None) -> str | None:
        if not stream_sid:
            return None
        with self._lock:
            return self._stream_to_call_sid.get(stream_sid)

    def _streaming_recognize(self, call_sid: str, audio_queue: queue.Queue[bytes | None]) -> None:
        if speech is None:
            self._session_store.set_error(
                call_sid,
                "google-cloud-speech is not installed, media stream recognition is unavailable.",
            )
            return

        try:
            client = speech.SpeechClient()
            config = speech.RecognitionConfig(
                encoding=speech.RecognitionConfig.AudioEncoding.MULAW,
                sample_rate_hertz=8000,
                language_code="bg-BG",
                enable_automatic_punctuation=True,
            )
            streaming_config = speech.StreamingRecognitionConfig(
                config=config,
                interim_results=True,
                single_utterance=False,
            )

            def request_generator() -> Any:
                while True:
                    chunk = audio_queue.get()
                    if chunk is None:
                        break
                    yield speech.StreamingRecognizeRequest(audio_content=chunk)

            responses = client.streaming_recognize(streaming_config, request_generator())
            for response in responses:
                for result in response.results:
                    if not result.alternatives:
                        continue
                    text = result.alternatives[0].transcript.strip()
                    if result.is_final and text:
                        self._session_store.append_transcript(call_sid, text)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Speech recognition failed", call_sid=call_sid, error=str(exc))
            self._session_store.set_error(call_sid, str(exc))


async def handle_media_message(
    manager: SpeechRecognitionManager,
    session_store: Any,
    message: str,
) -> str | None:
    payload = json.loads(message)
    event = payload.get("event")

    if event == "start":
        start_payload = payload.get("start", {})
        call_sid = start_payload.get("callSid")
        stream_sid = start_payload.get("streamSid")
        custom_parameters = start_payload.get("customParameters") or {}
        if not call_sid:
            return None
        session_store.attach_stream(
            call_sid,
            stream_sid=stream_sid,
            custom_parameters=custom_parameters,
        )
        manager.register_stream(call_sid, stream_sid)
        manager.ensure_stream(call_sid)
        return call_sid

    if event == "media":
        media_payload = payload.get("media", {})
        call_sid = media_payload.get("callSid") or manager.call_sid_for_stream(payload.get("streamSid"))
        track = media_payload.get("track")
        if track and track != "inbound":
            return None
        if not call_sid:
            return None
        encoded_audio = media_payload.get("payload", "")
        if encoded_audio:
            manager.push_audio(call_sid, base64.b64decode(encoded_audio))
        return call_sid

    if event == "stop":
        stop_payload = payload.get("stop", {})
        call_sid = stop_payload.get("callSid") or manager.call_sid_for_stream(payload.get("streamSid"))
        if call_sid:
            manager.stop_stream(call_sid)
        return call_sid

    await asyncio.sleep(0)
    return None
