from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response, WebSocket
from fastapi.responses import FileResponse

from app.core.config import get_settings
from app.core.logging import get_logger
from app.models.schemas import (
    TenantContact,
    TenantContactImportResponse,
    VoiceCall,
    VoiceCallCreateRequest,
)
from app.services import repository
from app.voice.extractors import extract_answers, next_question
from app.voice.media import handle_media_message
from app.voice.prompts import (
    INTRO_AUDIO_FILENAME,
    PROMPT_CACHE_DIR,
    THANKS_AUDIO_FILENAME,
    prompt_public_url,
)
from app.voice.runtime import (
    get_speech_recognition_manager,
    get_voice_service,
    get_voice_session_store,
)
from app.voice.service import TERMINAL_CALL_STATUSES, parse_tenant_contacts_csv
from app.voice.twiml import build_goodbye_twiml, build_question_twiml, build_start_twiml

logger = get_logger("voice.router")
router = APIRouter(tags=["voice"])


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


async def _twilio_params(request: Request) -> tuple[dict[str, str], str]:
    body = await request.body()
    params = (
        {key: value for key, value in parse_qsl(body.decode("utf-8"), keep_blank_values=True)}
        if body
        else {}
    )
    if not params:
        params = {key: value for key, value in request.query_params.items()}
    signature = request.headers.get("X-Twilio-Signature", "")
    return params, signature


async def _validate_twilio_request(request: Request) -> bool:
    params, signature = await _twilio_params(request)
    settings = get_settings()
    query_string = request.url.query
    base_url = settings.voice_public_base_url.rstrip("/")
    validation_url = f"{base_url}{request.url.path}"
    if query_string:
        validation_url = f"{validation_url}?{query_string}"
    return get_voice_service().validate_twilio_request(validation_url, params, signature)


def _voice_http_url(path: str) -> str:
    return f"{get_settings().voice_public_base_url.rstrip('/')}{path}"


def _voice_ws_url() -> str:
    return get_settings().voice_ws_public_url


@router.get("/voice/tts/{filename}")
async def serve_voice_prompt(filename: str) -> FileResponse:
    safe_name = Path(filename).name
    target = PROMPT_CACHE_DIR / safe_name
    if not target.exists():
        raise HTTPException(status_code=404, detail="Prompt audio file was not found.")
    return FileResponse(target, media_type="audio/mpeg")


@router.post("/voice/calls", response_model=VoiceCall)
async def create_voice_call(payload: VoiceCallCreateRequest) -> VoiceCall:
    service = get_voice_service()
    try:
        call = await service.start_listing_call(
            listing_ad_id=payload.listing_ad_id,
            initiated_by=payload.initiated_by,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return VoiceCall.model_validate(call)


@router.get("/voice/calls", response_model=list[VoiceCall])
async def get_voice_calls(limit: int = Query(default=100, ge=1, le=1000)) -> list[VoiceCall]:
    rows = await repository.list_voice_calls(limit=limit)
    return [VoiceCall.model_validate(row) for row in rows]


@router.get("/voice/calls/{voice_call_id}", response_model=VoiceCall)
async def get_voice_call(voice_call_id: int) -> VoiceCall:
    row = await repository.get_voice_call(voice_call_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Voice call was not found.")
    return VoiceCall.model_validate(row)


@router.get("/tenant-contacts", response_model=list[TenantContact])
async def get_tenant_contacts(limit: int = Query(default=100, ge=1, le=1000)) -> list[TenantContact]:
    rows = await repository.list_tenant_contacts(limit=limit)
    return [TenantContact.model_validate(row) for row in rows]


@router.post("/tenant-contacts/import", response_model=TenantContactImportResponse)
async def import_tenant_contacts(
    request: Request,
    filename: str = Query(default="upload.csv"),
    x_filename: str | None = Header(default=None, alias="X-Filename"),
) -> TenantContactImportResponse:
    content = await request.body()
    rows = parse_tenant_contacts_csv(content, filename=x_filename or filename)
    imported = await repository.upsert_tenant_contacts(rows)
    return TenantContactImportResponse(imported=imported, skipped=max(0, len(rows) - imported))


@router.api_route("/voice/twiml/start", methods=["GET", "POST"])
async def voice_twiml_start(request: Request, voice_call_id: int = Query(..., ge=1)) -> Response:
    if not await _validate_twilio_request(request):
        return Response("Invalid signature", status_code=403)

    params, _ = await _twilio_params(request)
    call_sid = params.get("CallSid", "")
    if not call_sid:
        raise HTTPException(status_code=400, detail="Twilio CallSid is required.")

    service = get_voice_service()
    try:
        await service.bootstrap_session(voice_call_id=voice_call_id, call_sid=call_sid)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    xml = build_start_twiml(
        ws_url=_voice_ws_url(),
        intro_url=prompt_public_url(get_settings().voice_public_base_url, INTRO_AUDIO_FILENAME),
        next_url=f"{_voice_http_url('/voice/twiml/next')}?call_sid={call_sid}",
        custom_parameters={"voice_call_id": str(voice_call_id), "call_sid": call_sid},
    )
    return Response(content=xml, media_type="application/xml")


@router.api_route("/voice/twiml/next", methods=["GET", "POST"])
async def voice_twiml_next(request: Request, call_sid: str = Query(...)) -> Response:
    if not await _validate_twilio_request(request):
        return Response("Invalid signature", status_code=403)

    session_store = get_voice_session_store()
    state, new_text = session_store.consume_pending_transcript(call_sid)
    if state is None:
        xml = build_goodbye_twiml(
            thanks_url=prompt_public_url(get_settings().voice_public_base_url, THANKS_AUDIO_FILENAME)
        )
        return Response(content=xml, media_type="application/xml")

    answers = dict(state.get("answers", {}))
    if new_text:
        answers.update(extract_answers(new_text, answers, state.get("current_question")))
        state["answers"] = answers

    question_attempts = dict(state.get("question_attempts", {}))
    question = next_question(answers, question_attempts)

    if question is None:
        state["current_question"] = None
        session_store.save_session(state)
        xml = build_goodbye_twiml(
            thanks_url=prompt_public_url(get_settings().voice_public_base_url, THANKS_AUDIO_FILENAME)
        )
        return Response(content=xml, media_type="application/xml")

    question_attempts[question.key] = question_attempts.get(question.key, 0) + 1
    state["question_attempts"] = question_attempts
    state["current_question"] = question.key
    state.setdefault("asked_questions", []).append(question.key)
    session_store.save_session(state)

    xml = build_question_twiml(
        audio_url=prompt_public_url(get_settings().voice_public_base_url, question.audio_file),
        next_url=f"{_voice_http_url('/voice/twiml/next')}?call_sid={call_sid}",
    )
    return Response(content=xml, media_type="application/xml")


@router.post("/voice/twilio/status")
async def voice_status_callback(request: Request, voice_call_id: int | None = Query(default=None)) -> Response:
    if not await _validate_twilio_request(request):
        return Response("Invalid signature", status_code=403)

    params, _ = await _twilio_params(request)
    call_sid = params.get("CallSid", "")
    call_status = (params.get("CallStatus", "") or "").lower()
    if not call_sid:
        return Response(status_code=204)

    patch_kwargs: dict[str, Any] = {"status": call_status or "unknown"}
    if call_status == "in-progress":
        patch_kwargs["answered_at"] = _utcnow()
        get_voice_session_store().mark_answered(call_sid)
    elif call_status in TERMINAL_CALL_STATUSES:
        patch_kwargs["completed_at"] = _utcnow()
        get_voice_session_store().mark_completed(call_sid, call_status)
    else:
        get_voice_session_store().set_status(call_sid, call_status or "unknown")

    if voice_call_id is not None:
        await repository.patch_voice_call(voice_call_id=voice_call_id, **patch_kwargs)
    else:
        await repository.patch_voice_call(twilio_call_sid=call_sid, **patch_kwargs)

    if call_status in TERMINAL_CALL_STATUSES:
        await get_voice_service().persist_session_snapshot(call_sid, status=call_status)

    return Response(status_code=204)


@router.post("/voice/twilio/recording")
async def voice_recording_callback(
    request: Request,
    voice_call_id: int | None = Query(default=None),
) -> Response:
    if not await _validate_twilio_request(request):
        return Response("Invalid signature", status_code=403)

    params, _ = await _twilio_params(request)
    call_sid = params.get("CallSid", "")
    recording_url = params.get("RecordingUrl", "")
    if not call_sid or not recording_url:
        return Response(status_code=204)

    get_voice_session_store().set_recording_url(call_sid, recording_url)
    if voice_call_id is not None:
        await repository.patch_voice_call(voice_call_id=voice_call_id, recording_url=recording_url)
    else:
        await repository.patch_voice_call(twilio_call_sid=call_sid, recording_url=recording_url)
    await get_voice_service().persist_session_snapshot(call_sid)
    return Response(status_code=204)


@router.websocket("/voice/media")
async def voice_media_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    manager = get_speech_recognition_manager()
    session_store = get_voice_session_store()

    try:
        while True:
            message = await websocket.receive_text()
            await handle_media_message(manager, session_store, message)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Voice media websocket closed", error=str(exc))
    finally:
        try:
            await websocket.close()
        except Exception:  # noqa: BLE001
            pass
