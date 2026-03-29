import os
import re
import json
import uuid
import time
import base64
import queue
import asyncio
import logging
import threading
from datetime import datetime
from urllib.parse import urlparse, parse_qs

from flask import Flask, request, Response, send_from_directory
from twilio.rest import Client
from twilio.twiml.voice_response import VoiceResponse, Connect
from twilio.request_validator import RequestValidator
from google.cloud import speech, texttospeech
import mysql.connector
import websockets

# ----------------------------
# Configuration and logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("apartment_agent")

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "+17154488602")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://apogamously-inspirable-kit.ngrok-free.dev")  # https://your-domain.com
WS_PUBLIC_URL = os.getenv("WS_PUBLIC_URL", "ws://localhost:9001")      # wss://your-domain.com/media
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "1")
MYSQL_DB = os.getenv("imoti", "listings")
TWILIO_VALIDATE = os.getenv("TWILIO_VALIDATE", "true").lower() == "true"

TTS_CACHE_DIR = "tts_cache"
WS_BIND_HOST = "0.0.0.0"
WS_BIND_PORT = int(os.getenv("WS_BIND_PORT", "9001"))
HTTP_BIND_HOST = "0.0.0.0"
HTTP_BIND_PORT = int(os.getenv("HTTP_BIND_PORT", "8000"))

# ----------------------------
# In-memory state
# ----------------------------
state_lock = threading.Lock()
pending_requests = {} 
call_states = {}     
audio_queues = {}       


app = Flask(__name__)


def env_required(name, value):
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")

def ensure_tts_cache():
    if not os.path.isdir(TTS_CACHE_DIR):
        os.makedirs(TTS_CACHE_DIR, exist_ok=True)

def verify_twilio_request():
    if not TWILIO_VALIDATE:
        return True
    validator = RequestValidator(TWILIO_AUTH_TOKEN)
    url = request.url
    params = request.form.to_dict() if request.form else request.args.to_dict()
    signature = request.headers.get("X-Twilio-Signature", "")
    return validator.validate(url, params, signature)

def mysql_connect():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
    )

def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS apartment_inquiries (
            id INT AUTO_INCREMENT PRIMARY KEY,
            phone_number VARCHAR(32) NOT NULL,
            name VARCHAR(128),
            apartment_details TEXT,
            conversation_transcript TEXT,
            timestamp DATETIME
        )
    """)
    conn.commit()
    cur.close()

def store_inquiry(call_sid):
    with state_lock:
        state = call_states.get(call_sid)
        if not state:
            return
        transcript = "\n".join(state.get("transcript_chunks", []))
        if state.get("recording_url"):
            transcript += f"\n[Recording] {state['recording_url']}"
        apartment_details = json.dumps(state.get("apartment_details", {}), ensure_ascii=False)
        name = state.get("name")
        phone_number = state.get("phone_number")
    try:
        conn = mysql_connect()
        ensure_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO apartment_inquiries
            (phone_number, name, apartment_details, conversation_transcript, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (phone_number, name, apartment_details, transcript, datetime.utcnow())
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Stored inquiry for call_sid=%s", call_sid)
    except Exception:
        logger.exception("Failed to store inquiry for call_sid=%s", call_sid)

def init_call_state(call_sid, phone_number, name, apartment_details):
    with state_lock:
        call_states[call_sid] = {
            "call_sid": call_sid,
            "phone_number": phone_number,
            "name": name,
            "apartment_details": apartment_details or {},
            "transcript_chunks": [],
            "last_processed_index": 0,
            "answers": {},
            "asked_followup": False,
            "recording_url": None,
            "current_question": None,
        }

def update_transcript(call_sid, text):
    with state_lock:
        state = call_states.get(call_sid)
        if not state:
            return
        state["transcript_chunks"].append(text)

def append_recording_url(call_sid, url):
    with state_lock:
        state = call_states.get(call_sid)
        if state:
            state["recording_url"] = url

def detect_price(text):
    match = re.search(r"(\d[\d\s]{2,6})\s*(лв|лева|eur|евро)", text, re.IGNORECASE)
    return match.group(0) if match else None

def detect_size(text):
    match = re.search(r"(\d{1,4})\s*(кв\.?м|квм|m2)", text, re.IGNORECASE)
    return match.group(0) if match else None

def detect_location(text):
    if any(k in text.lower() for k in ["квартал", "улица", "адрес", "район", "център"]):
        return text.strip()
    return None

def detect_condition(text):
    if any(k in text.lower() for k in ["ремонт", "обзаведен", "ново", "старо", "състояние"]):
        return text.strip()
    return None

def detect_availability(text):
    if any(k in text.lower() for k in ["свободен", "наличен", "дата", "от", "сега"]):
        return text.strip()
    return None

QUESTIONS = [
    ("price", "Каква е цената на апартамента?"),
    ("size", "Каква е квадратурата?"),
    ("location", "Къде точно се намира?"),
    ("condition", "Какво е състоянието и обзавеждането?"),
    ("availability", "От кога е наличен за наем или продажба?")
]
FOLLOWUP_PRICE = "Има ли възможност за договаряне на цената?"

def next_question(call_sid):
    with state_lock:
        state = call_states.get(call_sid)
        if not state:
            return None

        new_chunks = state["transcript_chunks"][state["last_processed_index"]:]
        state["last_processed_index"] = len(state["transcript_chunks"])
        new_text = " ".join(new_chunks)

        if new_text:
            if "price" not in state["answers"]:
                price = detect_price(new_text)
                if price:
                    state["answers"]["price"] = price
            if "size" not in state["answers"]:
                size = detect_size(new_text)
                if size:
                    state["answers"]["size"] = size
            if "location" not in state["answers"]:
                loc = detect_location(new_text)
                if loc:
                    state["answers"]["location"] = loc
            if "condition" not in state["answers"]:
                cond = detect_condition(new_text)
                if cond:
                    state["answers"]["condition"] = cond
            if "availability" not in state["answers"]:
                av = detect_availability(new_text)
                if av:
                    state["answers"]["availability"] = av

        if "price" in state["answers"] and not state["asked_followup"]:
            state["asked_followup"] = True
            return ("negotiation", FOLLOWUP_PRICE)

        for key, text in QUESTIONS:
            if key not in state["answers"]:
                return (key, text)

        return None

def synthesize_tts(text, filename):
    ensure_tts_cache()
    path = os.path.join(TTS_CACHE_DIR, filename)
    if os.path.exists(path):
        return path

    client = texttospeech.TextToSpeechClient()
    synthesis_input = texttospeech.SynthesisInput(text=text)
    voice = texttospeech.VoiceSelectionParams(
        language_code="bg-BG",
        name="bg-BG-Standard-A"
    )
    audio_config = texttospeech.AudioConfig(
        audio_encoding=texttospeech.AudioEncoding.MP3
    )
    response = client.synthesize_speech(
        input=synthesis_input,
        voice=voice,
        audio_config=audio_config
    )
    with open(path, "wb") as f:
        f.write(response.audio_content)
    return path

def prompt_url(filename):
    return f"{PUBLIC_BASE_URL}/tts/{filename}"

def prepare_prompts():
    prompts = {
        "intro.mp3": "Здравейте, аз съм AI асистент и се интересувам от апартамента ви. Можете ли да ми кажете повече детайли?",
        "thanks.mp3": "Благодаря ви! Това е всичко от мен. Приятен ден."
    }
    for key, text in QUESTIONS:
        prompts[f"{key}.mp3"] = text
    prompts["negotiation.mp3"] = FOLLOWUP_PRICE

    for filename, text in prompts.items():
        synthesize_tts(text, filename)

# ----------------------------
# Webhook endpoints
# ----------------------------
@app.route("/tts/<path:filename>", methods=["GET"])
def serve_tts(filename):
    return send_from_directory(TTS_CACHE_DIR, filename)

@app.route("/voice", methods=["POST", "GET"])
def voice():
    if not verify_twilio_request():
        return Response("Invalid signature", status=403)

    call_sid = request.values.get("CallSid", "")
    phone_number = request.values.get("To", "")
    request_id = request.values.get("request_id", "")

    with state_lock:
        pending = pending_requests.pop(request_id, {}) if request_id else {}
    name = pending.get("name")
    apartment_details = pending.get("apartment_details", {})

    if call_sid:
        init_call_state(call_sid, phone_number, name, apartment_details)

    response = VoiceResponse()

    if WS_PUBLIC_URL:
        connect = Connect()
        connect.stream(url=f"{WS_PUBLIC_URL}?call_sid={call_sid}")
        response.append(connect)

    response.play(prompt_url("intro.mp3"))
    response.pause(length=5)
    response.redirect(f"{PUBLIC_BASE_URL}/next?call_sid={call_sid}", method="POST")

    return Response(str(response), mimetype="text/xml")

@app.route("/next", methods=["POST", "GET"])
def next_step():
    if not verify_twilio_request():
        return Response("Invalid signature", status=403)

    call_sid = request.values.get("call_sid", "")
    response = VoiceResponse()

    question = next_question(call_sid)
    if question:
        key, text = question
        filename = f"{key}.mp3" if key != "negotiation" else "negotiation.mp3"
        response.play(prompt_url(filename))
        response.pause(length=6)
        response.redirect(f"{PUBLIC_BASE_URL}/next?call_sid={call_sid}", method="POST")
    else:
        response.play(prompt_url("thanks.mp3"))
        response.hangup()

    return Response(str(response), mimetype="text/xml")

@app.route("/status", methods=["POST"])
def status_callback():
    if not verify_twilio_request():
        return Response("Invalid signature", status=403)

    call_sid = request.values.get("CallSid", "")
    call_status = request.values.get("CallStatus", "")
    if call_status == "completed":
        store_inquiry(call_sid)
    return ("", 204)

@app.route("/recording", methods=["POST"])
def recording_callback():
    if not verify_twilio_request():
        return Response("Invalid signature", status=403)

    call_sid = request.values.get("CallSid", "")
    recording_url = request.values.get("RecordingUrl", "")
    if recording_url:
        append_recording_url(call_sid, recording_url)
    return ("", 204)

# ----------------------------
# WebSocket server for Twilio Media Streams
# ----------------------------
def start_recognizer(call_sid):
    q = queue.Queue()
    audio_queues[call_sid] = q
    thread = threading.Thread(target=streaming_recognize, args=(call_sid, q), daemon=True)
    thread.start()

def streaming_recognize(call_sid, q):
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
            single_utterance=False
        )

        def request_generator():
            while True:
                chunk = q.get()
                if chunk is None:
                    break
                yield speech.StreamingRecognizeRequest(audio_content=chunk)

        responses = client.streaming_recognize(streaming_config, request_generator())
        for response in responses:
            for result in response.results:
                if result.alternatives:
                    text = result.alternatives[0].transcript.strip()
                    if result.is_final and text:
                        update_transcript(call_sid, text)
    except Exception:
        logger.exception("Speech recognition failed for call_sid=%s", call_sid)

async def ws_handler(websocket, path):
    parsed = urlparse(path)
    params = parse_qs(parsed.query)
    call_sid = params.get("call_sid", [None])[0]
    if not call_sid:
        return

    if call_sid not in audio_queues:
        start_recognizer(call_sid)

    try:
        async for message in websocket:
            payload = json.loads(message)
            event = payload.get("event")
            if event == "media":
                b64 = payload["media"]["payload"]
                audio = base64.b64decode(b64)
                audio_queues[call_sid].put(audio)
            elif event == "stop":
                audio_queues[call_sid].put(None)
                break
    except Exception:
        logger.exception("WebSocket error for call_sid=%s", call_sid)
    finally:
        if call_sid in audio_queues:
            audio_queues[call_sid].put(None)

def run_ws_server():
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    server = websockets.serve(ws_handler, WS_BIND_HOST, WS_BIND_PORT, ping_interval=None)
    loop.run_until_complete(server)
    loop.run_forever()

def run_http_server():
    app.run(host=HTTP_BIND_HOST, port=HTTP_BIND_PORT)

# ----------------------------
# Main API
# ----------------------------
def make_inquiry(phone_number, name=None, initial_apartment_info={}):
    env_required("TWILIO_ACCOUNT_SID", TWILIO_ACCOUNT_SID)
    env_required("TWILIO_AUTH_TOKEN", TWILIO_AUTH_TOKEN)
    env_required("TWILIO_FROM_NUMBER", TWILIO_FROM_NUMBER)
    env_required("PUBLIC_BASE_URL", PUBLIC_BASE_URL)

    # Basic E.164 validation
    if not re.match(r"^\+\d{8,15}$", phone_number):
        raise ValueError("phone_number must be in E.164 format, e.g. +359XXXXXXXXX")

    request_id = str(uuid.uuid4())
    pending_requests[request_id] = {
        "name": name,
        "apartment_details": dict(initial_apartment_info or {})
    }

    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    try:
        call = client.calls.create(
            to=phone_number,
            from_=TWILIO_FROM_NUMBER,
            url=f"{PUBLIC_BASE_URL}/voice?request_id={request_id}",
            status_callback=f"{PUBLIC_BASE_URL}/status",
            status_callback_event=["initiated", "ringing", "answered", "completed"],
            record=True,
            recording_status_callback=f"{PUBLIC_BASE_URL}/recording",
        )
        logger.info("Call initiated: %s", call.sid)
        return call.sid
    except Exception:
        logger.exception("Failed to initiate call to %s", phone_number)
        raise

# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    prepare_prompts()

    # Start HTTP and WS servers (must be publicly accessible for Twilio)
    threading.Thread(target=run_http_server, daemon=True).start()
    threading.Thread(target=run_ws_server, daemon=True).start()

    time.sleep(1)
    make_inquiry(
        "+359888123456",
        name="Иван",
        initial_apartment_info={"id": 101, "city": "Sofia"}
    )

    while True:
        time.sleep(5)