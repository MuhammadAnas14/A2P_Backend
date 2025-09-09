#!/usr/bin/env python3
"""
Telnyx AI Assistant – Post-Call Pipeline (SQLite default) with Google Calendar Integration
Bug Fixes v8 + Syntax Corrections

Overview
--------
This Flask service receives Telnyx call webhooks and orchestrates:
1) IVR menu (press 1 for AI assistant, 2 for Sales)
2) AI live assistant start & conversation transcription
3) Dual-channel call recording and local WAV storage
4) Post-call pipeline:
   - Whisper transcription (fallback if Telnyx insights lack a transcript)
   - GPT-based refinement: summary, urgency (1–5), category, subcategory
   - SQLite (or Postgres) upsert of call metadata
   - Optional meeting extraction via GPT and Google Calendar event creation

It also exposes endpoints for:
- SSE events to notify a React UI about incoming/ended calls
- Knowledge base sync (upload file to S3-compatible storage and trigger embeddings)
- Fetching calls & profile summaries for the UI
- Fetching calls by source ID for communication history

Security Notes
--------------
- This assumes env vars are securely provided (no secrets hard-coded).
- Google token files must be pre-provisioned via OAuth.
- Telnyx webhooks should be authenticated/validated in production.
- CORS is limited to local/Vercel frontends; tighten for production.

"""

import os, json, logging, traceback, threading, requests, sqlite3, sys, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask, request, Response
from openai import OpenAI
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import boto3, pathlib, tempfile
from botocore.config import Config


# ─────────────────────────────── Environment ────────────────────────────────
load_dotenv()
TELNYX_KEY          = os.getenv("TELNYX_API_KEY")
TELNYX_ASSISTANT_ID = os.getenv("TELNYX_ASSISTANT_ID")
OPENAI_KEY          = os.getenv("OPENAI_API_KEY")
DATABASE_URL        = os.getenv("DATABASE_URL", "sqlite:///call_data.db")
GOOGLE_TOKEN_PATH   = os.getenv("GOOGLE_TOKEN_PATH", "token.json")
GOOGLE_CRED_PATH    = os.getenv("GOOGLE_CRED_PATH", "credentials.json")
PORT                = int(os.getenv("PORT", 5000))

# Default Sales transfer number for IVR option 2
SALES_NUMBER = os.getenv("SALES_NUMBER", "+17059986135")

# Temp directory for KB uploads
TMP_DIR = Path(tempfile.gettempdir()) / "telnyx_kb"
TMP_DIR.mkdir(parents=True, exist_ok=True)

# Fail fast if core keys are missing
assert TELNYX_KEY and TELNYX_ASSISTANT_ID and OPENAI_KEY

# OpenAI client (used for Whisper & GPT classification/extraction)
client = OpenAI(api_key=OPENAI_KEY)

# S3-compatible KB storage (Telnyx Cloud Storage in this example)
KB_BUCKET   = os.getenv("TELNYX_BUCKET", "kb-main")
KB_ENDPOINT = os.getenv("TELNYX_STORAGE_ENDPOINT",
                        "https://us-east-1.telnyxcloudstorage.com")

_s3_cfg = Config(
    request_checksum_calculation="when_required",
    response_checksum_validation="when_required"
)

s3 = boto3.client(
    "s3",
    endpoint_url=KB_ENDPOINT,
    aws_access_key_id=TELNYX_KEY,
    aws_secret_access_key=TELNYX_KEY,
    config=_s3_cfg
)

def kb_upload(local_path: str) -> str:
    """
    Upload a file to the KB bucket.
    Returns the object key if successful.
    """
    key = pathlib.Path(local_path).name
    s3.upload_file(local_path, KB_BUCKET, key, ExtraArgs={"ACL": "private"})
    logging.info("KB ► uploaded %s to %s/%s", local_path, KB_BUCKET, key)
    return key

def kb_trigger_embed(bucket: str = KB_BUCKET) -> str:
    """
    Instruct Telnyx (or your vector service) to rebuild embeddings for the KB.
    Expects TELNYX_API & HEADERS to be configured.
    """
    r = requests.post(
        f"{TELNYX_API}/ai/embeddings",
        json={"bucket_name": bucket},
        headers=HEADERS, timeout=15
    )
    r.raise_for_status()
    task_id = r.json()["data"]["task_id"]
    logging.info("KB ► embedding task %s queued", task_id)
    return task_id


# ───────────────────────────── Google Calendar ──────────────────────────────
def load_calendar_service():
    """
    Build an authenticated Google Calendar API client.
    Assumes token.json exists & has 'calendar.events' scope.
    """
    creds = Credentials.from_authorized_user_file(
        GOOGLE_TOKEN_PATH,
        ["https://www.googleapis.com/auth/calendar.events"]
    )
    return build('calendar', 'v3', credentials=creds)


# ────────────────────────── Extract Meeting Time via GPT ────────────────────
def extract_meeting_time(transcript: str) -> datetime | None:
    """
    Ask GPT to extract a precise meeting datetime (with timezone).
    Return a Python datetime or None if nothing is found.
    """
    prompt = f"""
You are an assistant that extracts meeting date and time from conversations.
Return ONLY JSON with a single key \"datetime\" whose value is the meeting start
in ISO 8601 format with full date and time (e.g. \"2025-07-10T15:00:00+02:00\").
If no meeting time is mentioned, return an empty string.
Conversation transcript: 
""" + transcript
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role":"system", "content": "You extract precise meeting datetimes including timezone offset."},
                {"role":"user",   "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0
        )
        out = json.loads(res.choices[0].message.content)
        dt_str = out.get("datetime", "").strip()
        if dt_str:
            return datetime.fromisoformat(dt_str)
    except Exception as e:
        logging.error("Time extraction error: %s", e)
    return None


# ───────────────────────────────── Constants ────────────────────────────────
TELNYX_API = "https://api.telnyx.com/v2"
HEADERS    = {"Authorization": f"Bearer {TELNYX_KEY}", "Content-Type": "application/json"}

# Local directory for saving call recordings
REC_DIR = Path("recordings")
REC_DIR.mkdir(exist_ok=True)

# Static taxonomies for GPT categorization
SALES_SUBCATS   = ["Inquiry","Pricing","Availability","Demo/Book","Order","Price Negotiable","Deposit or Reserve","After Sales Service","Referred"]
SUPPORT_SUBCATS = ["Troubleshooting","Installation/Setup","Account","Billing","Technical Issue","Complaint","Warranty/Return","How-To","Follow-up"]


# ───────────────────────────── Database setup ───────────────────────────────
# Supports SQLite by default, or Postgres if DATABASE_URL starts with 'postgres'
if DATABASE_URL.startswith("postgres"):
    try:
        import psycopg2
    except ModuleNotFoundError:
        logging.error("psycopg2 missing – install or unset DATABASE_URL")
        sys.exit(1)
    conn = psycopg2.connect(DATABASE_URL)
    autocommit = True
    PLACE = "%s"
else:
    db_path = DATABASE_URL.replace("sqlite:///", "")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    autocommit = False
    PLACE = "?"

# Simple upsertable call log table for storing raw & GPT-enriched metadata
conn.execute("""
CREATE TABLE IF NOT EXISTS calls(
  rec_id TEXT PRIMARY KEY,        -- Telnyx recording ID (used as unique key)
  phone TEXT,                     -- caller number
  wav_path TEXT,                  -- local path of saved WAV file
  summary_tx TEXT,                -- Telnyx insights: summary
  urgency_tx INTEGER,             -- Telnyx insights: urgency (1-5)
  category_tx TEXT,               -- Telnyx insights: category
  subcat_tx TEXT,                 -- Telnyx insights: subcategory
  transcript TEXT,                -- transcript (Telnyx or Whisper fallback)
  summary_gpt TEXT,               -- GPT refined: summary
  urgency_gpt INTEGER,            -- GPT refined: urgency (1-5)
  category_gpt TEXT,              -- GPT refined: category
  subcat_gpt TEXT,                -- GPT refined: subcategory
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
if autocommit:
    conn.commit()


# ───────────────────────────── Flask & SSE State ────────────────────────────
app = Flask(__name__)
from flask_cors import CORS
CORS(app, origins=["http://localhost:3000", "https://*.vercel.app"])

# In-memory call state keyed by call_control_id
state: dict[str, dict] = {}

# Server-Sent Events plumbing to notify the UI of call lifecycle
import queue
sse_clients = []
sse_lock = threading.Lock()

def send_sse_event(event_type, data):
    """
    Broadcast a Server-Sent Event to connected UI clients.
    """
    with sse_lock:
        disconnected_clients = []
        for client_queue in sse_clients:
            try:
                client_queue.put(f"event: {event_type}\ndata: {json.dumps(data)}\n\n")
            except:
                disconnected_clients.append(client_queue)
        # Prune clients that dropped
        for client in disconnected_clients:
            sse_clients.remove(client)

@app.route("/api/events")
def events():
    """
    SSE endpoint: the React app subscribes to receive 'incoming_call' and 'call_ended'.
    """
    def event_stream():
        client_queue = queue.Queue()
        with sse_lock:
            sse_clients.append(client_queue)
        try:
            while True:
                try:
                    data = client_queue.get(timeout=30)
                    yield data
                except queue.Empty:
                    # Keep the connection alive
                    yield "data: heartbeat\n\n"
        except GeneratorExit:
            with sse_lock:
                if client_queue in sse_clients:
                    sse_clients.remove(client_queue)
    return Response(event_stream(), mimetype="text/event-stream")


# ───────────────────────────────── Helpers ──────────────────────────────────

def telnyx_cmd(cid: str, action: str, body: dict | None = None):
    """
    Send a Call Control command to Telnyx for a given call_control_id.
    """
    r = requests.post(
        f"{TELNYX_API}/calls/{cid}/actions/{action}",
        json=body or {}, headers=HEADERS, timeout=15
    )
    r.raise_for_status()
    return r.json()

def gather_using_speak(cid: str, prompt: str, valid_digits: str = "12", num_digits: int = 1, timeout_secs: int = 8):
    """
    Simple IVR gather using text-to-speech; captures DTMF digits.
    `valid_digits` MUST be a string of allowed digits (e.g., "12").
    """
    return telnyx_cmd(cid, "gather_using_speak", {
        "payload": prompt,
        "valid_digits": valid_digits,
        "num_digits": num_digits,
        "timeout_secs": timeout_secs,
        "invalid_payload": "Sorry, I didn’t catch that.",
        "voice": "AWS.Polly.Brian-Neural"
    })

def speak(cid: str, text: str):
    """Speak a TTS message into the call."""
    return telnyx_cmd(cid, "speak", {"voice": "AWS.Polly.Brian-Neural", "payload": text})

def transfer_to_sales(cid: str):
    """Warm/Blind transfer to Sales number."""
    return telnyx_cmd(cid, "transfer", {"to": SALES_NUMBER})

MENU = "Welcome. Press 1 to talk to our A I assistant, or press 2 for Sales."

def start_menu(cid: str):
    """Kick off the IVR prompt."""
    return gather_using_speak(cid, MENU, valid_digits="12", num_digits=1, timeout_secs=8)

def handle_menu_selection(cid: str, digits: str):
    """
    IVR branch:
      1 → start Telnyx AI Assistant (with transcription)
      2 → transfer to Sales
      else → retry once, then fallback to Sales
    """
    if digits == "1":
        speak(cid, "Connecting you now.")
        return telnyx_cmd(cid, "ai_assistant_start", {
            "assistant": {"id": TELNYX_ASSISTANT_ID},
            "voice": "AWS.Polly.Brian-Neural",
            "greeting": "Hello! I'm your assistant. How can I help you today?",
            "transcription": {
                "enabled": True,
                "model": "distil-whisper/distil-large-v2",
                "enable_interruption_events": True
            }
        })
    if digits == "2":
        speak(cid, "Connecting you to Sales.")
        return transfer_to_sales(cid)

    # Invalid/no input → retry once, then Sales fallback
    info = state.setdefault(cid, {})
    tries = int(info.get("ivr_tries", 0))
    if tries < 1:
        info["ivr_tries"] = tries + 1
        return gather_using_speak(cid, "Sorry, I didn’t catch that. " + MENU, valid_digits="12", num_digits=1, timeout_secs=8)

    speak(cid, "Connecting you to Sales.")
    return transfer_to_sales(cid)


def whisper_transcribe(path: Path) -> str:
    """
    Fallback transcription using OpenAI Whisper if Telnyx insights don't include a transcript.
    """
    with path.open("rb") as f:
        return client.audio.transcriptions.create(model="whisper-1", file=f).text


def gpt_refine(summary: str, transcript: str) -> dict:
    """
    Ask GPT to (re)classify the call and produce:
    - summary (str)
    - urgency (int 1..5)
    - category (str)
    - subcat (str)
    Returns a dict with these keys (defaults if parsing fails).
    """
    prompt = f"""
You are an AI assistant classifying phone calls.
Sales sub-cats: {', '.join(SALES_SUBCATS)}
Support sub-cats: {', '.join(SUPPORT_SUBCATS)}
Return ONLY JSON keys: summary(str), urgency(int 1-5), category(str), subcat(str).

Summary: {summary}
Transcript: {transcript[:3500]}
"""
    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            response_format={"type":"json_object"}
        )
        return json.loads(res.choices[0].message.content)
    except Exception as e:
        logging.error("GPT classify error: %s", e)
        raw = res.choices[0].message.content if 'res' in locals() else ""
        return {"summary": raw[:300], "urgency": 3, "category": "Unknown", "subcat": "Other"}


def save_row(data: dict):
    """
    Upsert call metadata into the 'calls' table.
    Uses rec_id as the primary key.
    """
    cols = [
        "rec_id","phone","wav_path",
        "summary_tx","urgency_tx","category_tx","subcat_tx",
        "transcript",
        "summary_gpt","urgency_gpt","category_gpt","subcat_gpt"
    ]
    vals = [data.get(k, None) for k in cols]
    placeholders = ",".join([PLACE] * len(cols))
    set_clause = ", ".join([f"{c}=excluded.{c}" for c in cols if c != "rec_id"])

    sql = f"""
    INSERT INTO calls ({", ".join(cols)}) VALUES ({placeholders})
    ON CONFLICT(rec_id) DO UPDATE SET {set_clause};
    """
    conn.execute(sql, vals)
    conn.commit()


def create_calendar_event(summary: str, start: datetime, duration_minutes: int = 30):
    """
    Create a Google Calendar event starting at `start` for `duration_minutes`.
    """
    service = load_calendar_service()
    event = {
        'summary': summary,
        'start': {'dateTime': start.isoformat(), 'timeZone': 'Europe/Berlin'},
        'end':   {'dateTime': (start + timedelta(minutes=duration_minutes)).isoformat(), 'timeZone': 'Europe/Berlin'}
    }
    created = service.events().insert(calendarId='primary', body=event).execute()
    logging.info("Event created: %s", created.get('htmlLink'))
    return created


def maybe_finish(cid: str):
    """
    If the call ended and we have a WAV, kick off post-processing
    in a background thread (delayed by ~60s to allow recording availability).
    """
    info = state.get(cid, {})
    if info.get("ended") and info.get("wav_path") and not info.get("processing"):
        info["processing"] = True
        threading.Thread(target=lambda: (time.sleep(60), post_process(cid)), daemon=True).start()


def post_process(cid: str):
    """
    Post-call workflow:
    - Ensure WAV exists
    - Ensure transcript (Telnyx provided or Whisper fallback)
    - GPT refine (summary/urgency/category/subcat)
    - Save to DB
    - Try to extract a meeting datetime and create a GCal event when present
    - Cleanup in-memory state
    """
    info = state.get(cid, {})
    wav = info.get("wav_path")
    if not wav or not Path(wav).exists():
        logging.warning("WAV missing %s", cid)
        return

    transcript = info.get("transcript") or whisper_transcribe(Path(wav))
    gpt_out = gpt_refine(info.get("summary_tx", ""), transcript)
    info.update({
        "transcript":  transcript,
        "summary_gpt": gpt_out.get("summary", ""),
        "urgency_gpt": int(gpt_out.get("urgency", 3)),
        "category_gpt": gpt_out.get("category", ""),
        "subcat_gpt":   gpt_out.get("subcat", "")
    })
    save_row(info)

    meet_dt = extract_meeting_time(transcript)
    if meet_dt:
        create_calendar_event("Meeting from Telnyx AI", meet_dt)

    state.pop(cid, None)


# ───────────────────────────────── Webhook ──────────────────────────────────
@app.route("/telnyx", methods=["POST"])
def hook():
    """
    Telnyx webhook handler for the call lifecycle.

    Handles (among others):
    - call.initiated → answer
    - call.answered → start recording + IVR + notify UI
    - call.gather.ended → branch IVR choice
    - call.conversation_insights.generated → capture Telnyx insights
    - call.recording.saved → download WAV & maybe post-process
    - call.hangup/call.terminated → mark ended & maybe post-process
    """
    try:
        data = request.json.get("data", {})
        ev   = data.get("event_type")
        pl   = data.get("payload", {})
        cid  = pl.get("call_control_id")
        phone= pl.get("from")

        c = state.setdefault(cid, {"phone": phone})
        logging.info("▶ %s", ev)

        if ev == "call.initiated":
            telnyx_cmd(cid, "answer")

        elif ev == "call.answered":
            # start dual-channel recording (no inline transcription here)
            rec = telnyx_cmd(cid, "record_start", {"format": "wav", "channels": "dual", "transcription": False})
            c["rec_id"] = rec.get("data", {}).get("payload", {}).get("recording_id")
            start_menu(cid)
            # Notify UI (SSE)
            send_sse_event("incoming_call", {
                "sourceId": phone,
                "callId": cid,
                "timestamp": datetime.now().isoformat()
            })

        elif ev == "call.gather.ended":
            # IVR selection captured; route to AI assistant or Sales
            digits = pl.get("digits", "") or ""
            handle_menu_selection(cid, digits)

        elif ev in ("call.conversation_insights.generated", "call.conversation.generated"):
            # Store Telnyx insights (if present); Whisper will backfill if absent
            ins = pl.get("insights", {})
            c.update({
                "summary_tx": ins.get("summary", ""),
                "urgency_tx": int(ins.get("urgency", 3)),
                "category_tx": ins.get("category", ""),
                "subcat_tx": ins.get("subcat", ""),
                "transcript": ins.get("transcript", "")
            })

        elif ev == "call.recording.saved":
            # Download the final recording WAV so we can process it later
            rid = pl.get("recording_id")
            p = REC_DIR / f"{datetime.utcnow():%Y%m%d-%H%M%S}_{rid}.wav"
            p.write_bytes(requests.get(pl.get("recording_urls", {}).get("wav"), timeout=30).content)
            c["wav_path"] = str(p)
            maybe_finish(cid)

        elif ev in ("call.hangup", "call.terminated", "call.conversation.ended"):
            # Mark call ended; if WAV is ready, trigger post processing
            c["ended"] = True
            maybe_finish(cid)
            # Notify UI (SSE)
            send_sse_event("call_ended", {
                "sourceId": phone,
                "callId": cid,
                "timestamp": datetime.now().isoformat()
            })

        return "", 204
    except Exception:
        traceback.print_exc()
        return "", 500


# ───────────────────────────── Knowledge Base Sync ──────────────────────────
@app.route("/kb_sync", methods=["POST"])
def kb_sync():
    """
    Upload a file into KB storage and trigger an embeddings rebuild.

    Usage:
      - multipart/form-data with field 'file'
      - or JSON: { "file_url": "https://…" }

    Returns 202 on success (embedding job queued).
    """
    try:
        if "file" in request.files:
            f   = request.files["file"]
            tmp = TMP_DIR / f.filename
            f.save(tmp)
        else:
            url = request.json["file_url"]
            tmp = TMP_DIR / pathlib.Path(url).name
            tmp.write_bytes(requests.get(url, timeout=30).content)

        kb_upload(str(tmp))
        kb_trigger_embed()
        return {"status": "queued"}, 202
    except Exception as e:
        logging.error("KB sync failed: %s", e, exc_info=True)
        return {"error": str(e)}, 500


# ─────────────────────────────── API – Calls List ───────────────────────────
@app.route("/api/calls", methods=["GET"])
def get_calls():
    """
    Fetch all call logs in a UI-friendly format for the React communications page.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT rec_id, phone, created_at, summary_tx, urgency_tx, category_tx, 
                   subcat_tx, summary_gpt, urgency_gpt, category_gpt, subcat_gpt, transcript
            FROM calls 
            ORDER BY created_at DESC
        """)
        
        rows = cursor.fetchall()
        calls = []
        
        for row in rows:
            rec_id = row[0] if isinstance(row[0], str) else (str(row[0]) if row[0] is not None else "")
            comm_id = f"COMM-{rec_id[:6]}" if rec_id else "COMM-XXXXXX"
            call_data = {
                "id": row[0],
                "date": row[2] if row[2] else datetime.now().strftime("%m-%d %H:%M%p"),
                "type": "In",  # NOTE: mark Out if you later add outbound support
                "sourceId": row[1] or "Unknown",
                "endPoint": "AI Agent",
                "company": "Unknown Company",  # TODO: map phone → account if available
                "disposition": get_disposition_from_category(row[5] or row[9]),
                "urgency": row[8] if row[8] else (row[4] if row[4] else 3),
                "aiSummary": row[7] or row[3] or "No summary available",
                "commId": comm_id,
                "transcript": row[11] or "",
                "subcategory": row[10] or row[6] or "General"
            }
            calls.append(call_data)
        
        return {"calls": calls}, 200
        
    except Exception as e:
        logging.error("Error fetching calls: %s", e)
        return {"error": str(e)}, 500


# ───────────────────────────── API – Profile Lookup ─────────────────────────
@app.route("/api/profile/<source_id>", methods=["GET"])
def get_profile_by_source_id(source_id):
    """
    Check if we have any call data for a given phone number and return a simple profile.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT rec_id, phone, created_at, summary_tx, urgency_tx, category_tx, 
                   subcat_tx, summary_gpt, urgency_gpt, category_gpt, subcat_gpt, transcript
            FROM calls 
            WHERE phone = ? 
            ORDER BY created_at DESC 
            LIMIT 1
        """, (source_id,))
        
        row = cursor.fetchone()
        if not row:
            return {"error": "Profile not found"}, 404
        
        profile_data = {
            "sourceId": row[1],
            "companyName": f"Company for {row[1]}",  # TODO: real mapping
            "lastContact": row[2] if row[2] else datetime.now().isoformat(),
            "category": row[9] or row[5] or "General",
            "subcategory": row[10] or row[6] or "General",
            "urgency": row[8] if row[8] else (row[4] if row[4] else 3),
            "lastSummary": row[7] or row[3] or "No summary available",
            "totalCalls": get_call_count_for_source(source_id),
            "engagementScore": 0,  # Explicitly 0 for now
            "exists": True
        }
        return profile_data, 200
        
    except Exception as e:
        logging.error("Error fetching profile for source_id %s: %s", source_id, e)
        return {"error": str(e)}, 500


def get_call_count_for_source(source_id):
    """
    Helper: count total calls associated with a given phone number.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM calls WHERE phone = ?", (source_id,))
        return cursor.fetchone()[0]
    except Exception:
        return 0


# ─────────────────────── API – Calls by Source (History) ────────────────────
@app.route("/api/calls/<source_id>", methods=["GET"])
def get_calls_by_source_id(source_id):
    """
    Return communication history filtered by phone number (source_id).
    Designed for the right-hand detail panel in the communications UI.
    """
    try:
        if not source_id or source_id == "undefined" or source_id == "null":
            logging.warning("Invalid source_id received: %s", source_id)
            return {"calls": [], "message": "No source ID provided"}, 200
        
        normalized_source_id = source_id.strip()
        logging.info("Fetching calls for source_id: %s", normalized_source_id)
        
        # Create a short-lived dedicated connection for this request
        if DATABASE_URL.startswith("postgres"):
            import psycopg2
            db_conn = psycopg2.connect(DATABASE_URL)
        else:
            db_path = DATABASE_URL.replace("sqlite:///", "")
            db_conn = sqlite3.connect(db_path, check_same_thread=False)
        
        try:
            cursor = db_conn.cursor()
            cursor.execute("""
                SELECT rec_id, phone, created_at, summary_tx, urgency_tx, category_tx, 
                       subcat_tx, summary_gpt, urgency_gpt, category_gpt, subcat_gpt, transcript
                FROM calls 
                WHERE phone LIKE ? OR phone = ?
                ORDER BY created_at DESC
            """, (f"%{normalized_source_id}%", normalized_source_id))
            
            rows = cursor.fetchall()
            logging.info("Found %d calls for source_id: %s", len(rows), normalized_source_id)
            
            calls = []
            for i, row in enumerate(rows):
                try:
                    call_data = {
                        "id": str(row[0]) if row[0] else f"call_{i}",
                        "date": str(row[2]) if row[2] else datetime.now().strftime("%m-%d %H:%M%p"),
                        "type": "In",
                        "sourceId": str(row[1]) if row[1] else normalized_source_id,
                        "endPoint": "AI Agent",
                        "company": f"Company for {str(row[1]) if row[1] else 'Unknown'}",
                        "disposition": get_disposition_from_category(row[5] or row[9]),
                        "urgency": int(row[8]) if row[8] else (int(row[4]) if row[4] else 3),
                        "aiSummary": str(row[7] or row[3] or "No summary available"),
                        "commId": f"COMM-{str(row[0])[:6] if row[0] else f'{i:06d}'}",
                        "transcript": str(row[11] or ""),
                        "subcategory": str(row[10] or row[6] or "General")
                    }
                    calls.append(call_data)
                except Exception as row_error:
                    # Log and continue if individual row has unexpected data
                    logging.error("Error processing row %d: %s", i, row_error)
                    continue
            
            cursor.close()
        finally:
            db_conn.close()
        
        logging.info("Successfully processed %d calls for source_id: %s", len(calls), normalized_source_id)
        return {"calls": calls}, 200
        
    except Exception as e:
        logging.error("Error fetching calls for source_id %s: %s", source_id, e, exc_info=True)
        return {"error": f"Database error: {str(e)}"}, 500


def get_disposition_from_category(category: str | None) -> str:
    """
    Map high-level category text into a UI 'disposition' label.
    """
    if not category:
        return "Unknown"
    
    category_lower = category.lower()
    if "sales" in category_lower or "inquiry" in category_lower:
        return "Sales Inquiry"
    elif "support" in category_lower or "technical" in category_lower:
        return "Technical Support"
    elif "booking" in category_lower or "demo" in category_lower:
        return "Demo Booked"
    else:
        return category


# ──────────────────────────────── Entrypoint ────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("Listening on :%s (DB=%s)", PORT,
                 ("PostgreSQL" if DATABASE_URL.startswith("postgres") else "SQLite"))
    # In production, consider:
    #   - debug=False (already set)
    #   - behind a reverse proxy with HTTPS
    #   - webhook signature verification
    app.run(host="0.0.0.0", port=PORT, debug=False)
