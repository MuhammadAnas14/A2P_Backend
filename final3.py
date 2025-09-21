#!/usr/bin/env python3
"""
Telnyx AI Assistant – Post-Call Pipeline (SQLite default) with Google Calendar Integration
Bug Fixes v8 + Syntax Corrections
"""

import os, json, logging, traceback, threading, requests, sqlite3, sys, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask, request,Response
from openai import OpenAI
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import boto3, pathlib, tempfile
from botocore.config import Config


# ─── Environment ───────────────────────────────────────────
load_dotenv()
TELNYX_KEY          = os.getenv("TELNYX_API_KEY")
TELNYX_ASSISTANT_ID = os.getenv("TELNYX_ASSISTANT_ID")
OPENAI_KEY          = os.getenv("OPENAI_API_KEY")
DATABASE_URL        = os.getenv("DATABASE_URL", "sqlite:///call_data.db")
GOOGLE_TOKEN_PATH   = os.getenv("GOOGLE_TOKEN_PATH", "token.json")
GOOGLE_CRED_PATH    = os.getenv("GOOGLE_CRED_PATH", "credentials.json")
PORT                = int(os.getenv("PORT", 5000))
SALES_NUMBER = os.getenv("SALES_NUMBER", "+17059986135")
TMP_DIR = Path(tempfile.gettempdir()) / "telnyx_kb"
TMP_DIR.mkdir(parents=True, exist_ok=True)
assert TELNYX_KEY and TELNYX_ASSISTANT_ID and OPENAI_KEY

client = OpenAI(api_key=OPENAI_KEY)

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
    key = pathlib.Path(local_path).name
    s3.upload_file(local_path, KB_BUCKET, key, ExtraArgs={"ACL": "private"})
    logging.info("KB ► uploaded %s to %s/%s", local_path, KB_BUCKET, key)
    return key

def kb_trigger_embed(bucket: str = KB_BUCKET) -> str:
    r = requests.post(
        f"{TELNYX_API}/ai/embeddings",
        json={"bucket_name": bucket},
        headers=HEADERS, timeout=15
    )
    r.raise_for_status()
    task_id = r.json()["data"]["task_id"]
    logging.info("KB ► embedding task %s queued", task_id)
    return task_id

# ─── Google Calendar Setup ─────────────────────────────────
def load_calendar_service():
    creds = Credentials.from_authorized_user_file(
        GOOGLE_TOKEN_PATH,
        ["https://www.googleapis.com/auth/calendar.events"]
    )
    return build('calendar', 'v3', credentials=creds)

# ─── Extract Meeting Time via GPT ────────────────────────────
def extract_meeting_time(transcript: str) -> datetime | None:
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

# ─── Constants ─────────────────────────────────────────────
TELNYX_API = "https://api.telnyx.com/v2"
HEADERS    = {"Authorization": f"Bearer {TELNYX_KEY}", "Content-Type": "application/json"}
REC_DIR    = Path("recordings")
REC_DIR.mkdir(exist_ok=True)

# ─── Categories ────────────────────────────────────────────
SALES_SUBCATS   = ["Inquiry","Pricing","Availability","Demo/Book","Order","Price Negotiable","Deposit or Reserve","After Sales Service","Referred"]
SUPPORT_SUBCATS = ["Troubleshooting","Installation/Setup","Account","Billing","Technical Issue","Complaint","Warranty/Return","How-To","Follow-up"]

# ─── Database setup ────────────────────────────────────────
if DATABASE_URL.startswith("postgres"):
    try:
        import psycopg2
    except ModuleNotFoundError:
        logging.error("psycopg2 missing – install or unset DATABASE_URL"); sys.exit(1)
    conn = psycopg2.connect(DATABASE_URL)
    autocommit = True
    PLACE = "%s"
else:
    db_path = DATABASE_URL.replace("sqlite:///", "")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    autocommit = False
    PLACE = "?"

conn.execute("""
CREATE TABLE IF NOT EXISTS calls(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rec_id TEXT, phone TEXT, wav_path TEXT,
  summary_tx TEXT, urgency_tx INTEGER, category_tx TEXT, subcat_tx TEXT,
  transcript TEXT,
  summary_gpt TEXT, urgency_gpt INTEGER, category_gpt TEXT, subcat_gpt TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
if autocommit:
    conn.commit()

# ─── Flask & State ─────────────────────────────────────────
app = Flask(__name__)
from flask_cors import CORS
CORS(app, origins=["http://localhost:3000", "https://*.vercel.app"])
state: dict[str, dict] = {}

from flask import Response
import queue
import threading

# Global queue for SSE events
sse_clients = []
sse_lock = threading.Lock()

def send_sse_event(event_type, data):
    """Send Server-Sent Event to all connected clients"""
    with sse_lock:
        disconnected_clients = []
        for client_queue in sse_clients:
            try:
                client_queue.put(f"event: {event_type}\ndata: {json.dumps(data)}\n\n")
            except:
                disconnected_clients.append(client_queue)
        
        # Remove disconnected clients
        for client in disconnected_clients:
            sse_clients.remove(client)

@app.route("/api/events")
def events():
    """Server-Sent Events endpoint for real-time notifications"""
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
                    yield "data: heartbeat\n\n"
        except GeneratorExit:
            with sse_lock:
                if client_queue in sse_clients:
                    sse_clients.remove(client_queue)
    
    return Response(event_stream(), mimetype="text/event-stream")


# ─── Helpers ───────────────────────────────────────────────

def telnyx_cmd(cid: str, action: str, body: dict | None = None):
    r = requests.post(
        f"{TELNYX_API}/calls/{cid}/actions/{action}",
        json=body or {}, headers=HEADERS, timeout=15
    )
    r.raise_for_status()
    return r.json()

def gather_using_speak(cid: str, prompt: str, valid_digits: str = "12", num_digits: int = 1, timeout_secs: int = 8):
    # IMPORTANT: valid_digits must be a STRING (e.g., "12")
    return telnyx_cmd(cid, "gather_using_speak", {
        "payload": prompt,
        "valid_digits": valid_digits,
        "num_digits": num_digits,
        "timeout_secs": timeout_secs,
        "invalid_payload": "Sorry, I didn’t catch that.",
        "voice": "AWS.Polly.Brian-Neural"
    })

def speak(cid: str, text: str):
    return telnyx_cmd(cid, "speak", {"voice": "AWS.Polly.Brian-Neural", "payload": text})

def transfer_to_sales(cid: str):
    return telnyx_cmd(cid, "transfer", {"to": SALES_NUMBER})

MENU = "Welcome. Press 1 to talk to our A I assistant, or press 2 for Sales."

def start_menu(cid: str):
    # start the IVR prompt & DTMF gather
    return gather_using_speak(cid, MENU, valid_digits="12", num_digits=1, timeout_secs=8)

def handle_menu_selection(cid: str, digits: str):
    # Option 1: AI assistant
    if digits == "1":
        speak(cid, "Connecting you now.")
        return telnyx_cmd(cid, "ai_assistant_start", {
            "assistant": {"id": TELNYX_ASSISTANT_ID},
            "voice": "AWS.Polly.Brian-Neural",  # you can switch to a Telnyx voice if you prefer
            "greeting": "Hello! I'm your assistant. How can I help you today?",
            "transcription": {
                "enabled": True,
                "model": "distil-whisper/distil-large-v2",
                "enable_interruption_events": True
            }
        })

    # Option 2: Sales transfer
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
    with path.open("rb") as f:
        return client.audio.transcriptions.create(model="whisper-1", file=f).text


def gpt_refine(summary: str, transcript: str) -> dict:
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
    cols = "rec_id,phone,wav_path,summary_tx,urgency_tx,category_tx,subcat_tx,transcript,summary_gpt,urgency_gpt,category_gpt,subcat_gpt"
    vals = [str(data.get(k, '')) for k in cols.split(',')]
    placeholders = ','.join([PLACE] * len(vals))
    
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO calls({cols}) VALUES({placeholders})", vals)
    
    # Get the auto-incremented ID
    if DATABASE_URL.startswith("postgres"):
        cursor.execute("SELECT LASTVAL()")
        new_id = cursor.fetchone()[0]
    else:
        new_id = cursor.lastrowid
    
    if not autocommit:
        conn.commit()
    else:
        conn.commit()
    
    cursor.close()
    logging.info("Saved call record with ID: %d", new_id)
    return new_id


def create_calendar_event(summary: str, start: datetime, duration_minutes: int = 30):
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
    info = state.get(cid, {})
    if info.get("ended") and info.get("wav_path") and not info.get("processing"):
        info["processing"] = True
        threading.Thread(target=lambda: (time.sleep(60), post_process(cid)), daemon=True).start()


def post_process(cid: str):
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

# ─── Webhook ───────────────────────────────────────────────
@app.route("/telnyx", methods=["POST"])
def hook():
    try:
        data = request.json.get("data", {})
        ev   = data.get("event_type")
        pl   = data.get("payload", {})
        cid  = pl.get("call_control_id")
        phone= pl.get("from")
        c    = state.setdefault(cid, {"phone": phone})
        logging.info("▶ %s", ev)

        if ev == "call.initiated":
            telnyx_cmd(cid, "answer")

        elif ev == "call.answered":
            rec = telnyx_cmd(cid, "record_start", {"format": "wav", "channels": "dual", "transcription": False})
            c["rec_id"] = rec.get("data", {}).get("payload", {}).get("recording_id")
            start_menu(cid)
            send_sse_event("incoming_call", {
                "sourceId": phone,
                "callId": cid,
                "timestamp": datetime.now().isoformat()
            })
            
            
        elif ev == "call.gather.ended":
            digits = pl.get("digits", "") or ""
            handle_menu_selection(cid, digits)

        elif ev in ("call.conversation_insights.generated", "call.conversation.generated"):
            ins = pl.get("insights", {})
            c.update({
                "summary_tx": ins.get("summary", ""),
                "urgency_tx": int(ins.get("urgency", 3)),
                "category_tx": ins.get("category", ""),
                "subcat_tx": ins.get("subcat", ""),
                "transcript": ins.get("transcript", "")
            })

        elif ev == "call.recording.saved":
            rid = pl.get("recording_id")
            p = REC_DIR / f"{datetime.utcnow():%Y%m%d-%H%M%S}_{rid}.wav"
            p.write_bytes(requests.get(pl.get("recording_urls", {}).get("wav"), timeout=30).content)
            c["wav_path"] = str(p)
            maybe_finish(cid)

        elif ev in ("call.hangup", "call.terminated", "call.conversation.ended"):
            c["ended"] = True
            maybe_finish(cid)
            
            send_sse_event("call_ended", {
                "sourceId": phone,
                "callId": cid,
                "timestamp": datetime.now().isoformat()
            })

        return "", 204
    except Exception:
        traceback.print_exc()
        return "", 500
@app.route("/kb_sync", methods=["POST"])
def kb_sync():
    """
    POST multipart/form-data  field 'file'
    OR   POST JSON { "file_url": "https://…" }
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
    
    
@app.route("/api/calls", methods=["GET"])
def get_calls():
    """
    GET endpoint to fetch all call logs for the communication log page
    Returns call data in the format expected by the React frontend
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, rec_id, phone, created_at, summary_tx, urgency_tx, category_tx, 
                   subcat_tx, summary_gpt, urgency_gpt, category_gpt, subcat_gpt, transcript
            FROM calls 
            ORDER BY created_at DESC
        """)
        
        rows = cursor.fetchall()
        calls = []
        
        for row in rows:
            # Format the data to match the React component structure
            call_data = {
                "id": row[1],  # rec_id
                "date": row[3] if row[3] else datetime.now().strftime("%m-%d %H:%M%p"),  # created_at
                "type": "In" if row[2] else "Out",  # Assuming incoming calls for now
                "sourceId": row[2] or "Unknown",  # phone
                "endPoint": "AI Agent",
                "company": "Unknown Company",  # You may want to add company mapping
                "disposition": get_disposition_from_category(row[6] or row[10]),  # category_gpt or category_tx
                "urgency": row[9] if row[9] else (row[5] if row[5] else 3),  # urgency_gpt or urgency_tx
                "aiSummary": row[8] or row[4] or "No summary available",  # summary_gpt or summary_tx
                "commId": f"COMM-{row[0]:06d}",  # id (auto-increment)
                "transcript": row[12] or "",  # transcript
                "subcategory": row[11] or row[7] or "General"  # subcat_gpt or subcat_tx
            }
            calls.append(call_data)
        
        return {"calls": calls}, 200
        
    except Exception as e:
        logging.error("Error fetching calls: %s", e)
        return {"error": str(e)}, 500
    
@app.route("/api/profile/<source_id>", methods=["GET"])
def get_profile_by_source_id(source_id):
    """
    GET endpoint to check if a profile exists for a given source_id (phone number)
    Returns profile data if exists, 404 if not found
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, rec_id, phone, created_at, summary_tx, urgency_tx, category_tx, 
                   subcat_tx, summary_gpt, urgency_gpt, category_gpt, subcat_gpt, transcript
            FROM calls 
            WHERE phone = ? 
            ORDER BY created_at DESC 
            LIMIT 1
        """, (source_id,))
        
        row = cursor.fetchone()
        
        if not row:
            return {"error": "Profile not found"}, 404
        
        # Return profile data for existing source_id
        profile_data = {
            "sourceId": row[2],  # phone
            "companyName": f"Company for {row[2]}",  # You may want to add company mapping
            "lastContact": row[3] if row[3] else datetime.now().isoformat(),
            "category": row[10] or row[6] or "General",  # category_gpt or category_tx
            "subcategory": row[11] or row[7] or "General",  # subcat_gpt or subcat_tx
            "urgency": row[9] if row[9] else (row[5] if row[5] else 3),  # urgency_gpt or urgency_tx
            "lastSummary": row[8] or row[4] or "No summary available",  # summary_gpt or summary_tx
            "totalCalls": get_call_count_for_source(source_id),
            "engagementScore": 0,  # Set to 0 as requested
            "exists": True
        }
        
        return profile_data, 200
        
    except Exception as e:
        logging.error("Error fetching profile for source_id %s: %s", source_id, e)
        return {"error": str(e)}, 500

def get_call_count_for_source(source_id):
    """Helper function to get total call count for a source_id"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM calls WHERE phone = ?", (source_id,))
        return cursor.fetchone()[0]
    except Exception:
        return 0
# @app.route("/api/calls/<source_id>", methods=["GET"])
# def get_calls_by_source_id(source_id):
#     """
#     GET endpoint to fetch call logs filtered by source_id for communication history
#     """
#     try:
#         if not source_id or source_id == "undefined" or source_id == "null":
#             logging.warning("Invalid source_id received: %s", source_id)
#             return {"calls": [], "message": "No source ID provided"}, 200
        
#         normalized_source_id = source_id.strip()
        
#         cursor = conn.cursor()
#         cursor.execute("""
#             SELECT rec_id, phone, created_at, summary_tx, urgency_tx, category_tx, 
#                    subcat_tx, summary_gpt, urgency_gpt, category_gpt, subcat_gpt, transcript
#             FROM calls 
#             WHERE phone LIKE ? OR phone = ?
#             ORDER BY created_at DESC
#         """, (f"%{normalized_source_id}%", normalized_source_id))
        
#         rows = cursor.fetchall()
#         calls = []
        
#         logging.info("Found %d calls for source_id: %s", len(rows), source_id)
        
#         for row in rows:
#             call_data = {
#                 "id": row[0],  # rec_id
#                 "date": row[2] if row[2] else datetime.now().strftime("%m-%d %H:%M%p"),
#                 "type": "In",  # Assuming incoming calls
#                 "sourceId": row[1],  # phone
#                 "endPoint": "AI Agent",
#                 "company": f"Company for {row[1]}",
#                 "disposition": get_disposition_from_category(row[5] or row[9]),
#                 "urgency": row[8] if row[8] else (row[4] if row[4] else 3),
#                 "aiSummary": row[7] or row[3] or "No summary available",
#                 "commId": f"COMM-{row[0][:6]}",
#                 "transcript": row[11] or "",
#                 "subcategory": row[10] or row[6] or "General"
#             }
#             calls.append(call_data)
#             logging.info("Found  calls for source_id: %s", call_data)
        
#         return {"calls": calls}, 200
        
#     except Exception as e:
#         logging.error("Error fetching calls for source_id %s: %s", source_id, e)
#         return {"error": str(e)}, 500

@app.route("/api/calls/<source_id>", methods=["GET"])
def get_calls_by_source_id(source_id):
    """
    GET endpoint to fetch call logs filtered by source_id for communication history
    """
    try:
        if not source_id or source_id == "undefined" or source_id == "null":
            logging.warning("Invalid source_id received: %s", source_id)
            return {"calls": [], "message": "No source ID provided"}, 200
        
        normalized_source_id = source_id.strip()
        logging.info("Fetching calls for source_id: %s", normalized_source_id)
        
        if DATABASE_URL.startswith("postgres"):
            import psycopg2
            db_conn = psycopg2.connect(DATABASE_URL)
        else:
            db_path = DATABASE_URL.replace("sqlite:///", "")
            db_conn = sqlite3.connect(db_path, check_same_thread=False)
        
        try:
            cursor = db_conn.cursor()
            cursor.execute("""
                SELECT id, rec_id, phone, created_at, summary_tx, urgency_tx, category_tx, 
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
                        "id": str(row[1]) if row[1] else f"call_{i}",  # rec_id
                        "date": str(row[3]) if row[3] else datetime.now().strftime("%m-%d %H:%M%p"),  # created_at
                        "type": "In",
                        "sourceId": str(row[2]) if row[2] else normalized_source_id,  # phone
                        "endPoint": "AI Agent",
                        "company": f"Company for {str(row[2]) if row[2] else 'Unknown'}",
                        "disposition": get_disposition_from_category(row[6] or row[10]),  # category_tx or category_gpt
                        "urgency": int(row[9]) if row[9] else (int(row[5]) if row[5] else 3),  # urgency_gpt or urgency_tx
                        "aiSummary": str(row[8] or row[4] or "No summary available"),  # summary_gpt or summary_tx
                        "commId": f"COMM-{row[0]:06d}",  # id (auto-increment)
                        "transcript": str(row[12] or ""),  # transcript
                        "subcategory": str(row[11] or row[7] or "General")  # subcat_gpt or subcat_tx
                    }
                    calls.append(call_data)
                    
                except Exception as row_error:
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

def get_disposition_from_category(category):
    """Helper function to map category to disposition"""
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
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("Listening on :%s (DB=%s)", PORT,
                 ("PostgreSQL" if DATABASE_URL.startswith("postgres") else "SQLite"))
    app.run(host="0.0.0.0", port=PORT, debug=False)