


from __future__ import annotations
import os
import sys
import re
import base64
import json
import argparse
import pathlib
import sqlite3
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import logging

import yaml
import html2text
from dateutil import parser as dtparser

from concurrent.futures import ThreadPoolExecutor, as_completed

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google_auth_httplib2 import AuthorizedHttp
import httplib2



# Flask API
from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import atexit

import time
from dotenv import load_dotenv

try:
    from openai import OpenAI  # New SDK
except Exception:
    OpenAI = None  # We'll error politely later

# -----------------------------------------------
# Config & Environment
# -----------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
DEFAULT_TIMEZONE = os.environ.get("LOCAL_TIMEZONE", "Europe/Berlin")
DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

ROOT = pathlib.Path(__file__).resolve().parent
CRED_PATH = ROOT / "credentials.json"
TOKEN_PATH = ROOT / "token.json"
CATS_PATH = ROOT / "categories.yaml"

# Load .env next to script (safe if missing)
try:
    load_dotenv(dotenv_path=ROOT / ".env")
except Exception:
    pass

# Default run (no CLI necessary)
ENV_QUERY = os.environ.get("EMAIL_ANALYST_QUERY", "in:inbox")
ENV_MAX = int(os.environ.get("EMAIL_ANALYST_MAX", "10"))
ENV_DB = os.environ.get("EMAIL_ANALYST_DB", str(ROOT / "email_analyst.db"))
ENV_WITH_THREAD = os.environ.get("EMAIL_ANALYST_WITH_THREAD", "0").lower() in {"1","true","yes","y"}
ENV_LANGUAGE = os.environ.get("EMAIL_ANALYST_LANG", None)
ENV_MODEL = os.environ.get("OPENAI_MODEL", DEFAULT_MODEL)

# Timeouts / retries
OPENAI_TIMEOUT = float(os.environ.get("OPENAI_TIMEOUT", "45"))          # seconds
OPENAI_MAX_RETRIES = int(os.environ.get("OPENAI_MAX_RETRIES", "3"))
OPENAI_RETRY_BASE_SLEEP = float(os.environ.get("OPENAI_RETRY_BASE_SLEEP", "1.5"))
OPENAI_RETRY_MAX_SLEEP  = float(os.environ.get("OPENAI_RETRY_MAX_SLEEP", "8"))
OPENAI_MAX_TOKENS = int(os.environ.get("OPENAI_MAX_TOKENS", "350"))
GMAIL_TIMEOUT = float(os.environ.get("GMAIL_TIMEOUT", "30"))            # seconds
GMAIL_CONCURRENCY = int(os.environ.get("GMAIL_CONCURRENCY", "4"))       # parallel Gmail fetches

# OAuth sources
ENV_FORCE_REAUTH = os.environ.get("EMAIL_ANALYST_FORCE_REAUTH", "0").lower() in {"1","true","yes","y"}
GOOGLE_CRED_PATH_ENV = os.environ.get("GOOGLE_CRED_PATH")         # path to OAuth client JSON
GOOGLE_CRED_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")      # raw JSON string of OAuth client

# Debug log
DEBUG = os.environ.get("EMAIL_ANALYST_DEBUG", "0").lower() in {"1","true","yes","y"}
def dlog(*args):
    if DEBUG:
        print("[DEBUG]", *args, flush=True)
        
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("email_analyst")

# expose creds for worker threads
GMAIL_CREDS: Optional[Credentials] = None

DEFAULT_TAXONOMY_YAML = """
categories:
  - name: Sales
    subcategories: [New Lead, Quote Request, Follow-up, Appointment, Contract]
  - name: Support
    subcategories: [Bug/Issue, How-to Question, Feature Request, Outage]
  - name: Billing
    subcategories: [Invoice, Payment Issue, Refund, Quote]
  - name: Hiring/Jobs
    subcategories: [Application, Resume, Interview Scheduling]
  - name: Partnerships/Vendors
    subcategories: [Partnership, Vendor Offer, Sponsorship]
  - name: Complaint
    subcategories: [Service, Product, Staff]
  - name: Spam/Marketing
    subcategories: [Newsletter, Promo, Cold Outreach]
  - name: Personal/Internal
    subcategories: [Internal, FYI]
reply_guidance:
  tone: "polite, concise, professional"
  signoff: "Best regards"
  org_name: "Your Company"
  default_language: "English"
urgency_rubric:
  0: "Not urgent (FYI, promo, spam)."
  1: "Low (no action expected)."
  2: "Routine (respond within 2–3 days)."
  3: "Timely (respond within 24–48h)."
  4: "Same-day (time-sensitive)."
  5: "Immediate (critical)."
"""

# -----------------------------------------------
# Sender/company heuristics
# -----------------------------------------------
FREE_EMAIL_DOMAINS = {
    "gmail.com","googlemail.com","yahoo.com","ymail.com","hotmail.com","outlook.com","live.com","msn.com",
    "icloud.com","me.com","aol.com","gmx.com","gmx.de","web.de","proton.me","protonmail.com","zoho.com",
    "mail.com","fastmail.com"
}
ROLE_LOCALPARTS = {"info","support","hello","contact","sales","billing","accounts","admin","noreply","no-reply","career","careers","jobs","hr"}
SECOND_LEVEL_TLDS = {"co","com","org","net","gov","ac","edu"}
COMPANY_SUFFIX_PAT = re.compile(r"\b(?:gmbh|ag|kg|mbh|ltd|llc|inc\.?|co\.?|company|sarl|sas|bv|oy|ab|plc|s\.p\.a|s\.a\.|pte|pty|kft|sro|oyj|nv|bvba|aps|a/s)\b", re.I)
NAME_LINE_PAT = re.compile(r"^(?:[A-ZÄÖÜ][a-zäöüß]+(?: [A-ZÄÖÜ][a-zäöüß]+){1,2})$")

SIGNOFFS = [
    "regards","kind regards","best regards","cheers","thanks","thank you",
    "mit freundlichen grüßen","beste grüße","vg","mfg"
]

def _split_email_parts(addr: str) -> Tuple[str, str]:
    if not addr or "@" not in addr:
        return addr or "", ""
    local, domain = addr.split("@", 1)
    return local.strip().lower(), domain.strip().lower()

def _company_from_domain(domain: str) -> str:
    if not domain:
        return ""
    parts = domain.split(".")
    if len(parts) >= 3 and parts[-2] in SECOND_LEVEL_TLDS:
        base = parts[-3]
    elif len(parts) >= 2:
        base = parts[-2]
    else:
        base = parts[0]
    base = re.sub(r"[^a-z0-9]+", " ", base, flags=re.I).strip()
    return base.title() if base else ""

def _guess_name_from_local(local: str) -> str:
    token = re.sub(r"\d+", "", local or "").strip("._-")
    if not token or token in ROLE_LOCALPARTS:
        return ""
    parts = [p for p in re.split(r"[._-]+", token) if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0].title()
    return (parts[0].title() + " " + parts[-1].title()).strip()

def _guess_from_signature(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    idx = -1
    for i in range(len(lines)-1, -1, -1):
        low = lines[i].lower()
        if any(s in low for s in SIGNOFFS):
            idx = i
            break
    window = lines[idx+1: idx+6] if idx != -1 else lines[-6:]
    name_guess = ""
    company_guess = ""
    for l in window:
        if not name_guess and NAME_LINE_PAT.match(l):
            name_guess = l
        if not company_guess and (COMPANY_SUFFIX_PAT.search(l) or (l.isupper() and 2 <= len(l.split()) <= 5)):
            company_guess = l.title() if l.isupper() else l
    return name_guess, company_guess

def enrich_sender(sender_name_hdr: str, sender_email: str, body_text: str) -> Dict[str, Any]:
    local, domain = _split_email_parts(sender_email)
    is_free = int(domain in FREE_EMAIL_DOMAINS)
    name_local = _guess_name_from_local(local)
    name_sig, comp_sig = _guess_from_signature(body_text)
    comp_domain = _company_from_domain(domain) if not is_free else ""
    name_hdr = (sender_name_hdr or "").strip()
    name_guess = name_hdr or name_sig or name_local
    company_guess = comp_sig or comp_domain
    return {
        "sender_local": local,
        "sender_domain": domain,
        "sender_is_free": is_free,
        "sender_name_guess": name_guess,
        "sender_company_guess": company_guess,
    }

# -----------------------------------------------
# Data Models
# -----------------------------------------------
@dataclass
class EmailEnvelope:
    id: str
    thread_id: str
    subject: str
    sender_name: str
    sender_email: str
    to: List[str]
    cc: List[str]
    date_rfc2822: str
    internal_epoch_ms: int
    body_text: str
    snippet: str

    # Enriched sender metadata
    sender_local: str = ""
    sender_domain: str = ""
    sender_company_guess: str = ""
    sender_name_guess: str = ""
    sender_is_free: int = 0

    @property
    def received_dt(self) -> Optional[datetime]:
        """Parse RFC2822 Date; fall back to internalDate (ms since epoch)."""
        try:
            if self.date_rfc2822:
                return dtparser.parse(self.date_rfc2822)
        except Exception:
            pass
        try:
            if self.internal_epoch_ms:
                return datetime.fromtimestamp(self.internal_epoch_ms / 1000.0, tz=timezone.utc)
        except Exception:
            pass
        return None

@dataclass
class Analysis:
    received_datetime_iso: str
    sender: str
    sender_name: str
    recipients: List[str]
    subject: str
    summary: str
    urgency: int  # 0-5
    category: str
    subcategory: str
    confidence: float
    actionability: str
    suggested_reply: str


# -----------------------------------------------
# SQLite helpers (single-table, calls-like schema)
# -----------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS emails(
  id INTEGER PRIMARY KEY AUTOINCREMENT,

  -- unify on Gmail message id
  rec_id TEXT UNIQUE,           -- e.g., Gmail message id

  -- calls-like placeholders (not used for emails but kept for parity)
  phone TEXT,
  wav_path TEXT,

  -- lightweight pre-GPT heuristics
  summary_tx TEXT,
  urgency_tx INTEGER,
  category_tx TEXT,
  subcat_tx TEXT,

  -- main text we analyzed (email body + optional thread context)
  transcript TEXT,

  -- GPT results
  summary_gpt TEXT,
  urgency_gpt INTEGER,
  category_gpt TEXT,
  subcat_gpt TEXT,

  -- enrichment (requested)
  company_name TEXT,
  person_name  TEXT,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_emails_created_at ON emails(created_at);
CREATE INDEX IF NOT EXISTS idx_emails_urg_gpt   ON emails(urgency_gpt);
"""

def db_connect(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    conn = sqlite3.connect(path)
    return conn

def db_init(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()

def db_existing_ids(conn: sqlite3.Connection, ids: List[str]) -> set:
    """Return set of rec_ids already present so we only analyze new Gmail ids."""
    if not ids:
        return set()
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"SELECT rec_id FROM emails WHERE rec_id IN ({placeholders})",
        ids,
    ).fetchall()
    return {r[0] for r in rows}

# ---------- tiny local heuristics for *_tx ----------
def _quick_tx_signals(subject: str, body: str) -> Tuple[str, int, str, str]:
    text = f"{subject or ''}\n{body or ''}".lower()
    summary_tx = (body or subject or "").strip()[:300].strip()

    urgent_keywords = ["urgent", "asap", "immediately", "deadline", "critical", "fail", "outage"]
    high = any(k in text for k in urgent_keywords)
    urgency_tx = 4 if high else (2 if ("tomorrow" in text or "today" in text) else 1)

    if any(k in text for k in ["invoice", "payment", "refund", "quote", "pricing"]):
        category_tx, subcat_tx = "Billing", "General"
    elif any(k in text for k in ["bug", "error", "not working", "issue", "problem", "crash"]):
        category_tx, subcat_tx = "Support", "Incident"
    elif any(k in text for k in ["meeting", "schedule", "call", "availability", "demo"]):
        category_tx, subcat_tx = "Sales", "Scheduling"
    else:
        category_tx, subcat_tx = "General", "Uncategorized"

    return summary_tx, urgency_tx, category_tx, subcat_tx

# ---------- upserts that keep your existing call sites ----------
def db_upsert_email(conn: sqlite3.Connection, env: EmailEnvelope) -> None:
    summary_tx, urgency_tx, category_tx, subcat_tx = _quick_tx_signals(env.subject, env.body_text)

    sql = """
    INSERT INTO emails (
      rec_id, phone, wav_path,
      summary_tx, urgency_tx, category_tx, subcat_tx,
      transcript, company_name, person_name
    )
    VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(rec_id) DO UPDATE SET
      phone       = excluded.phone,          -- keep sender email in 'phone'
      summary_tx  = excluded.summary_tx,
      urgency_tx  = excluded.urgency_tx,
      category_tx = excluded.category_tx,
      subcat_tx   = excluded.subcat_tx,
      transcript  = excluded.transcript,
      company_name= excluded.company_name,
      person_name = excluded.person_name
    """
    conn.execute(sql, (
        env.id,
        env.sender_email or "",                         # <-- sender email → phone
        summary_tx, int(urgency_tx), category_tx, subcat_tx,
        env.body_text or "",
        env.sender_company_guess or "",
        (env.sender_name or env.sender_name_guess or "").strip(),
    ))
    conn.commit()

def db_upsert_contact(conn: sqlite3.Connection, env: EmailEnvelope) -> None:
    conn.execute(
        """
        UPDATE emails
           SET phone       = COALESCE(NULLIF(phone, ''), ?),  -- backfill if empty
               company_name = ?,
               person_name  = ?
         WHERE rec_id = ?
        """,
        (
            env.sender_email or "",
            env.sender_company_guess or "",
            (env.sender_name or env.sender_name_guess or "").strip(),
            env.id,
        ),
    )
    conn.commit()

def db_upsert_analysis(conn: sqlite3.Connection, gmail_id: str, ana: Analysis, model: str) -> None:
    """
    Keep signature: write GPT outputs onto the same row.
    """
    conn.execute(
        """
        UPDATE emails
           SET summary_gpt  = ?,
               urgency_gpt  = ?,
               category_gpt = ?,
               subcat_gpt   = ?
         WHERE rec_id = ?
        """,
        (
            ana.summary or "",
            int(ana.urgency),
            ana.category or "",
            ana.subcategory or "",
            gmail_id,
        ),
    )
    conn.commit()



# -----------------------------------------------
# Gmail Helpers
# -----------------------------------------------
def get_gmail_service() -> Any:
    """Return an authenticated Gmail service (with HTTP timeout)."""
    creds = None

    if not ENV_FORCE_REAUTH and TOKEN_PATH.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
        except Exception:
            creds = None

    if creds and not creds.valid and creds.expired and getattr(creds, "refresh_token", None) and not ENV_FORCE_REAUTH:
        try:
            creds.refresh(Request())
        except Exception:
            creds = None

    if not creds or not creds.valid or ENV_FORCE_REAUTH:
        if GOOGLE_CRED_JSON:
            try:
                cfg = json.loads(GOOGLE_CRED_JSON)
            except Exception as e:
                raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON is not valid JSON: {e}")
            flow = InstalledAppFlow.from_client_config(cfg, SCOPES)
        else:
            cred_path = GOOGLE_CRED_PATH_ENV or str(CRED_PATH)
            if not os.path.exists(cred_path):
                raise FileNotFoundError(
                    "Google OAuth client not found. Provide one of:\n"
                    "  1) Set GOOGLE_CREDENTIALS_JSON (inline JSON),\n"
                    "  2) Set GOOGLE_CRED_PATH (file path), or\n"
                    f"  3) Place a Desktop OAuth client as {CRED_PATH}."
                )
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
        try:
            creds = flow.run_local_server(port=0, prompt="consent", access_type="offline", include_granted_scopes="true")
        except Exception:
            creds = flow.run_console(prompt="consent")
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())

    # expose creds for worker threads
    global GMAIL_CREDS
    GMAIL_CREDS = creds

    http = httplib2.Http(timeout=GMAIL_TIMEOUT)
    authed_http = AuthorizedHttp(creds, http=http)
    service = build("gmail", "v1", http=authed_http, cache_discovery=False)
    return service

def gmail_list_new_ids(service, query: str, max_needed: int, conn: sqlite3.Connection) -> List[str]:
    """Page until we have up to `max_needed` ids not already in DB."""
    out: List[str] = []
    seen: set = set()
    page_token: Optional[str] = None
    while len(out) < max_needed:
        res = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=min(50, max_needed * 3),
            pageToken=page_token,
            prettyPrint=False,
        ).execute(num_retries=2)
        msgs = res.get("messages", [])
        if not msgs:
            break
        ids = [m["id"] for m in msgs if m.get("id")]
        existing = db_existing_ids(conn, ids)
        for mid in ids:
            if mid in seen or mid in existing:
                continue
            out.append(mid)
            seen.add(mid)
            if len(out) >= max_needed:
                break
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return out

def fetch_messages_concurrently(service, ids: List[str], workers: int = 4) -> Dict[str, Dict[str, Any]]:
    """Fetch Gmail messages concurrently with per-thread HTTPS (prevents TLS races)."""
    results: Dict[str, Dict[str, Any]] = {}
    if not ids:
        return results

    def fetch(mid: str):
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                http = httplib2.Http(timeout=GMAIL_TIMEOUT)
                authed = AuthorizedHttp(GMAIL_CREDS, http=http) if GMAIL_CREDS else None
                req = service.users().messages().get(userId="me", id=mid, format="full", prettyPrint=False)
                obj = req.execute(http=authed, num_retries=2) if authed else req.execute(num_retries=2)
                return mid, obj
            except Exception as e:
                last_err = e
                time.sleep(min(OPENAI_RETRY_MAX_SLEEP, OPENAI_RETRY_BASE_SLEEP * (2 ** attempt)))
        return mid, last_err

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = [ex.submit(fetch, mid) for mid in ids]
        for fut in as_completed(futures):
            mid, val = fut.result()
            if isinstance(val, Exception):
                print(f"ERROR fetching {mid}: {val}", file=sys.stderr)
            else:
                results[mid] = val
    return results

def gmail_get_thread(service, thread_id: str) -> Dict[str, Any]:
    return service.users().threads().get(userId="me", id=thread_id, format="full").execute(num_retries=2)

def _b64url_decode(data: str) -> bytes:
    data += "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data.encode("utf-8"))

def _headers_to_dict(headers: List[Dict[str, str]]) -> Dict[str, str]:
    return {h.get("name", "").lower(): h.get("value", "") for h in headers}

def _parse_addr(addr: str) -> Tuple[str, str]:
    import email.utils
    name, email = email.utils.parseaddr(addr or "")
    return name or "", email or ""

def _extract_recipients(raw: str) -> List[str]:
    import email.utils
    addrs = email.utils.getaddresses([raw or ""]) if raw else []
    return [a[1] for a in addrs if a[1]]

def extract_body_text(payload: Dict[str, Any]) -> str:
    """Traverse MIME parts. Prefer text/plain; else convert text/html → markdownish text."""
    texts: List[str] = []

    def walk(part: Dict[str, Any]):
        mime = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data")
        parts = part.get("parts")

        if parts:
            for p in parts:
                walk(p)
            return

        raw = ""
        if data:
            try:
                raw = _b64url_decode(data).decode("utf-8", errors="ignore")
            except Exception:
                raw = ""

        if mime.startswith("text/plain"):
            texts.append(raw)
        elif mime.startswith("text/html"):
            h = html2text.HTML2Text()
            h.ignore_links = False
            h.ignore_images = True
            h.body_width = 0
            texts.append(h.handle(raw))
        elif mime == "message/rfc822" and body.get("attachmentId") is None and parts:
            for p in parts:
                walk(p)

    walk(payload)
    return "\n\n".join([t for t in texts if t]).strip() or ""

def parse_message(obj: Dict[str, Any]) -> EmailEnvelope:
    headers = _headers_to_dict(obj.get("payload", {}).get("headers", []))
    subject = headers.get("subject", "")
    date_hdr = headers.get("date", obj.get("internalDate"))
    from_hdr = headers.get("from", "")
    to_hdr = headers.get("to", "")
    cc_hdr = headers.get("cc", "")
    sender_name, sender_email = _parse_addr(from_hdr)
    to_list = _extract_recipients(to_hdr)
    cc_list = _extract_recipients(cc_hdr)

    body_text = extract_body_text(obj.get("payload", {})) or obj.get("snippet", "")

    enrich = enrich_sender(sender_name, sender_email, body_text)

    return EmailEnvelope(
        id=obj.get("id", ""),
        thread_id=obj.get("threadId", ""),
        subject=subject or "(no subject)",
        sender_name=sender_name,
        sender_email=sender_email,
        to=to_list,
        cc=cc_list,
        date_rfc2822=date_hdr or "",
        internal_epoch_ms=int(obj.get("internalDate", "0")),
        body_text=body_text,
        snippet=obj.get("snippet", ""),
        sender_local=enrich["sender_local"],
        sender_domain=enrich["sender_domain"],
        sender_company_guess=enrich["sender_company_guess"],
        sender_name_guess=enrich["sender_name_guess"],
        sender_is_free=enrich["sender_is_free"],
    )

# -----------------------------------------------
# OpenAI Analysis
# -----------------------------------------------
PROMPT_TEMPLATE = """
You are an expert email triage assistant for a busy operations + sales team.

TASKS
1) Read the inbound email.
2) Summarize it in 2–4 sentences.
3) Assign an urgency from 0–5 using this rubric:
   0 Not urgent (FYI/promo/spam)
   1 Low (no action expected)
   2 Routine (respond in 2–3 days)
   3 Timely (respond within 24–48h)
   4 Same-day (time-sensitive)
   5 Immediate (critical)
4) Choose the best category and subcategory from the provided taxonomy.
5) Propose a concise, professional reply that resolves or moves the request forward. If scheduling is mentioned, suggest concrete windows and ask for confirmation. If you need more info, ask 2–4 specific questions.

CONSTRAINTS
- Output must be STRICT JSON only. No markdown. No prose. Use the schema keys exactly.
- "urgency" must be an integer 0–5.
- "category" and "subcategory" MUST come from the taxonomy below; if nothing fits, use "Other" for category and "General" for subcategory.
- Keep reply within ~8–12 lines, using the desired language.

INPUT
- Subject: {subject}
- From: {sender_name} <{sender_email}>
- To: {to}
- CC: {cc}
- Received (localized): {received_local}
- Organization: {org_name}
- Desired reply language: {language}
- Taxonomy: {taxonomy}
- Email Body (may be truncated):
"""

SCHEMA_KEYS = [
    "received_datetime_iso",
    "sender",
    "sender_name",
    "recipients",
    "subject",
    "summary",
    "urgency",
    "category",
    "subcategory",
    "confidence",
    "actionability",
    "suggested_reply",
]

# Heuristic to drop quoted trails & disclaimers → fewer tokens/timeouts
RE_QUOTED = re.compile(r"(?im)^(?:>+|On .+wrote:|From: .+|-----Original Message-----|Begin forwarded message:).*$")

def strip_reply_trail(text: str) -> str:
    if not text:
        return ""
    out_lines = []
    for line in text.splitlines():
        if RE_QUOTED.match(line.strip()):
            break
        out_lines.append(line)
    return "\n".join(out_lines).strip()

def ensure_taxonomy_file() -> Dict[str, Any]:
    if not CATS_PATH.exists():
        CATS_PATH.write_text(DEFAULT_TAXONOMY_YAML.strip(), encoding="utf-8")
    return yaml.safe_load(CATS_PATH.read_text(encoding="utf-8"))

def truncate_text(s: str, max_chars: int = 12000) -> str:
    s = s or ""
    if len(s) <= max_chars:
        return s
    half = max_chars // 2
    return s[:half] + "\n\n...[TRUNCATED]...\n\n" + s[-half:]

def to_local_iso(dt: Optional[datetime], tz: str = DEFAULT_TIMEZONE) -> str:
    if not dt:
        return ""
    try:
        local = dt.astimezone(ZoneInfo(tz))
    except Exception:
        local = dt
    return local.isoformat()

def _as_int(x, default: int) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default

def _as_float(x, default: float) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _as_str(x, default: str = "") -> str:
    return default if x is None else str(x)

def _as_list(x, default=None):
    if default is None:
        default = []
    if x is None:
        return default
    return x if isinstance(x, list) else [str(x)]

def openai_analyze(env: EmailEnvelope, taxonomy: Dict[str, Any], language: str, model: str) -> Analysis:
    if OpenAI is None:
        raise RuntimeError("openai package not available. pip install openai")

    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set (add it to .env or your shell).")

    client = OpenAI(api_key=key)

    received_local = to_local_iso(env.received_dt, DEFAULT_TIMEZONE)
    org_name = taxonomy.get("reply_guidance", {}).get("org_name", "Your Company")
    taxonomy_compact = [
        {"category": c.get("name"), "subcategories": c.get("subcategories", [])}
        for c in taxonomy.get("categories", [])
    ]

    prompt = PROMPT_TEMPLATE.format(
        subject=env.subject,
        sender_name=env.sender_name or env.sender_email,
        sender_email=env.sender_email,
        to=", ".join(env.to) if env.to else "",
        cc=", ".join(env.cc) if env.cc else "",
        received_local=received_local,
        org_name=org_name,
        language=language,
        taxonomy=json.dumps(taxonomy_compact, ensure_ascii=False),
    )

    # Adaptive body shrinking across retries
    BODY_LIMITS = [4000, 1500, 700]

    last_err = None
    for attempt in range(OPENAI_MAX_RETRIES):
        body_limit = BODY_LIMITS[min(attempt, len(BODY_LIMITS)-1)]
        cleaned = strip_reply_trail(env.body_text)
        body_text = truncate_text(cleaned, body_limit)

        messages = [
            {"role": "system", "content": "You return only strict JSON that matches the requested keys."},
            {"role": "user", "content": prompt + body_text},
            {"role": "user", "content": json.dumps({"schema_keys": SCHEMA_KEYS})},
        ]

        try:
            resp = client.chat.completions.create(
                model=model,
                temperature=0.2,
                messages=messages,
                response_format={"type": "json_object"},
                max_tokens=OPENAI_MAX_TOKENS,
                timeout=OPENAI_TIMEOUT,
            )
            content = resp.choices[0].message.content
            try:
                data = json.loads(content) if content else {}
            except Exception:
                data = {}

            ana = Analysis(
                received_datetime_iso=_as_str(data.get("received_datetime_iso"), received_local),
                sender=_as_str(env.sender_email),
                sender_name=_as_str(env.sender_name) or _as_str(data.get("sender_name"), ""),
                recipients=_as_list(env.to, []),
                subject=_as_str(env.subject, "(no subject)"),
                summary=_as_str(data.get("summary"), ""),
                urgency=_as_int(data.get("urgency"), 2),
                category=_as_str(data.get("category"), "Other"),
                subcategory=_as_str(data.get("subcategory"), "General"),
                confidence=_as_float(data.get("confidence"), 0.7),
                actionability=_as_str(data.get("actionability"), "Respond appropriately"),
                suggested_reply=_as_str(data.get("suggested_reply"), ""),
            )
            return ana
        except Exception as e:
            last_err = e
            time.sleep(min(OPENAI_RETRY_MAX_SLEEP, OPENAI_RETRY_BASE_SLEEP * (2 ** attempt)))

    raise RuntimeError(f"LLM request failed after {OPENAI_MAX_RETRIES} retries: {last_err}")

# -----------------------------------------------
# Thread helpers (optional)
# -----------------------------------------------
def hydrate_thread_text(service, thread_id: str, max_messages: int = 10) -> str:
    th = gmail_get_thread(service, thread_id)
    msgs = th.get("messages", [])[-max_messages:]
    blocks = []
    for m in msgs:
        hdrs = _headers_to_dict(m.get("payload", {}).get("headers", []))
        who = hdrs.get("from", "")
        when = hdrs.get("date", "")
        subj = hdrs.get("subject", "")
        body = extract_body_text(m.get("payload", {}))
        blocks.append(f"From: {who}\nDate: {when}\nSubject: {subj}\n\n{body}\n\n---\n")
    return "\n".join(blocks)

# -----------------------------------------------
# CLI / Main
# -----------------------------------------------
def analyze_one(service, message_id: str, taxonomy: Dict[str, Any], language: str, model: str, with_thread: bool) -> Tuple[EmailEnvelope, Analysis]:
    raw = service.users().messages().get(userId="me", id=message_id, format="full", prettyPrint=False).execute(num_retries=2)
    env = parse_message(raw)

    if with_thread and env.thread_id:
        thread_text = hydrate_thread_text(service, env.thread_id, max_messages=10)
        env.body_text = env.body_text + "\n\n[THREAD CONTEXT]\n" + truncate_text(thread_text, 8000)

    ana = openai_analyze(env, taxonomy, language=language, model=model)
    return env, ana

def main():
    # Default mode (no CLI args): analyze top N *new* emails
    if len(sys.argv) == 1:
        taxonomy = ensure_taxonomy_file()
        language = ENV_LANGUAGE or taxonomy.get("reply_guidance", {}).get("default_language", "English")
        model = ENV_MODEL
        service = get_gmail_service()

        conn = db_connect(ENV_DB)
        db_init(conn)

        ids = gmail_list_new_ids(service, ENV_QUERY, ENV_MAX, conn)
        if not ids:
            print("No new emails to analyze (based on IDs already in DB).")
            conn.close()
            return

        # Fetch Gmail messages concurrently (thread-safe HTTPS per worker)
        raw_map = fetch_messages_concurrently(service, ids, workers=GMAIL_CONCURRENCY)

        errors = False
        for mid in ids:  # keep original order
            raw = raw_map.get(mid)
            if raw is None or isinstance(raw, Exception):
                errors = True
                print(f"ERROR analyzing {mid}: failed to fetch message", file=sys.stderr)
                continue
            try:
                env = parse_message(raw)
                if ENV_WITH_THREAD and env.thread_id:
                    thread_text = hydrate_thread_text(service, env.thread_id, max_messages=10)
                    env.body_text = env.body_text + "\n\n[THREAD CONTEXT]\n" + truncate_text(thread_text, 8000)

                ana = openai_analyze(env, taxonomy, language=language, model=model)
                with conn:
                    db_upsert_email(conn, env)
                    db_upsert_contact(conn, env)
                    db_upsert_analysis(conn, env.id, ana, model)

                print(f"saved {env.id} | {env.subject[:80]} | urgent={ana.urgency} | {ana.category}/{ana.subcategory}")
            except Exception as e:
                errors = True
                print(f"ERROR analyzing {mid}: {e}", file=sys.stderr)
        conn.close()
        if errors:
            sys.exit(1)
        return

    # CLI path (advanced)
    ap = argparse.ArgumentParser(description="Analyze inbound Gmail and persist results to SQLite.")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--query", help="Gmail search query (e.g., 'in:inbox newer_than:7d')")
    g.add_argument("--id", help="Analyze a specific Gmail message id")
    g.add_argument("--thread", help="Analyze the most recent message in a specific thread id")

    ap.add_argument("--max", type=int, default=10, help="Max messages to analyze (with --query)")
    ap.add_argument("--with-thread", action="store_true", help="Append recent thread context to the body")
    ap.add_argument("--timezone", default=DEFAULT_TIMEZONE, help="Local timezone for display")
    ap.add_argument("--language", default=None, help="Reply language (default from categories.yaml)")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="OpenAI model (env OPENAI_MODEL)")
    ap.add_argument("--db", default=str(ROOT / "email_analyst.db"), help="SQLite DB path (will be created if missing)")
    ap.add_argument("--print", action="store_true", help="Also print a one-line summary per email")

    args = ap.parse_args()

    taxonomy = ensure_taxonomy_file()
    if not args.language:
        args.language = taxonomy.get("reply_guidance", {}).get("default_language", "English")

    service = get_gmail_service()

    ids: List[str] = []
    if args.id:
        ids = [args.id]
    elif args.thread:
        th = gmail_get_thread(service, args.thread)
        msgs = th.get("messages", [])
        if not msgs:
            print("No messages in thread", file=sys.stderr)
            sys.exit(2)
        ids = [msgs[-1]["id"]]
    else:
        query = args.query or "in:inbox"
        # list more, then keep new ones only
        res = service.users().messages().list(userId="me", q=query, maxResults=max(args.max * 5, 50), prettyPrint=False).execute(num_retries=2)
        ids_all = [m["id"] for m in res.get("messages", [])]
        conn_tmp = db_connect(args.db)
        db_init(conn_tmp)
        unseen_set = set(ids_all) - db_existing_ids(conn_tmp, ids_all)
        conn_tmp.close()
        ids = [mid for mid in ids_all if mid in unseen_set][:args.max]
        if not ids:
            print("No new messages matched query", file=sys.stderr)
            sys.exit(0)

    conn = db_connect(args.db)
    db_init(conn)

    any_error = False
    for mid in ids:
        try:
            env, ana = analyze_one(service, mid, taxonomy, args.language, args.model, args.with_thread)
            with conn:
                db_upsert_email(conn, env)
                db_upsert_contact(conn, env)
                db_upsert_analysis(conn, env.id, ana, args.model)
            if args.print:
                print(f"saved {env.id} | {env.subject[:80]} | urgent={ana.urgency} | {ana.category}/{ana.subcategory}")
        except Exception as e:
            any_error = True
            print(f"ERROR analyzing {mid}: {e}", file=sys.stderr)

    conn.close()
    if any_error:
        sys.exit(1)

# if __name__ == "__main__":
#     main()

# ────────────────────────────── Flask API ───────────────────────────────

email_app = Flask(__name__)
CORS(email_app)

import threading
thread_local = threading.local()

def get_email_db_connection():
    """Get a thread-local database connection"""
    if not hasattr(thread_local, 'connection'):
        thread_local.connection = db_connect(ENV_DB)
        db_init(thread_local.connection)
    return thread_local.connection

def cleanup_email_service():
    """Cleanup email service resources"""
    if hasattr(thread_local, 'connection'):
        thread_local.connection.close()
        log.info("Email service database connection closed")

atexit.register(cleanup_email_service)


@email_app.route('/api/emails', methods=['GET'])
def get_all_emails():
    """Get all emails from the database"""
    try:
        conn = get_email_db_connection()
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, rec_id, phone, wav_path, summary_tx, urgency_tx, 
                   category_tx, subcat_tx, transcript, summary_gpt, urgency_gpt, 
                   category_gpt, subcat_gpt, company_name, person_name, created_at
            FROM emails 
            ORDER BY created_at DESC
        """)
        
        rows = cursor.fetchall()
        emails = []
        
        for row in rows:
            email_data = {
                "commId": f"COMM-{row[0]:06d}",  # id (auto-increment) # Use auto-increment id as commId
                'type': 'email',
                'direction': 'inbound',
                'sourceId': row[2],  # phone field contains email address
                'subject': row[8].split('\n')[0].replace('Subject: ', '') if row[8] and row[8].startswith('Subject:') else '',
                'body': '\n'.join(row[8].split('\n')[2:]) if row[8] and row[8].startswith('Subject:') else row[8] or '',
                'summary': row[9] or row[4] or '',  # Prefer GPT summary, fallback to TX
                'urgency': row[10] or row[5] or 1,  # Prefer GPT urgency, fallback to TX
                'category': row[11] or row[6] or '',  # Prefer GPT category, fallback to TX
                'subcategory': row[12] or row[7] or '',  # Prefer GPT subcategory, fallback to TX
                'companyName': row[13] or '',
                'personName': row[14] or '',
                'timestamp': row[15] or '',
                'duration': 0,  # Emails don't have duration
                'status': 'completed'
            }
            emails.append(email_data)
        
        log.info("Fetched %d emails successfully", len(emails))
        return jsonify(emails)
    
    except Exception as e:
        log.error("Error fetching emails: %s", e)
        return jsonify({'error': str(e)}), 500

@email_app.route('/api/emails/<source_id>', methods=['GET'])
def get_emails_by_source(source_id):
    """Get emails filtered by source ID (email address)"""
    try:
        conn = get_email_db_connection()
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, rec_id, phone, wav_path, summary_tx, urgency_tx, 
                   category_tx, subcat_tx, transcript, summary_gpt, urgency_gpt, 
                   category_gpt, subcat_gpt, company_name, person_name, created_at
            FROM emails 
            WHERE phone = ? OR phone LIKE ?
            ORDER BY created_at DESC
        """, (source_id, f"%{source_id}%"))
        
        rows = cursor.fetchall()
        emails = []
        
        for row in rows:
            email_data = {
                'commId': row[0],  # Use auto-increment id as commId
                'type': 'In',
                'direction': 'inbound',
                'sourceId': row[2],  # phone field contains email address
                'subject': row[8].split('\n')[0].replace('Subject: ', '') if row[8] and row[8].startswith('Subject:') else '',
                'body': '\n'.join(row[8].split('\n')[2:]) if row[8] and row[8].startswith('Subject:') else row[8] or '',
                'summary': row[9] or row[4] or '',  # Prefer GPT summary, fallback to TX
                'urgency': row[10] or row[5] or 1,  # Prefer GPT urgency, fallback to TX
                'category': row[11] or row[6] or '',  # Prefer GPT category, fallback to TX
                'subcategory': row[12] or row[7] or '',  # Prefer GPT subcategory, fallback to TX
                'companyName': row[13] or '',
                'personName': row[14] or '',
                'timestamp': row[15] or '',
                'duration': 0,  # Emails don't have duration
                'status': 'completed'
            }
            emails.append(email_data)
        
        log.info("Found %d emails for source_id: %s", len(emails), source_id)
        return jsonify(emails)
    
    except Exception as e:
        log.error("Error fetching emails for source %s: %s", source_id, e)
        return jsonify({'error': str(e)}), 500

@email_app.route('/api/emails/sync', methods=['POST'])
def sync_emails():
    """Manually trigger email sync"""
    try:
        # Run email analysis in background thread
        def run_sync():
            try:
                conn = get_email_db_connection()
                
                # Run the email analysis pipeline
                creds = get_gmail_creds()
                service = build_gmail_service(creds)
                client = openai_client()
                
                # Get recent emails (last 24 hours)
                query = "in:inbox newer_than:1d"
                ids = gmail_search_ids(service, query, 50)
                
                processed = 0
                for mid in ids:
                    try:
                        msg = gmail_get_message(service, mid)
                        row = process_message_record(conn, client, msg, with_thread=False)
                        if row:
                            processed += 1
                    except Exception as e:
                        log.error("Failed processing email %s: %s", mid, e)
                    time.sleep(0.3)  # Rate limiting
                
                log.info("Email sync completed. Processed %d emails", processed)
                
            except Exception as e:
                log.error("Email sync error: %s", e)
        
        # Start sync in background
        sync_thread = threading.Thread(target=run_sync)
        sync_thread.daemon = True
        sync_thread.start()
        
        return jsonify({'message': 'Email sync started', 'status': 'processing'})
    
    except Exception as e:
        log.error("Error starting email sync: %s", e)
        return jsonify({'error': str(e)}), 500

def run_email_service(host='localhost', port=5001, debug=False):
    """Run the email service Flask app"""
    log.info("Starting email service on %s:%d", host, port)
    email_app.run(host=host, port=port, debug=debug, threaded=True)


# if __name__ == "__main__":
#     try:
#         main(sys.argv[1:])
#     except KeyboardInterrupt:
#         print("\nInterrupted.")

if __name__ == "__main__":
    import sys
    
    # Check if running as service
    if len(sys.argv) > 1 and sys.argv[1] == '--service':
        run_email_service(debug=True)
    else:
        # Original CLI functionality
        try:
            main()
        except KeyboardInterrupt:
            print("\nInterrupted.")
