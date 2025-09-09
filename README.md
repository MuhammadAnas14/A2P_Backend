# Telnyx AI Assistant – Post-Call Pipeline with Google Calendar Integration

This project is a **Flask-based AI call assistant** that integrates **Telnyx**, **OpenAI**, and **Google Calendar**.  
It acts as a post-call pipeline for an AI-powered communication app:

- Handles incoming calls via **Telnyx Call Control**
- Provides an **IVR menu** (press 1 for AI assistant, 2 for Sales)
- Connects to a live **Telnyx AI assistant** (with transcription)
- Records calls locally in WAV format
- Runs **post-call analysis**:
  - Transcribes audio (Telnyx or Whisper fallback)
  - Summarizes and classifies calls with **OpenAI GPT**
  - Extracts urgency, category, subcategory
  - Optionally creates **Google Calendar events** from detected meeting times
- Stores metadata in **SQLite (default)** or **Postgres**
- Exposes REST APIs and **Server-Sent Events (SSE)** for a React-based communications dashboard
- Supports **knowledge base sync** by uploading files to Telnyx cloud storage and rebuilding embeddings

---

## Features

- 📞 Telnyx IVR & AI Assistant integration  
- 🎙 Dual-channel recording & Whisper transcription fallback  
- 🤖 GPT-powered classification (summary, urgency, category, subcategory)  
- 📅 Automatic Google Calendar event creation  
- 🗄 SQLite/Postgres persistence for call metadata  
- 🔌 REST APIs + SSE for frontend UI integration  
- 📚 Knowledge Base upload + embeddings rebuild  

---

## Requirements

- Python 3.10+  
- Telnyx account & API key  
- OpenAI API key  
- Google Calendar API credentials (`credentials.json` & `token.json`)  
- (Optional) PostgreSQL if not using SQLite  

---

## Environment Variables

Create a `.env` file with:

```ini
TELNYX_API_KEY=your_telnyx_api_key
TELNYX_ASSISTANT_ID=your_telnyx_assistant_id
OPENAI_API_KEY=your_openai_api_key

# Database (defaults to SQLite if not provided)
DATABASE_URL=sqlite:///call_data.db
# DATABASE_URL=postgresql://user:pass@localhost:5432/dbname

# Google Calendar paths
GOOGLE_TOKEN_PATH=token.json
GOOGLE_CRED_PATH=credentials.json

# Optional
PORT=5000
SALES_NUMBER=+17059986135
