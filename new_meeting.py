"""
Single-file Fathom -> Airtable webhook handler.

Deploy/start with Functions Framework:
  functions-framework --source new_meeting.py --target fathom_webhook --port $PORT

Behavior:
- POST / : expects JSON containing recording_id or call_id
- GET /  : returns last received payload (instance-local, best-effort), optionally protected by WEBHOOK_DEBUG_TOKEN

Required env vars:
- FATHOM_API_KEY
- AIRTABLE_API_KEY
- AIRTABLE_BASE_ID
- AIRTABLE_MEETINGS_TABLE
- AIRTABLE_ACTION_ITEMS_TABLE

Optional env vars:
- AIRTABLE_PARTICIPANTS_TABLE (default: People)
- CONNECT_TIMEOUT_S (default: 5)
- READ_TIMEOUT_S (default: 25)
- WEBHOOK_DEBUG_TOKEN (protects GET /)
- WEBHOOK_PAYLOAD_STORE_PATH (default: /tmp/last_webhook_payload.json)
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import functions_framework
import requests
from dotenv import load_dotenv
from flask import jsonify


# Load environment variables from .env when running locally
load_dotenv()


# --- Configuration ---
CONNECT_TIMEOUT_S = float(os.environ.get("CONNECT_TIMEOUT_S", "5"))
READ_TIMEOUT_S = float(os.environ.get("READ_TIMEOUT_S", "25"))
TIMEOUT = (CONNECT_TIMEOUT_S, READ_TIMEOUT_S)

FATHOM_API_KEY = os.environ.get("FATHOM_API_KEY")
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID")
AIRTABLE_MEETINGS_TABLE = os.environ.get("AIRTABLE_MEETINGS_TABLE")
AIRTABLE_ACTION_ITEMS_TABLE = os.environ.get("AIRTABLE_ACTION_ITEMS_TABLE")
AIRTABLE_PARTICIPANTS_TABLE = os.environ.get("AIRTABLE_PARTICIPANTS_TABLE", "People")

WEBHOOK_DEBUG_TOKEN = os.environ.get("WEBHOOK_DEBUG_TOKEN")
WEBHOOK_PAYLOAD_STORE_PATH = os.environ.get("WEBHOOK_PAYLOAD_STORE_PATH", "/tmp/last_webhook_payload.json")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(event: str, **fields: Any) -> None:
    """
    Structured logging for Cloud Run. Prints one-line JSON for easy filtering.
    """
    record = {"event": event, "ts": time.time(), **fields}
    try:
        print(json.dumps(record, ensure_ascii=False), flush=True)
    except Exception:
        print(f"{event} {fields}", flush=True)


def save_last_payload(*, payload: Any, meta: Optional[Dict[str, Any]] = None) -> None:
    record: Dict[str, Any] = {"received_at": _utc_now_iso(), "payload": payload}
    if meta:
        record["meta"] = meta

    parent_dir = os.path.dirname(WEBHOOK_PAYLOAD_STORE_PATH) or "."
    os.makedirs(parent_dir, exist_ok=True)

    with tempfile.NamedTemporaryFile("w", delete=False, dir=parent_dir, encoding="utf-8") as tmp:
        json.dump(record, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = tmp.name
    os.replace(tmp_path, WEBHOOK_PAYLOAD_STORE_PATH)


def load_last_payload() -> Optional[Dict[str, Any]]:
    if not os.path.exists(WEBHOOK_PAYLOAD_STORE_PATH):
        return None
    try:
        with open(WEBHOOK_PAYLOAD_STORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


@functions_framework.http
def fathom_webhook(request):
    """
    HTTP handler:
    - POST: receives {recording_id/call_id}, fetches details from Fathom, writes to Airtable
    - GET: returns last received payload (best-effort), optionally protected by WEBHOOK_DEBUG_TOKEN
    """

    debug_request_id = request.headers.get("X-Debug-Request-Id") or request.headers.get("X-Request-Id")
    trace = request.headers.get("X-Cloud-Trace-Context")
    started_at = time.time()

    if request.method == "GET":
        provided = (
            request.headers.get("X-Debug-Token")
            or request.headers.get("X-Webhook-Debug-Token")
            or request.args.get("token")
        )
        if WEBHOOK_DEBUG_TOKEN and provided != WEBHOOK_DEBUG_TOKEN:
            return jsonify({"error": "Unauthorized"}), 401

        last = load_last_payload()
        if not last:
            return jsonify({"status": "empty"}), 404
        return jsonify(last), 200

    if request.method != "POST":
        return jsonify({"error": "Only POST requests are allowed"}), 405

    stage = "start"
    try:
        _log(
            "webhook_start",
            debug_request_id=debug_request_id,
            trace=trace,
            method=request.method,
            content_type=request.headers.get("Content-Type"),
        )

        missing = []
        if not FATHOM_API_KEY:
            missing.append("FATHOM_API_KEY")
        if not AIRTABLE_API_KEY:
            missing.append("AIRTABLE_API_KEY")
        if not AIRTABLE_BASE_ID:
            missing.append("AIRTABLE_BASE_ID")
        if not AIRTABLE_MEETINGS_TABLE:
            missing.append("AIRTABLE_MEETINGS_TABLE")
        if not AIRTABLE_ACTION_ITEMS_TABLE:
            missing.append("AIRTABLE_ACTION_ITEMS_TABLE")
        if missing:
            _log("config_missing", debug_request_id=debug_request_id, missing=missing)
            return jsonify({"error": "Missing required environment variables", "missing": missing}), 500

        stage = "parse_payload"
        _log("stage", debug_request_id=debug_request_id, stage=stage)
        webhook_data = request.get_json(silent=True)
        if not webhook_data:
            return jsonify({"error": "No data received", "stage": stage}), 400

        recording_id = webhook_data.get("recording_id") or webhook_data.get("call_id")
        if not recording_id:
            return jsonify({"error": "No recording_id or call_id in webhook data", "stage": stage}), 400

        # Best-effort persistence for debugging (instance-local)
        try:
            safe_headers = {}
            for k in ["Content-Type", "User-Agent", "X-Forwarded-For", "X-Forwarded-Proto", "X-Cloud-Trace-Context"]:
                v = request.headers.get(k)
                if v:
                    safe_headers[k] = v
            save_last_payload(
                payload=webhook_data,
                meta={
                    "debug_request_id": debug_request_id,
                    "recording_id": str(recording_id),
                    "headers": safe_headers,
                },
            )
        except Exception:
            pass

        _log("recording_received", debug_request_id=debug_request_id, recording_id=str(recording_id))

        stage = "fetch_fathom_call_data"
        t0 = time.time()
        _log("stage", debug_request_id=debug_request_id, stage=stage)
        call_data = fetch_fathom_call_data(recording_id)
        _log("stage_done", debug_request_id=debug_request_id, stage=stage, duration_s=round(time.time() - t0, 3))
        if not call_data:
            return jsonify({"error": "Failed to fetch call data from Fathom", "stage": stage}), 500

        stage = "upload_meeting_to_airtable"
        t0 = time.time()
        _log("stage", debug_request_id=debug_request_id, stage=stage)
        meeting_record = upload_meeting_to_airtable(call_data)
        _log("stage_done", debug_request_id=debug_request_id, stage=stage, duration_s=round(time.time() - t0, 3))
        if not meeting_record:
            return jsonify({"error": "Failed to upload meeting to Airtable", "stage": stage}), 500

        meeting_record_id = meeting_record.get("id")

        stage = "create_action_items"
        t0 = time.time()
        _log("stage", debug_request_id=debug_request_id, stage=stage)
        action_items_created = create_action_items(
            call_data.get("action_items", []),
            meeting_record_id,
            call_data.get("title"),
        )
        _log("stage_done", debug_request_id=debug_request_id, stage=stage, duration_s=round(time.time() - t0, 3))

        _log(
            "webhook_success",
            debug_request_id=debug_request_id,
            recording_id=str(recording_id),
            meeting_record_id=meeting_record_id,
            action_items_created=action_items_created,
            total_duration_s=round(time.time() - started_at, 3),
        )

        return (
            jsonify(
                {
                    "status": "success",
                    "message": "Recording synced to Airtable",
                    "recording_id": recording_id,
                    "meeting_record_id": meeting_record_id,
                    "action_items_created": action_items_created,
                    "debug_request_id": debug_request_id,
                }
            ),
            200,
        )

    except Exception as e:
        _log("webhook_error", debug_request_id=debug_request_id, stage=stage, error=str(e))
        return jsonify({"error": str(e), "stage": stage, "debug_request_id": debug_request_id}), 500


def fetch_fathom_call_data(recording_id: Any) -> Optional[Dict[str, Any]]:
    url = "https://api.fathom.ai/external/v1/meetings"
    headers = {"X-Api-Key": FATHOM_API_KEY, "Content-Type": "application/json"}
    params = {"include_action_items": "true", "include_summary": "true", "include_transcript": "true"}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()

        meeting = None
        for item in data.get("items", []):
            if str(item.get("recording_id")) == str(recording_id):
                meeting = item
                break

        if not meeting:
            cursor = data.get("next_cursor")
            while cursor and not meeting:
                params["cursor"] = cursor
                response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
                response.raise_for_status()
                page_data = response.json()
                for item in page_data.get("items", []):
                    if str(item.get("recording_id")) == str(recording_id):
                        meeting = item
                        break
                cursor = page_data.get("next_cursor")

        if not meeting:
            _log("fathom_meeting_not_found", recording_id=str(recording_id))
            return None

        summary_text = ""
        if meeting.get("default_summary"):
            summary_text = meeting["default_summary"].get("markdown_formatted", "")

        participants = [invitee.get("name", invitee.get("email", "")) for invitee in meeting.get("calendar_invitees", [])]

        action_items = []
        for item in meeting.get("action_items", []):
            action_item = {"description": item.get("description", ""), "assignee": None}
            if item.get("assignee"):
                action_item["assignee"] = item["assignee"].get("name")
            action_items.append(action_item)

        duration = None
        if meeting.get("recording_start_time") and meeting.get("recording_end_time"):
            try:
                start = datetime.fromisoformat(meeting["recording_start_time"].replace("Z", "+00:00"))
                end = datetime.fromisoformat(meeting["recording_end_time"].replace("Z", "+00:00"))
                duration = int((end - start).total_seconds())
            except Exception:
                pass

        recording_url = meeting.get("url")
        share_url = meeting.get("share_url")
        embed_url = meeting.get("share_url").replace("/share/", "/embed/") if share_url else None

        # Extract transcript text and URL
        transcript_text = ""
        transcript_url = None
        if meeting.get("transcript"):
            # Transcript can be a string or object with text/url
            transcript_data = meeting.get("transcript")
            if isinstance(transcript_data, dict):
                transcript_text = transcript_data.get("text", "") or transcript_data.get("markdown_formatted", "")
                transcript_url = transcript_data.get("url")
            elif isinstance(transcript_data, str):
                transcript_text = transcript_data
        
        # Fallback: check for transcript_url field directly
        if not transcript_url and meeting.get("transcript_url"):
            transcript_url = meeting.get("transcript_url")

        return {
            "call_id": str(recording_id),
            "recording_id": recording_id,
            "meeting_id": meeting.get("id"),
            "title": meeting.get("title") or meeting.get("meeting_title", "Untitled Meeting"),
            "start_time": meeting.get("recording_start_time") or meeting.get("scheduled_start_time"),
            "end_time": meeting.get("recording_end_time") or meeting.get("scheduled_end_time"),
            "duration": duration,
            "recording_url": recording_url,
            "embed_url": embed_url,
            "share_url": share_url,
            "summary": summary_text,
            "action_items": action_items,
            "participants": participants,
            "transcript_url": transcript_url,
            "transcript": transcript_text,
        }

    except requests.exceptions.RequestException as e:
        _log("fathom_request_error", recording_id=str(recording_id), error=str(e))
        if hasattr(e, "response") and e.response is not None:
            _log("fathom_response", status=e.response.status_code, body=e.response.text[:5000])
        return None


def get_linked_record_fields(table_name: str) -> set[str]:
    try:
        url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"
        headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        if response.status_code != 200:
            return set()
        data = response.json()
        for table in data.get("tables", []):
            if table.get("name") == table_name:
                linked_fields = set()
                for field in table.get("fields", []):
                    if field.get("type") in ["multipleRecordLinks", "singleCollaborator", "multipleCollaborators"]:
                        linked_fields.add(field.get("name"))
                return linked_fields
    except Exception as e:
        _log("airtable_schema_error", table=table_name, error=str(e))
    return set()


def reformat_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return name
    name = name.strip()
    if "," in name:
        parts = [part.strip() for part in name.split(",", 1)]
        if len(parts) == 2:
            return f"{parts[1]} {parts[0]}"
    return name


def find_or_create_participant(participant_name: str) -> Optional[str]:
    if not participant_name:
        return None

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_PARTICIPANTS_TABLE}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}

    escaped_name = participant_name.replace("'", "''")
    formula = f"{{Name}} = '{escaped_name}'"
    params = {"filterByFormula": formula, "maxRecords": 1}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
        if response.status_code == 200:
            data = response.json()
            if data.get("records"):
                return data["records"][0]["id"]

        schema_url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"
        schema_response = requests.get(schema_url, headers=headers, timeout=TIMEOUT)
        name_field = "Name"
        if schema_response.status_code == 200:
            schema_data = schema_response.json()
            for table in schema_data.get("tables", []):
                if table.get("name") == AIRTABLE_PARTICIPANTS_TABLE:
                    for field in table.get("fields", []):
                        if field.get("type") in ["singleLineText", "email"]:
                            name_field = field.get("name")
                            break
                    break

        create_data = {"fields": {name_field: participant_name}}
        create_response = requests.post(url, headers=headers, json=create_data, timeout=TIMEOUT)
        if create_response.status_code == 200:
            return create_response.json().get("id")

        _log("airtable_create_participant_failed", status=create_response.status_code, body=create_response.text[:5000])
        return None

    except Exception as e:
        _log("airtable_participant_error", name=participant_name, error=str(e))
        return None


def upload_meeting_to_airtable(call_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_MEETINGS_TABLE}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}

    linked_record_fields = get_linked_record_fields(AIRTABLE_MEETINGS_TABLE)

    participant_names = [
        reformat_name(str(p.get("name", p) if isinstance(p, dict) else p)) for p in call_data.get("participants", [])
    ]

    participant_record_ids: list[str] = []
    if "Participants" in linked_record_fields and participant_names:
        for participant_name in participant_names:
            rid = find_or_create_participant(participant_name)
            if rid:
                participant_record_ids.append(rid)

    fields: Dict[str, Any] = {}
    if call_data.get("title") and "Title" not in linked_record_fields:
        fields["Title"] = str(call_data.get("title"))
    if call_data.get("recording_url") and "Recording URL" not in linked_record_fields:
        fields["Recording URL"] = str(call_data.get("recording_url"))
    if call_data.get("embed_url") and "Embed URL" not in linked_record_fields:
        fields["Embed URL"] = str(call_data.get("embed_url"))
    if call_data.get("share_url") and "Share URL" not in linked_record_fields:
        fields["Share URL"] = str(call_data.get("share_url"))
    if call_data.get("summary") and "Summary" not in linked_record_fields:
        fields["Summary"] = str(call_data.get("summary"))
    if call_data.get("start_time") and "Start Time" not in linked_record_fields:
        fields["Start Time"] = str(call_data.get("start_time"))

    duration = call_data.get("duration")
    if duration is not None and "Duration" not in linked_record_fields:
        try:
            fields["Duration"] = int(duration)
        except Exception:
            pass

    if "Participants" in linked_record_fields:
        if participant_record_ids:
            fields["Participants"] = participant_record_ids
    else:
        if participant_names:
            fields["Participants"] = participant_names

    call_id = call_data.get("call_id") or call_data.get("recording_id")
    if call_id and "Fathom Call ID" not in linked_record_fields:
        fields["Fathom Call ID"] = str(call_id)

    if call_data.get("transcript_url") and "Transcript URL" not in linked_record_fields:
        fields["Transcript URL"] = str(call_data.get("transcript_url"))
    
    if call_data.get("transcript") and "Transcript" not in linked_record_fields:
        fields["Transcript"] = str(call_data.get("transcript"))

    airtable_record = {"fields": fields}
    _log("airtable_meeting_create", fields=list(fields.keys()))

    try:
        response = requests.post(url, headers=headers, json=airtable_record, timeout=TIMEOUT)
        if response.status_code >= 400:
            _log("airtable_meeting_create_failed", status=response.status_code, body=response.text[:5000], fields=fields)
            return None
        return response.json()
    except requests.exceptions.RequestException as e:
        _log("airtable_meeting_request_error", error=str(e), fields=fields)
        return None


def extract_assignee(action_item_text: str) -> Optional[str]:
    mention_match = re.search(r"@(\w+(?:\s+\w+)?)", action_item_text)
    if mention_match:
        return mention_match.group(1).strip()
    bracket_match = re.search(r"\[([^\]]+)\]", action_item_text)
    if bracket_match:
        return bracket_match.group(1).strip()
    assigned_match = re.search(
        r"[Aa]ssigned\s+(?:to\s+)?:?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)", action_item_text
    )
    if assigned_match:
        return assigned_match.group(1).strip()
    colon_match = re.search(r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:", action_item_text)
    if colon_match:
        return colon_match.group(1).strip()
    action_match = re.search(r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:to|will|should|needs to)\s+", action_item_text)
    if action_match:
        return action_match.group(1).strip()
    return None


def create_action_items(action_items: list[Any], meeting_record_id: str, meeting_title: Optional[str]) -> int:
    if not action_items:
        return 0

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_ACTION_ITEMS_TABLE}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}

    linked_record_fields = get_linked_record_fields(AIRTABLE_ACTION_ITEMS_TABLE)

    created_count = 0
    for item in action_items:
        if isinstance(item, dict):
            item_text = item.get("text", "") or item.get("description", "")
            item_assignee = item.get("assignee")
            if isinstance(item_assignee, dict):
                item_assignee = item_assignee.get("name")
        else:
            item_text = str(item)
            item_assignee = None

        if not item_text:
            continue

        if not item_assignee:
            item_assignee = extract_assignee(item_text)
        if item_assignee:
            item_assignee = reformat_name(item_assignee)

        assignee_record_id = find_or_create_participant(item_assignee) if item_assignee else None

        fields: Dict[str, Any] = {
            "Description": str(item_text),
            "Status": "To Do",
            "Meeting": [meeting_record_id],
        }

        if assignee_record_id:
            fields["Assigned To"] = [assignee_record_id]
        elif item_assignee and "Assigned To" not in linked_record_fields:
            fields["Assigned To"] = str(item_assignee)

        action_item_record = {"fields": fields}

        try:
            response = requests.post(url, headers=headers, json=action_item_record, timeout=TIMEOUT)
            if response.status_code >= 400:
                _log("airtable_action_item_failed", status=response.status_code, body=response.text[:2000])
                continue
            created_count += 1
        except requests.exceptions.RequestException as e:
            _log("airtable_action_item_request_error", error=str(e))
            continue

    return created_count

