# Fathom to Airtable Webhook Handler

A single-file webhook handler that syncs Fathom meeting recordings to Airtable, including meeting details, summaries, and action items.

## Overview

This script receives webhook notifications from Fathom when a meeting recording is processed, fetches the meeting data from Fathom's API, and automatically creates structured records in Airtable with:

- **Meeting records** with title, recording URL, embed URL, share URL, summary, transcript, participants, duration, and timestamps
- **Action item records** with descriptions, assignees, and links back to the meeting
- **Participant records** that are automatically created and linked to meetings and action items

## Features

- ✅ Automatic participant management (finds existing or creates new records)
- ✅ Name reformatting (handles "Last, First" format)
- ✅ Smart assignee extraction from action item text (supports @mentions, [brackets], "Assigned to:", etc.)
- ✅ Automatic pagination when fetching meetings from Fathom
- ✅ Debug endpoint to view last received webhook payload
- ✅ Structured JSON logging for Cloud Run compatibility
- ✅ Robust error handling and timeouts

## Deployment

Deploy using Google Cloud Functions Framework:

```bash
functions-framework --source new_meeting.py --target fathom_webhook --port $PORT
```

Or deploy to Google Cloud Run/Cloud Functions directly.

## API Endpoints

### POST /
Webhook endpoint that receives Fathom notifications.

**Request body:**
```json
{
  "recording_id": "abc123",
  "call_id": "abc123"
}
```

**Response (success):**
```json
{
  "status": "success",
  "message": "Recording synced to Airtable",
  "recording_id": "abc123",
  "meeting_record_id": "rec123",
  "action_items_created": 3,
  "debug_request_id": "req_xyz"
}
```

### GET /
Debug endpoint that returns the last received webhook payload.

**Authentication:** Requires `WEBHOOK_DEBUG_TOKEN` via:
- Header: `X-Debug-Token` or `X-Webhook-Debug-Token`
- Query param: `?token=YOUR_TOKEN`

## Environment Variables

### Required

- `FATHOM_API_KEY` - Your Fathom API key
- `AIRTABLE_API_KEY` - Your Airtable personal access token or API key
- `AIRTABLE_BASE_ID` - The base ID containing your tables
- `AIRTABLE_MEETINGS_TABLE` - Table name for meetings (e.g., "Meetings")
- `AIRTABLE_ACTION_ITEMS_TABLE` - Table name for action items (e.g., "Action Items")

### Optional

- `AIRTABLE_PARTICIPANTS_TABLE` - Table name for participants (default: "People")
- `CONNECT_TIMEOUT_S` - Connection timeout in seconds (default: 5)
- `READ_TIMEOUT_S` - Read timeout in seconds (default: 25)
- `WEBHOOK_DEBUG_TOKEN` - Token to protect the GET / debug endpoint
- `WEBHOOK_PAYLOAD_STORE_PATH` - Path to store last payload (default: "/tmp/last_webhook_payload.json")

## Expected Airtable Schema

### Meetings Table
- **Title** (Single line text)
- **Recording URL** (URL)
- **Embed URL** (URL)
- **Share URL** (URL)
- **Summary** (Long text)
- **Start Time** (Date/time or text)
- **Duration** (Number - seconds)
- **Participants** (Link to People table)
- **Fathom Call ID** (Single line text)
- **Transcript URL** (URL, optional)
- **Transcript** (Long text, optional)

### Action Items Table
- **Description** (Long text or single line text)
- **Status** (Single select - must include "To Do")
- **Meeting** (Link to Meetings table)
- **Assigned To** (Link to People table)

### People Table
- **Name** (Single line text or email)

## How It Works

1. **Webhook received**: Fathom sends a POST request with `recording_id` or `call_id`
2. **Fetch meeting data**: Script queries Fathom API to get full meeting details (with pagination if needed)
3. **Process participants**: Reformats names and finds/creates participant records in Airtable
4. **Create meeting record**: Uploads meeting with all details to Airtable Meetings table
5. **Create action items**: Parses action items, extracts assignees, creates linked records in Airtable
6. **Return success**: Returns JSON with created record IDs and counts

## Assignee Extraction

The script intelligently extracts assignees from action item text using multiple patterns:

- `@John` or `@John Doe` - @mention format
- `[John]` or `[John Doe]` - Bracket format
- `Assigned to: John` - Explicit assignment
- `John: do the thing` - Colon prefix
- `John to follow up` - Action format

## Local Development

1. Create a `.env` file with all required environment variables
2. Install dependencies:
   ```bash
   pip install functions-framework requests python-dotenv
   ```
3. Run locally:
   ```bash
   functions-framework --source new_meeting.py --target fathom_webhook --debug --port 8080
   ```
4. Test with curl:
   ```bash
   curl -X POST http://localhost:8080/ \
     -H "Content-Type: application/json" \
     -d '{"recording_id": "YOUR_RECORDING_ID"}'
   ```

## Logging

The script outputs structured JSON logs suitable for Cloud Logging, including:
- Stage progression (parse_payload → fetch_fathom_call_data → upload_meeting_to_airtable → create_action_items)
- Duration tracking for each stage
- Error details with context
- Debug request IDs for tracing

## Error Handling

- Graceful degradation if optional fields are missing
- Automatic retry logic not included (handled by Cloud Run)
- Detailed error responses with stage information
- Best-effort payload persistence for debugging
