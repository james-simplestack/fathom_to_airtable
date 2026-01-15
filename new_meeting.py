
import functions_framework
import requests
import os
import re
from datetime import datetime
from flask import jsonify
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment variables - set these in GCP Cloud Functions
FATHOM_API_KEY = os.environ.get('FATHOM_API_KEY')
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.environ.get('AIRTABLE_BASE_ID')
AIRTABLE_MEETINGS_TABLE = os.environ.get('AIRTABLE_MEETINGS_TABLE')
AIRTABLE_ACTION_ITEMS_TABLE = os.environ.get('AIRTABLE_ACTION_ITEMS_TABLE')
AIRTABLE_PARTICIPANTS_TABLE = os.environ.get('AIRTABLE_PARTICIPANTS_TABLE', 'People')  # Default to 'People' table

@functions_framework.http
def fathom_webhook(request):
    """
    HTTP Cloud Function to handle Fathom webhooks and sync to Airtable.
    Triggered by Fathom when a new recording is available.
    """
    
    # Verify the request method
    if request.method != 'POST':
        return jsonify({'error': 'Only POST requests are allowed'}), 405
    
    try:
        # Parse the webhook payload from Fathom
        webhook_data = request.get_json()
        
        if not webhook_data:
            return jsonify({'error': 'No data received'}), 400
        
        # Extract the recording ID from the webhook (Fathom uses recording_id)
        recording_id = webhook_data.get('recording_id') or webhook_data.get('call_id')
        
        if not recording_id:
            return jsonify({'error': 'No recording_id or call_id in webhook data'}), 400
        
        print(f"Processing Fathom recording: {recording_id}")
        
        # Fetch detailed call data from Fathom API
        call_data = fetch_fathom_call_data(recording_id)
        
        if not call_data:
            return jsonify({'error': 'Failed to fetch call data from Fathom'}), 500
        
        # Upload meeting to Airtable
        meeting_record = upload_meeting_to_airtable(call_data)
        
        if not meeting_record:
            return jsonify({'error': 'Failed to upload meeting to Airtable'}), 500
        
        meeting_record_id = meeting_record.get('id')
        
        # Create action item records
        action_items_created = create_action_items(
            call_data.get('action_items', []),
            meeting_record_id,
            call_data.get('title')
        )
        
        return jsonify({
            'status': 'success',
            'message': 'Recording synced to Airtable',
            'recording_id': recording_id,
            'meeting_record_id': meeting_record_id,
            'action_items_created': action_items_created
        }), 200
            
    except Exception as e:
        print(f"Error processing webhook: {str(e)}")
        return jsonify({'error': str(e)}), 500


def meeting_has_participant(meeting, participant_name):
    """
    Check if a meeting has a specific participant.
    Returns True if the participant is found, False otherwise.
    """
    if not meeting or not participant_name:
        return False
    
    # Check calendar_invitees
    for invitee in meeting.get('calendar_invitees', []):
        name = invitee.get('name', '')
        email = invitee.get('email', '')
        # Check if name matches (case-insensitive)
        if participant_name.lower() in name.lower() or participant_name.lower() in email.lower():
            return True
    
    return False

def get_meeting_id_from_call_id(call_id, participant_filter=None):
    """
    Get the meeting ID from a call/recording ID.
    Optionally filter by participant name (e.g., "James Nevada").
    Returns the meeting ID (id field) or None if not found.
    """
    url = "https://api.fathom.ai/external/v1/meetings"
    headers = {
        'X-Api-Key': FATHOM_API_KEY,
        'Content-Type': 'application/json'
    }
    
    params = {
        'include_action_items': 'false',
        'include_summary': 'false',
        'include_transcript': 'false',
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Find the meeting with matching recording_id
        meeting = None
        if 'items' in data:
            for item in data['items']:
                if str(item.get('recording_id')) == str(call_id):
                    # If participant filter is set, check if meeting has that participant
                    if participant_filter:
                        if meeting_has_participant(item, participant_filter):
                            meeting = item
                            break
                    else:
                        meeting = item
                        break
        
        if not meeting:
            # Try paginating
            cursor = data.get('next_cursor')
            while cursor and not meeting:
                params['cursor'] = cursor
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                page_data = response.json()
                for item in page_data.get('items', []):
                    if str(item.get('recording_id')) == str(call_id):
                        # If participant filter is set, check if meeting has that participant
                        if participant_filter:
                            if meeting_has_participant(item, participant_filter):
                                meeting = item
                                break
                        else:
                            meeting = item
                            break
                cursor = page_data.get('next_cursor')
        
        if meeting:
            # Return the meeting ID (id field)
            return meeting.get('id')
        
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching meeting ID: {str(e)}")
        return None

def fetch_fathom_call_data(recording_id):
    """
    Fetch detailed call data from Fathom API including recording URL,
    summary, title, and action items.
    Uses the Fathom API: https://developers.fathom.ai/api-reference/meetings/list-meetings
    """
    
    # Fathom API endpoint - list meetings and filter by recording_id
    # Since there's no direct GET /meetings/{id} endpoint, we filter the list
    url = "https://api.fathom.ai/external/v1/meetings"
    headers = {
        'X-Api-Key': FATHOM_API_KEY,
        'Content-Type': 'application/json'
    }
    
    # Query parameters to get full meeting details
    params = {
        'include_action_items': 'true',
        'include_summary': 'true',
        'include_transcript': 'false',  # Set to true if you need transcript
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Find the meeting with matching recording_id
        meeting = None
        if 'items' in data:
            for item in data['items']:
                if str(item.get('recording_id')) == str(recording_id):
                    meeting = item
                    break
        
        if not meeting:
            # If not found in first page, try paginating (though unlikely for webhook)
            print(f"Meeting {recording_id} not found in first page, checking pagination...")
            cursor = data.get('next_cursor')
            while cursor and not meeting:
                params['cursor'] = cursor
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                page_data = response.json()
                for item in page_data.get('items', []):
                    if str(item.get('recording_id')) == str(recording_id):
                        meeting = item
                        break
                cursor = page_data.get('next_cursor')
        
        if not meeting:
            print(f"Error: Meeting with recording_id {recording_id} not found")
            return None
        
        # Extract summary text from markdown
        summary_text = ''
        if meeting.get('default_summary'):
            summary_text = meeting['default_summary'].get('markdown_formatted', '')
        
        # Format participants from calendar_invitees
        participants = []
        for invitee in meeting.get('calendar_invitees', []):
            participants.append(invitee.get('name', invitee.get('email', '')))
        
        # Format action items - extract description and assignee name
        action_items = []
        for item in meeting.get('action_items', []):
            action_item = {
                'description': item.get('description', ''),
                'assignee': None
            }
            if item.get('assignee'):
                action_item['assignee'] = item['assignee'].get('name')
            action_items.append(action_item)
        
        # Calculate duration if not provided
        duration = None
        if meeting.get('recording_start_time') and meeting.get('recording_end_time'):
            try:
                start = datetime.fromisoformat(meeting['recording_start_time'].replace('Z', '+00:00'))
                end = datetime.fromisoformat(meeting['recording_end_time'].replace('Z', '+00:00'))
                duration_seconds = (end - start).total_seconds()
                duration = int(duration_seconds)
            except:
                pass

        # Prepare URLs
        recording_url = meeting.get('url') or meeting.get('share_url')
        embed_url = None
        if recording_url:
            embed_url = recording_url.replace('/share/', '/embed/')

        # Extract relevant fields matching the expected structure
        call_info = {
            'call_id': str(recording_id),  # Keep for backward compatibility
            'recording_id': recording_id,
            'meeting_id': meeting.get('id'),  # Meeting ID from Fathom
            'title': meeting.get('title') or meeting.get('meeting_title', 'Untitled Meeting'),
            'start_time': meeting.get('recording_start_time') or meeting.get('scheduled_start_time'),
            'end_time': meeting.get('recording_end_time') or meeting.get('scheduled_end_time'),
            'duration': duration,
            'recording_url': recording_url,
            'embed_url': embed_url,   # <--- New Field
            'summary': summary_text,
            'action_items': action_items,
            'participants': participants,
            'transcript_url': None  # Not directly available in this endpoint
        }
        
        return call_info
        
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Fathom call data: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
        return None


def get_linked_record_fields(table_name):
    """
    Get list of field names that are linked record fields in the given table.
    Returns a set of field names.
    """
    try:
        url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"
        headers = {
            'Authorization': f'Bearer {AIRTABLE_API_KEY}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for table in data.get('tables', []):
                if table.get('name') == table_name:
                    linked_fields = set()
                    for field in table.get('fields', []):
                        field_type = field.get('type')
                        if field_type in ['multipleRecordLinks', 'singleCollaborator', 'multipleCollaborators']:
                            linked_fields.add(field.get('name'))
                    return linked_fields
    except Exception as e:
        print(f"Warning: Could not fetch table schema: {e}")
    return set()

def reformat_name(name):
    """
    Reformat name from "Last Name, First Name" to "First Name Last Name".
    If no comma, returns name as-is.
    """
    if not name:
        return name
    
    name = name.strip()
    # Check if name is in "Last Name, First Name" format
    if ',' in name:
        parts = [part.strip() for part in name.split(',', 1)]
        if len(parts) == 2:
            # Swap: Last Name, First Name -> First Name Last Name
            return f"{parts[1]} {parts[0]}"
    
    return name

def find_or_create_participant(participant_name):
    """
    Find a person record by name in the People table, or create it if it doesn't exist.
    Used for both meeting participants and action item assignees.
    Returns the record ID.
    Note: Name should already be reformatted to "First Name Last Name" format before calling.
    """
    if not participant_name:
        return None
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_PARTICIPANTS_TABLE}"
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # First, try to find existing participant by name
    # We'll search for records where Name field matches
    # Escape single quotes in participant name for Airtable formula
    escaped_name = participant_name.replace("'", "''")
    formula = f"{{Name}} = '{escaped_name}'"
    params = {
        'filterByFormula': formula,
        'maxRecords': 1
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('records') and len(data.get('records', [])) > 0:
                # Found existing record
                record_id = data['records'][0]['id']
                print(f"Found existing participant: {participant_name} ({record_id})")
                return record_id
        
        # Participant doesn't exist, create it
        # Try different common field names for the name field
        name_fields = ['Name', 'Participant', 'Full Name', 'Email']
        create_data = {}
        
        # Try to get the schema to find the correct name field
        schema_url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"
        schema_response = requests.get(schema_url, headers=headers, timeout=10)
        name_field = 'Name'  # Default
        
        if schema_response.status_code == 200:
            schema_data = schema_response.json()
            for table in schema_data.get('tables', []):
                if table.get('name') == AIRTABLE_PARTICIPANTS_TABLE:
                    # Find the first text field or name-like field
                    for field in table.get('fields', []):
                        if field.get('type') == 'singleLineText' or field.get('type') == 'email':
                            name_field = field.get('name')
                            break
                    break
        
        create_data['fields'] = {name_field: participant_name}
        
        create_response = requests.post(url, headers=headers, json=create_data, timeout=10)
        if create_response.status_code == 200:
            record_id = create_response.json().get('id')
            print(f"Created new participant: {participant_name} ({record_id})")
            return record_id
        else:
            print(f"Error creating participant {participant_name}: {create_response.status_code} - {create_response.text}")
            return None
            
    except Exception as e:
        print(f"Error finding/creating participant {participant_name}: {e}")
        return None

def upload_meeting_to_airtable(call_data):
    """
    Upload the Fathom meeting data to the Meetings table in Airtable.
    """
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_MEETINGS_TABLE}"
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Get linked record fields to skip them (we don't have record IDs)
    linked_record_fields = get_linked_record_fields(AIRTABLE_MEETINGS_TABLE)
    if linked_record_fields:
        print(f"DEBUG: Found linked record fields: {linked_record_fields}")
    
    # Get participant names and reformat them
    participant_names = [
        reformat_name(str(p.get('name', p) if isinstance(p, dict) else p))
        for p in call_data.get('participants', [])
    ]
    
    # Find or create participant records in People table and get their IDs
    participant_record_ids = []
    if 'Participants' in linked_record_fields and participant_names:
        print(f"Finding/creating {len(participant_names)} participant records in People table...")
        for participant_name in participant_names:
            record_id = find_or_create_participant(participant_name)
            if record_id:
                participant_record_ids.append(record_id)
        print(f"Got {len(participant_record_ids)} participant record IDs")
    
    # Prepare the Airtable record for meetings table
    # Only include fields that have values (skip None) and skip linked record fields
    fields = {}
    
    # Title - string
    if call_data.get('title') and 'Title' not in linked_record_fields:
        fields['Title'] = str(call_data.get('title'))
    
    # Recording URL - string
    if call_data.get('recording_url') and 'Recording URL' not in linked_record_fields:
        fields['Recording URL'] = str(call_data.get('recording_url'))

   
    # Embed URL - string
    if call_data.get('embed_url') and 'Embed URL' not in linked_record_fields:
        fields['Embed URL'] = str(call_data.get('embed_url'))

    # Summary - string
    if call_data.get('summary') and 'Summary' not in linked_record_fields:
        fields['Summary'] = str(call_data.get('summary'))
    
    # Start Time - string (ISO format)
    if call_data.get('start_time') and 'Start Time' not in linked_record_fields:
        fields['Start Time'] = str(call_data.get('start_time'))
    
    # Duration - integer (only include if not None)
    duration = call_data.get('duration')
    if duration is not None and 'Duration' not in linked_record_fields:
        try:
            fields['Duration'] = int(duration)
        except (ValueError, TypeError):
            # Skip if duration can't be converted to int
            pass
    
    # Participants - handle based on field type
    if 'Participants' in linked_record_fields:
        # It's a linked record field - use record IDs
        if participant_record_ids:
            fields['Participants'] = participant_record_ids
    else:
        # If it's not a linked record field, send as array of strings
        if participant_names:
            fields['Participants'] = participant_names
    
    # Fathom Call ID - string (convert to string)
    call_id = call_data.get('call_id') or call_data.get('recording_id')
    if call_id and 'Fathom Call ID' not in linked_record_fields:
        fields['Fathom Call ID'] = str(call_id)
    
    # Transcript URL - string
    if call_data.get('transcript_url') and 'Transcript URL' not in linked_record_fields:
        fields['Transcript URL'] = str(call_data.get('transcript_url'))
    
    airtable_record = {
        'fields': fields
    }
    
    try:
        # Debug: print what we're sending
        print(f"DEBUG: Sending fields to Airtable: {list(fields.keys())}")
        response = requests.post(url, headers=headers, json=airtable_record)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error uploading meeting to Airtable: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
            # Try to identify which field is problematic
            try:
                error_data = e.response.json()
                if 'error' in error_data:
                    print(f"Error details: {error_data['error']}")
            except:
                pass
        print(f"DEBUG: Fields being sent: {fields}")
        return None


def extract_assignee(action_item_text):
    """
    Extract the assignee from action item text.
    Looks for patterns like:
    - @Name
    - [Name]
    - Name:
    - Assigned to Name
    - Name to do something
    """
    
    # Pattern 1: @mention
    mention_match = re.search(r'@(\w+(?:\s+\w+)?)', action_item_text)
    if mention_match:
        return mention_match.group(1).strip()
    
    # Pattern 2: [Name]
    bracket_match = re.search(r'\[([^\]]+)\]', action_item_text)
    if bracket_match:
        return bracket_match.group(1).strip()
    
    # Pattern 3: "Assigned to Name" or "assigned: Name"
    assigned_match = re.search(r'[Aa]ssigned\s+(?:to\s+)?:?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', action_item_text)
    if assigned_match:
        return assigned_match.group(1).strip()
    
    # Pattern 4: "Name:" at the start
    colon_match = re.search(r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:', action_item_text)
    if colon_match:
        return colon_match.group(1).strip()
    
    # Pattern 5: "Name to [verb]" or "Name will"
    action_match = re.search(r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:to|will|should|needs to)\s+', action_item_text)
    if action_match:
        return action_match.group(1).strip()
    
    return None


def create_action_items(action_items, meeting_record_id, meeting_title):
    """
    Create individual action item records in Airtable, 
    extracting and assigning to mentioned persons.
    """
    
    if not action_items:
        return 0
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_ACTION_ITEMS_TABLE}"
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Get linked record fields to handle them properly
    linked_record_fields = get_linked_record_fields(AIRTABLE_ACTION_ITEMS_TABLE)
    if linked_record_fields:
        print(f"DEBUG: Action items table linked record fields: {linked_record_fields}")
    
    created_count = 0
    
    for item in action_items:
        # Handle both string and dict formats
        if isinstance(item, dict):
            item_text = item.get('text', '') or item.get('description', '')
            # Assignee can be a string (name) or dict with name/email
            item_assignee = item.get('assignee')
            if isinstance(item_assignee, dict):
                item_assignee = item_assignee.get('name')
        else:
            item_text = str(item)
            item_assignee = None
        
        if not item_text:
            continue
        
        # Extract assignee if not already provided
        if not item_assignee:
            item_assignee = extract_assignee(item_text)
        
        # Reformat assignee name: "Last Name, First Name" -> "First Name Last Name"
        if item_assignee:
            item_assignee = reformat_name(item_assignee)
        
        # Find or create assignee record in People table if assignee exists
        assignee_record_id = None
        if item_assignee:
            assignee_record_id = find_or_create_participant(item_assignee)
            if not assignee_record_id:
                print(f"Warning: Could not find or create assignee in People table: {item_assignee}")
        
        # Prepare action item record - only include fields that exist and have values
        fields = {}
        
        # Description - Action item description goes to Description field in Airtable
        if item_text:
            fields['Description'] = str(item_text)
        
        # Status - string (should be a select field)
        fields['Status'] = 'To Do'
        
        # Meeting - linked record (array of record IDs)
        # Meeting should always be a linked record field linking to Meetings table
        if meeting_record_id:
            fields['Meeting'] = [meeting_record_id]
        
        # Assigned To - linked record field (use record ID if it's a linked record field)
        if assignee_record_id:
            fields['Assigned To'] = [assignee_record_id]
        elif item_assignee and 'Assigned To' not in linked_record_fields:
            # If it's not a linked record field, send as string
            fields['Assigned To'] = str(item_assignee)
        
        if not fields:
            print(f"Warning: No valid fields to create action item: {item_text[:50]}...")
            continue
        
        action_item_record = {
            'fields': fields
        }
        
        try:
            response = requests.post(url, headers=headers, json=action_item_record)
            response.raise_for_status()
            created_count += 1
            print(f"Created action item: {item_text[:50]}... (Assigned to: {item_assignee or 'Unassigned'})")
            
        except requests.exceptions.RequestException as e:
            print(f"Error creating action item: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response body: {e.response.text}")
            print(f"DEBUG: Fields being sent: {fields}")
            continue
    
    return created_count
