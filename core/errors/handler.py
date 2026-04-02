"""
OTA Command — Error Handling & Retry Engine
Exponential backoff retries, dead-letter queue, Notion alerts.
"""

import json
import time
import traceback
import requests
import os
from datetime import datetime, timezone
from pathlib import Path
from functools import wraps
from core.config import get_setting

_ROOT = Path(__file__).resolve().parent.parent.parent
DLQ_DIR = _ROOT / "queue" / "dead_letter"
DLQ_DIR.mkdir(parents=True, exist_ok=True)

# Track consecutive failures per phase
_failure_counts = {}

# Notion Pipeline Log database ID (data_source_id)
NOTION_PIPELINE_DB = "40c5f2be-0b41-4113-b012-b185c455280a"


def retry_with_backoff(phase_name: str):
    """
    Decorator: retry a phase function with exponential backoff.
    Uses settings from config/settings.yaml → errors section.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            max_retries = get_setting("errors", "max_retries", default=3)
            backoffs = get_setting("errors", "retry_backoff_seconds", default=[30, 120, 600])
            alert_threshold = get_setting("errors", "alert_on_failure_count", default=3)

            last_error = None
            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    # Reset failure count on success
                    _failure_counts[phase_name] = 0
                    return result
                except Exception as e:
                    last_error = e
                    _failure_counts[phase_name] = _failure_counts.get(phase_name, 0) + 1

                    if attempt < max_retries:
                        wait = backoffs[min(attempt, len(backoffs) - 1)]
                        print(f"[error] {phase_name} failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                        print(f"[error] Retrying in {wait}s...")
                        time.sleep(wait)
                    else:
                        print(f"[error] {phase_name} FAILED after {max_retries + 1} attempts")

                        # Send to dead-letter queue
                        _send_to_dlq(phase_name, args, kwargs, last_error)

                        # Alert if threshold reached
                        if _failure_counts[phase_name] >= alert_threshold:
                            _send_notion_alert(phase_name, last_error)

                        raise

        return wrapper
    return decorator


def _send_to_dlq(phase_name: str, args: tuple, kwargs: dict, error: Exception):
    """Write failed job to dead-letter queue for manual inspection."""
    if not get_setting("errors", "dead_letter_queue", default=True):
        return

    dlq_entry = {
        "phase": phase_name,
        "error": str(error),
        "traceback": traceback.format_exc(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "args": [str(a) for a in args],
        "kwargs": {k: str(v) for k, v in kwargs.items()},
    }

    filename = f"{phase_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    with open(DLQ_DIR / filename, "w") as f:
        json.dump(dlq_entry, f, indent=2)

    print(f"[error] Written to dead-letter queue: {filename}")


# -----------------------------------------------------------
# Notion Integration — Pipeline Log
# -----------------------------------------------------------

PHASE_MAP = {
    "01_discovery": "01 Discovery",
    "01_discovery_api": "01 Discovery",
    "02_rights_gate": "02 Rights Gate",
    "03_extraction": "03 Extraction",
    "04_storage": "04 Storage",
    "04_storage_github": "04 Storage",
    "04_storage_drive": "04 Storage",
    "05_notebooklm": "05 NotebookLM",
    "06_multiplication": "06 Multiplication",
    "06_social_copy": "06 Multiplication",
    "06_blog": "06 Multiplication",
    "06_newsletter": "06 Multiplication",
    "06_quotes": "06 Multiplication",
    "06_thumbnails": "06 Multiplication",
    "07_production": "07 Production",
    "07_voiceover": "07 Production",
    "07_video": "07 Production",
    "07_clips": "07 Production",
    "08_qa_gate": "08 QA Gate",
    "09_distribution": "09 Distribution",
    "10_analytics": "10 Analytics",
}


def _notion_create_page(properties: dict, content: str = "") -> dict:
    """Create a page in the OTA Command Pipeline Log database via Notion API."""
    notion_token = os.getenv("NOTION_API_KEY", "")
    if not notion_token:
        print("[notify] No NOTION_API_KEY set — skipping Notion notification")
        return {}

    # Build Notion API properties
    notion_props = {}

    if "Event" in properties:
        notion_props["Event"] = {
            "title": [{"text": {"content": str(properties["Event"])[:100]}}]
        }
    if "Phase" in properties:
        notion_props["Phase"] = {"select": {"name": properties["Phase"]}}
    if "Status" in properties:
        notion_props["Status"] = {"select": {"name": properties["Status"]}}
    if "Video Title" in properties:
        notion_props["Video Title"] = {
            "rich_text": [{"text": {"content": str(properties["Video Title"])[:200]}}]
        }
    if "Slug" in properties:
        notion_props["Slug"] = {
            "rich_text": [{"text": {"content": str(properties["Slug"])[:100]}}]
        }
    if "Video URL" in properties:
        notion_props["Video URL"] = {"url": properties["Video URL"]}
    if "Asset Count" in properties:
        notion_props["Asset Count"] = {"number": properties["Asset Count"]}
    if "Details" in properties:
        notion_props["Details"] = {
            "rich_text": [{"text": {"content": str(properties["Details"])[:2000]}}]
        }

    body = {
        "parent": {"database_id": "1373bc60-a288-42cb-9e3b-1611d0c609fe"},
        "properties": notion_props,
    }

    # Add page content if provided
    if content:
        body["children"] = [{
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"text": {"content": content[:2000]}}]
            }
        }]

    try:
        resp = requests.post(
            "https://api.notion.com/v1/pages",
            headers={
                "Authorization": f"Bearer {notion_token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=10,
        )
        if resp.status_code in (200, 201):
            print(f"[notify] Notion page created: {properties.get('Event', '')}")
            return resp.json()
        else:
            print(f"[notify] Notion API error: {resp.status_code} {resp.text[:200]}")
            return {}
    except Exception as e:
        print(f"[notify] Failed to create Notion page: {e}")
        return {}


def _send_notion_alert(phase_name: str, error: Exception):
    """Send critical failure alert to Notion Pipeline Log."""
    phase_label = PHASE_MAP.get(phase_name, phase_name)

    _notion_create_page(
        properties={
            "Event": f"CRITICAL FAILURE — {phase_name}",
            "Phase": phase_label,
            "Status": "Error",
            "Details": f"Error: {str(error)[:500]} | Action: Check dead-letter queue and retry manually.",
        },
        content=(
            f"Critical failure in {phase_name}\n\n"
            f"Error: {str(error)[:1000]}\n\n"
            f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
            f"Action: Check queue/dead_letter/ for the failed job and retry."
        ),
    )
    print(f"[error] Notion alert created for {phase_name}")


def notify(
    event: str,
    phase: str = "",
    status: str = "Running",
    video_title: str = "",
    slug: str = "",
    video_url: str = "",
    asset_count: int = None,
    details: str = "",
    content: str = "",
):
    """
    Send a pipeline notification to Notion.
    This is the primary notification function — replaces notify_slack.
    """
    phase_label = PHASE_MAP.get(phase, phase) if phase else ""

    props = {"Event": event}
    if phase_label:
        props["Phase"] = phase_label
    if status:
        props["Status"] = status
    if video_title:
        props["Video Title"] = video_title
    if slug:
        props["Slug"] = slug
    if video_url:
        props["Video URL"] = video_url
    if asset_count is not None:
        props["Asset Count"] = asset_count
    if details:
        props["Details"] = details

    return _notion_create_page(props, content)


# Backward compat alias — any old notify_slack calls still work
def notify_slack(message: str, emoji: str = ""):
    """Legacy alias — routes to Notion. Parses message for context."""
    notify(event=message[:100], details=message)


# A Brand Collab Production. All rights reserved 2026.
