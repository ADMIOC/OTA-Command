"""
OTA Command — Event Dispatch System
Each phase emits an event on completion that triggers the next phase.
Events are stored as JSON files in the queue directory for persistence
and can trigger GitHub Actions via repository_dispatch.
"""

import json
import os
import requests
from datetime import datetime, timezone
from pathlib import Path
from enum import Enum

_ROOT = Path(__file__).resolve().parent.parent.parent
QUEUE_DIR = _ROOT / "queue"
QUEUE_DIR.mkdir(exist_ok=True)


class Phase(Enum):
    DISCOVERY = "01_discovery"
    RIGHTS_GATE = "02_rights_gate"
    EXTRACTION = "03_extraction"
    STORAGE = "04_storage"
    NOTEBOOKLM = "05_notebooklm"
    MULTIPLICATION = "06_multiplication"
    PRODUCTION = "07_production"
    QA_GATE = "08_qa_gate"
    DISTRIBUTION = "09_distribution"
    ANALYTICS = "10_analytics"


class EventStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    REQUIRES_APPROVAL = "requires_approval"


# Phase execution order — each phase triggers the next
PHASE_ORDER = [
    Phase.DISCOVERY,
    Phase.RIGHTS_GATE,
    Phase.EXTRACTION,
    Phase.STORAGE,
    Phase.NOTEBOOKLM,      # Manual gate — pauses here
    Phase.MULTIPLICATION,
    Phase.PRODUCTION,
    Phase.QA_GATE,
    Phase.DISTRIBUTION,
    Phase.ANALYTICS,
]


def create_event(
    phase: Phase,
    status: EventStatus,
    payload: dict,
    video_id: str = "",
    slug: str = "",
) -> dict:
    """Create a pipeline event."""
    event = {
        "event_id": f"{phase.value}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        "phase": phase.value,
        "status": status.value,
        "video_id": video_id,
        "slug": slug,
        "payload": payload,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Persist to queue
    event_file = QUEUE_DIR / f"{event['event_id']}.json"
    with open(event_file, "w") as f:
        json.dump(event, f, indent=2)

    return event


def emit_next_phase(current_phase: Phase, payload: dict, video_id: str = "", slug: str = ""):
    """Emit an event to trigger the next phase in the pipeline."""
    idx = PHASE_ORDER.index(current_phase)
    if idx + 1 >= len(PHASE_ORDER):
        print(f"[dispatch] Pipeline complete for {slug or video_id}")
        return None

    next_phase = PHASE_ORDER[idx + 1]

    # NotebookLM is a manual gate — mark as requires_approval
    if next_phase == Phase.NOTEBOOKLM:
        status = EventStatus.REQUIRES_APPROVAL
    else:
        status = EventStatus.PENDING

    event = create_event(
        phase=next_phase,
        status=status,
        payload=payload,
        video_id=video_id,
        slug=slug,
    )

    print(f"[dispatch] {current_phase.value} → {next_phase.value} | status={status.value}")

    # Trigger GitHub Actions workflow if token available
    _trigger_github_dispatch(next_phase, event)

    return event


def _trigger_github_dispatch(phase: Phase, event: dict):
    """Fire a repository_dispatch event to trigger the next GitHub Action."""
    token = os.getenv("GITHUB_TOKEN", "")
    if not token:
        print("[dispatch] No GITHUB_TOKEN — skipping repository_dispatch")
        return

    repo = "ADMIOC/OTA-Command"
    url = f"https://api.github.com/repos/{repo}/dispatches"

    try:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github.v3+json",
            },
            json={
                "event_type": f"phase_{phase.value}",
                "client_payload": {
                    "event_id": event["event_id"],
                    "video_id": event["video_id"],
                    "slug": event["slug"],
                    "phase": phase.value,
                },
            },
            timeout=10,
        )
        if resp.status_code == 204:
            print(f"[dispatch] GitHub Action triggered for {phase.value}")
        else:
            print(f"[dispatch] GitHub dispatch failed: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[dispatch] GitHub dispatch error: {e}")


def get_pending_events(phase: Phase = None) -> list:
    """Get all pending events, optionally filtered by phase."""
    events = []
    for f in sorted(QUEUE_DIR.glob("*.json")):
        with open(f) as fh:
            event = json.load(fh)
        if event["status"] == EventStatus.PENDING.value:
            if phase is None or event["phase"] == phase.value:
                events.append(event)
    return events


def update_event_status(event_id: str, status: EventStatus, payload_update: dict = None):
    """Update an event's status and optionally merge new payload data."""
    event_file = QUEUE_DIR / f"{event_id}.json"
    if not event_file.exists():
        raise FileNotFoundError(f"Event {event_id} not found")

    with open(event_file) as f:
        event = json.load(f)

    event["status"] = status.value
    event["updated_at"] = datetime.now(timezone.utc).isoformat()
    if payload_update:
        event["payload"].update(payload_update)

    with open(event_file, "w") as f:
        json.dump(event, f, indent=2)

    return event
