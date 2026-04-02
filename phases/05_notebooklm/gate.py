"""
OTA Command — Phase 5: NotebookLM Manual Gate
The one manual step. Pauses the pipeline, notifies the user via Slack
with exact instructions, then waits for a completion signal.

Completion signals (any of these resume the pipeline):
  1. Slack slash command: /ota-resume <slug>
  2. GitHub dispatch event: phase_05_complete
  3. Manual script: python -m phases.05_notebooklm.gate --complete <slug>
  4. Queue file: queue/05_complete_<slug>.json exists

Once signaled, emits to Phase 6 (Content Multiplication).
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from core.config import get_setting
from core.logging.logger import get_logger
from core.errors.handler import notify_slack
from core.dispatch.events import emit_next_phase, Phase, create_event, EventStatus, update_event_status

log = get_logger("05_notebooklm")

_ROOT = Path(__file__).resolve().parent.parent.parent
QUEUE_DIR = _ROOT / "queue"


def send_notebooklm_instructions(slug: str, payload: dict):
    """
    Send Slack notification with exact NotebookLM steps.
    """
    title = payload.get("title", slug)
    drive_info = payload.get("drive_sync", {})
    web_link = drive_info.get("web_link", "")
    filename = drive_info.get("filename", f"{slug}_notebooklm_source.md")

    instructions = (
        f":clipboard: *NotebookLM — Action Required*\n\n"
        f"*Video:* {title}\n"
        f"*Slug:* `{slug}`\n"
        f"*Source file:* `{filename}`\n"
    )

    if web_link:
        instructions += f"*Drive link:* {web_link}\n"

    instructions += (
        f"\n*Steps:*\n"
        f"1. Go to notebooklm.google.com\n"
        f"2. Click *+ New Notebook*\n"
        f"3. *Add Source* → Google Drive → OTA-Pipeline → OTA-NotebookLM-Source\n"
        f"4. Select: `{filename}`\n"
        f"5. Studio panel → *Slides* → *Detailed Deck* → English → Default → *Generate*\n"
        f"6. Save completed deck to *OTA-Pipeline → OTA-NotebookLM-Output* in Drive\n"
        f"\n"
        f"When done, signal completion:\n"
        f"```python -m phases.05_notebooklm.gate --complete {slug}```\n"
        f"Or push a file: `queue/05_complete_{slug}.json`"
    )

    notify_slack(instructions, emoji="")
    log.info(f"NotebookLM instructions sent for {slug}")


def check_completion(slug: str) -> bool:
    """
    Check if the human has signaled NotebookLM completion.
    Looks for a completion marker file in the queue directory.
    """
    marker = QUEUE_DIR / f"05_complete_{slug}.json"
    return marker.exists()


def mark_complete(slug: str, payload: dict = None):
    """
    Manually mark a video's NotebookLM step as complete.
    Creates the completion marker and emits Phase 6.
    """
    marker = QUEUE_DIR / f"05_complete_{slug}.json"
    completion = {
        "slug": slug,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "completed_by": "manual",
    }
    marker.parent.mkdir(parents=True, exist_ok=True)
    with open(marker, "w") as f:
        json.dump(completion, f, indent=2)

    log.success(f"NotebookLM marked complete for {slug}")

    # Emit to Phase 6
    if payload is None:
        payload = {"slug": slug}

    payload["notebooklm_completed"] = completion

    event = emit_next_phase(
        current_phase=Phase.NOTEBOOKLM,
        payload=payload,
        video_id=payload.get("video_id", ""),
        slug=slug,
    )

    notify_slack(
        f":white_check_mark: *NotebookLM complete* — `{slug}`\n"
        f"Pipeline resuming → Phase 6 (Content Multiplication)",
    )

    return event


def run_gate(slug: str, payload: dict) -> dict:
    """
    Main gate logic:
    1. Send instructions to Slack
    2. Create a requires_approval event
    3. Return — the pipeline pauses here

    Resumption happens via mark_complete() called externally.
    """
    log.start(f"NotebookLM gate for {slug}")

    # Send instructions
    send_notebooklm_instructions(slug, payload)

    # Create gate event
    event = create_event(
        phase=Phase.NOTEBOOKLM,
        status=EventStatus.REQUIRES_APPROVAL,
        payload=payload,
        video_id=payload.get("video_id", ""),
        slug=slug,
    )

    log.info(f"Pipeline paused at NotebookLM gate. Waiting for completion signal.")

    return {
        "status": "waiting",
        "event_id": event["event_id"],
        "slug": slug,
        "instructions_sent": True,
    }


# -----------------------------------------------------------
# CLI Interface
# -----------------------------------------------------------

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 3 and sys.argv[1] == "--complete":
        slug = sys.argv[2]
        print(f"Marking NotebookLM complete for: {slug}")
        result = mark_complete(slug)
        print(json.dumps(result, indent=2, default=str) if result else "Marked complete")
    elif len(sys.argv) >= 3 and sys.argv[1] == "--check":
        slug = sys.argv[2]
        done = check_completion(slug)
        print(f"Complete: {done}")
    else:
        print("Usage:")
        print("  python -m phases.05_notebooklm.gate --complete <slug>")
        print("  python -m phases.05_notebooklm.gate --check <slug>")


# A Brand Collab Production. All rights reserved 2026.
