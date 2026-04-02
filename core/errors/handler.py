"""
OTA Command — Error Handling & Retry Engine
Exponential backoff retries, dead-letter queue, Slack alerts.
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
                            _send_slack_alert(phase_name, last_error)

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


def _send_slack_alert(phase_name: str, error: Exception):
    """Send critical failure alert to Slack."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        print("[error] No SLACK_WEBHOOK_URL set — skipping alert")
        return

    message = {
        "text": f":rotating_light: *OTA Command — Critical Failure*\n"
                f"*Phase:* `{phase_name}`\n"
                f"*Error:* `{str(error)[:500]}`\n"
                f"*Time:* {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
                f"*Action:* Check dead-letter queue and retry manually.",
    }

    try:
        requests.post(webhook_url, json=message, timeout=5)
        print(f"[error] Slack alert sent for {phase_name}")
    except Exception as e:
        print(f"[error] Failed to send Slack alert: {e}")


def notify_slack(message: str, emoji: str = ":robot_face:"):
    """Send a general notification to Slack."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return

    try:
        requests.post(
            webhook_url,
            json={"text": f"{emoji} {message}"},
            timeout=5,
        )
    except Exception:
        pass
