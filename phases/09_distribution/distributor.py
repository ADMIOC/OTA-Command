"""
OTA Command — Phase 9: Distribution via Restream
Publishes finalized content to all platforms (YouTube, TikTok, LinkedIn, etc.)
via Restream API, manages content calendar, and schedules posts.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import requests

from core.config import get_setting, get_secret
from core.logging.logger import get_logger
from core.errors.handler import retry_with_backoff, notify_slack
from core.dispatch.events import emit_next_phase, Phase

log = get_logger("09_distribution")

_ROOT = Path(__file__).resolve().parent.parent.parent
DISTRIBUTION_DIR = _ROOT / "outputs" / "distribution"
DISTRIBUTION_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------
# Restream Connection
# -----------------------------------------------------------

@retry_with_backoff("09_restream_auth")
def setup_restream_connection(api_key: str) -> dict:
    """Authenticate with Restream API and list connected channels."""
    log.info("Setting up Restream connection")

    if not api_key:
        log.warning("Restream API key not configured")
        return {"status": "mock", "channels": []}

    base_url = "https://api.restream.io/v2"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    try:
        response = requests.get(f"{base_url}/user", headers=headers, timeout=30)
        response.raise_for_status()

        user_data = response.json()
        log.info(f"Authenticated as {user_data.get('username', 'unknown')}")

        # Get connected channels
        channels_response = requests.get(
            f"{base_url}/channels",
            headers=headers,
            timeout=30,
        )
        channels_response.raise_for_status()
        channels = channels_response.json().get("data", [])

        result = {
            "status": "authenticated",
            "user": user_data.get("username"),
            "channels": [c.get("title") for c in channels],
            "channel_count": len(channels),
        }

        log.success(f"Connected to {len(channels)} channels")
        return result

    except requests.RequestException as e:
        log.warning(f"Restream auth error: {e}")
        return {"status": "error", "error": str(e), "channels": []}


# -----------------------------------------------------------
# Platform Publishing
# -----------------------------------------------------------

@retry_with_backoff("09_platform_publish")
def upload_to_restream(video_path: str, title: str, description: str, schedule: dict) -> dict:
    """Upload video to Restream API for multicast distribution."""
    log.info(f"Uploading video to Restream: {title}")

    api_key = get_secret("restream_api_key")
    if not api_key:
        log.warning("Restream API key not configured — returning mock")
        return {"status": "mock", "scheduled": True}

    base_url = "https://api.restream.io/v2"
    headers = {"Authorization": f"Bearer {api_key}"}

    if not Path(video_path).exists():
        log.warning(f"Video file not found: {video_path}")
        return {"status": "error", "message": "Video file not found"}

    try:
        # Upload video file
        with open(video_path, "rb") as f:
            files = {"file": (Path(video_path).name, f, "video/mp4")}

            # Get scheduled datetime
            scheduled_time = schedule.get("datetime", datetime.now() + timedelta(hours=1))
            if isinstance(scheduled_time, str):
                scheduled_time = datetime.fromisoformat(scheduled_time)

            # Build request payload
            data = {
                "title": title[:100],
                "description": description[:5000],
                "scheduled_start_time": scheduled_time.isoformat(),
            }

            # Determine which channels to publish to
            channels = schedule.get("channels", ["youtube", "tiktok"])
            data["platforms"] = channels

            response = requests.post(
                f"{base_url}/broadcasts",
                headers=headers,
                data=data,
                files=files,
                timeout=300,
            )

            response.raise_for_status()
            broadcast = response.json().get("data", {})

            result = {
                "status": "success",
                "broadcast_id": broadcast.get("id"),
                "scheduled_time": scheduled_time.isoformat(),
                "platforms": channels,
                "title": title,
            }

            log.success(f"Video uploaded: {broadcast.get('id')}")
            return result

    except requests.RequestException as e:
        log.error(f"Restream upload error: {e}")
        raise


# -----------------------------------------------------------
# Content Scheduling
# -----------------------------------------------------------

@retry_with_backoff("09_scheduling")
def calculate_optimal_schedule(slug: str, payload: dict) -> dict:
    """Determine optimal post times per platform."""
    log.info(f"Calculating optimal schedule for {slug}")

    # Read optimal post times from config
    optimal_times = get_setting("distribution", "optimal_post_times", default={
        "youtube": "10:00",
        "tiktok": "19:00",
        "instagram": "12:00",
        "linkedin": "09:00",
    })

    # Read posted content log to check for collisions
    queue_dir = _ROOT / "queue"
    posted_file = queue_dir / "posted.json"

    recently_posted = []
    if posted_file.exists():
        try:
            posted_data = json.loads(posted_file.read_text())
            cutoff = datetime.now() - timedelta(days=7)
            recently_posted = [
                p for p in posted_data
                if datetime.fromisoformat(p.get("posted_at", "")) > cutoff
            ]
        except (json.JSONDecodeError, ValueError):
            log.warning("Could not read posted.json")

    schedule = {}
    base_date = datetime.now() + timedelta(days=1)

    for platform, time_str in optimal_times.items():
        # Parse time
        hour, minute = map(int, time_str.split(":"))
        scheduled_dt = base_date.replace(hour=hour, minute=minute)

        # Check for collisions
        collision_count = sum(
            1 for p in recently_posted
            if p.get("platform") == platform
        )

        if collision_count > 2:  # Already posted 2+ times this week
            scheduled_dt += timedelta(days=3)

        schedule[platform] = {
            "datetime": scheduled_dt.isoformat(),
            "time": time_str,
            "platform": platform,
        }

    log.success(f"Schedule calculated: {len(schedule)} platforms")
    return {"schedule": schedule, "scheduled_date": base_date.isoformat()}


# -----------------------------------------------------------
# Distribution Orchestrator
# -----------------------------------------------------------

@retry_with_backoff("09_distribution")
def run_distribution(video_id: str, slug: str, payload: dict) -> dict:
    """
    Full Phase 9: Upload to Restream, schedule across platforms.
    """
    log.start(f"Distribution phase for {slug}")

    slug_dir = DISTRIBUTION_DIR / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Setup Restream connection
        restream_key = get_secret("restream_api_key")
        auth = setup_restream_connection(restream_key)
        payload["restream_auth"] = auth

        # 2. Calculate schedule
        schedule_result = calculate_optimal_schedule(slug, payload)
        payload["schedule"] = schedule_result

        # 3. Upload video to Restream
        video_path = payload.get("composed_video", {}).get("video_path", "")
        title = payload.get("title", slug)
        description = payload.get("blog_post", "")[:500]

        upload_result = upload_to_restream(
            video_path,
            title,
            description,
            schedule_result.get("schedule", {}),
        )
        payload["upload_result"] = upload_result

        # 4. Save distribution summary
        summary = {
            "slug": slug,
            "timestamp": datetime.now().isoformat(),
            "platforms": auth.get("channels", []),
            "scheduled_date": schedule_result.get("scheduled_date"),
            "broadcast_id": upload_result.get("broadcast_id"),
            "status": upload_result.get("status"),
        }

        (slug_dir / "distribution_summary.json").write_text(json.dumps(summary, indent=2))

        log.success(f"Distribution scheduled: {len(auth.get('channels', []))} platforms")

        # Emit to Phase 10
        event = emit_next_phase(
            current_phase=Phase.DISTRIBUTION,
            payload=payload,
            video_id=video_id,
            slug=slug,
        )

        notify_slack(
            f":rocket: *Distribution scheduled* — {slug}\n"
            f"Published to {len(auth.get('channels', []))} platforms",
        )

        return {"distribution": summary, "event": event}

    except Exception as e:
        log.error(f"Distribution failed: {e}")
        notify_slack(f":warning: Distribution error for {slug}: {e}")
        raise


if __name__ == "__main__":
    print("Phase 9 — run via pipeline, not standalone")


# A Brand Collab Production. All rights reserved 2026.
