"""
OTA Command — Phase 2: Rights Check & Relevance Gate
Pre-flight validation before extraction.
- Copyright/license scan via YouTube Data API
- Content ID flag detection
- Caption availability check
- Final go/no-go decision
"""

import requests
from core.config import get_setting, get_secret
from core.logging.logger import get_logger
from core.errors.handler import retry_with_backoff, notify_slack
from core.dispatch.events import emit_next_phase, Phase

log = get_logger("02_rights_gate")


@retry_with_backoff("02_rights_gate")
def check_rights(video_id: str, payload: dict) -> dict:
    """
    Run all pre-flight checks on a video before extraction.
    Returns enriched payload with rights metadata.
    """
    log.start(f"Rights check for {video_id}")

    results = {
        "license_ok": False,
        "captions_available": False,
        "content_id_clear": True,  # Assume clear unless detected
        "embeddable": True,
        "blocked": False,
        "block_reason": None,
    }

    try:
        api_key = get_secret("youtube_api_key")
        video_data = _fetch_video_details(video_id, api_key)

        if not video_data:
            results["blocked"] = True
            results["block_reason"] = "Video not found or unavailable"
            log.error(f"Video {video_id} not found")
            return results

        # Check license
        status = video_data.get("status", {})
        license_type = status.get("license", "youtube")
        allowed = get_setting("rights_gate", "allowed_licenses", default=["youtube", "creativeCommon"])
        results["license_ok"] = license_type in allowed

        # Check embeddable
        results["embeddable"] = status.get("embeddable", True)

        # Check if content has copyright claims (contentDetails.licensedContent)
        content_details = video_data.get("contentDetails", {})
        if content_details.get("licensedContent", False):
            # licensedContent = true means it's claimed content
            block_on_claim = get_setting("rights_gate", "block_on_copyright_claim", default=True)
            if block_on_claim:
                results["content_id_clear"] = False

        # Check captions
        captions = content_details.get("caption", "false")
        results["captions_available"] = captions == "true"

        # Check region restrictions
        region_restriction = content_details.get("regionRestriction", {})
        if region_restriction.get("blocked"):
            results["blocked"] = True
            results["block_reason"] = "Region-blocked content"

        # Enrich payload
        snippet = video_data.get("snippet", {})
        payload["title"] = snippet.get("title", payload.get("title", ""))
        payload["channel"] = snippet.get("channelTitle", payload.get("channel", ""))
        payload["description"] = snippet.get("description", "")[:1000]
        payload["tags"] = snippet.get("tags", [])
        payload["duration"] = content_details.get("duration", "")
        payload["view_count"] = int(video_data.get("statistics", {}).get("viewCount", 0))
        payload["rights"] = results

    except ValueError:
        # No API key — skip rights check, allow through with warning
        log.warn("No YouTube API key — skipping rights check, passing through")
        results["license_ok"] = True
        results["captions_available"] = True
        payload["rights"] = results

    # Decision
    if results["blocked"]:
        log.error(f"BLOCKED: {results['block_reason']}")
        notify_slack(
            f":no_entry: *Video blocked* — {payload.get('title', video_id)}\n"
            f"Reason: {results['block_reason']}",
        )
        return None

    if not results["license_ok"]:
        log.warn(f"License not in allowed list for {video_id}")
        notify_slack(
            f":warning: *License issue* — {payload.get('title', video_id)}\n"
            f"License type: {license_type}",
        )
        return None

    if not results["content_id_clear"]:
        log.warn(f"Content ID claim detected on {video_id}")
        notify_slack(
            f":warning: *Content ID claim* — {payload.get('title', video_id)}\n"
            f"Video has licensed/claimed content. Skipping.",
        )
        return None

    if not results["captions_available"]:
        require_captions = get_setting("rights_gate", "require_captions", default=False)
        if require_captions:
            log.warn(f"No captions and require_captions=true — blocking {video_id}")
            return None
        else:
            log.info("No captions — will use Whisper fallback in extraction")

    log.success(f"Rights check PASSED for {video_id}")

    # Emit to Phase 3
    event = emit_next_phase(
        current_phase=Phase.RIGHTS_GATE,
        payload=payload,
        video_id=video_id,
        slug=payload.get("slug", video_id),
    )

    return event


def _fetch_video_details(video_id: str, api_key: str) -> dict:
    """Fetch full video metadata from YouTube Data API."""
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,contentDetails,status,statistics",
        "id": video_id,
        "key": api_key,
    }

    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", [])
    return items[0] if items else None


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        vid = sys.argv[1]
        result = check_rights(vid, {"video_id": vid, "url": f"https://youtube.com/watch?v={vid}"})
        print(result)
