"""
OTA Command — Phase 1: YouTube Discovery Bot
Monitors target channels via RSS feeds and YouTube Data API.
Scores relevance and queues videos for pipeline processing.

Three discovery methods:
  1. RSS Feed Watcher — zero API quota, polls channel feeds
  2. YouTube Data API v3 — keyword/topic search with filters
  3. WebSub (PubSubHubbub) — real-time push (requires webhook server)

This module implements methods 1 and 2 (serverless-compatible).
Method 3 requires the webhook server (see scripts/websub_server.py).
"""

import re
import json
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

from core.config import get_setting, get_secret
from core.logging.logger import get_logger
from core.errors.handler import retry_with_backoff, notify_slack
from core.dispatch.events import create_event, emit_next_phase, Phase, EventStatus

log = get_logger("01_discovery")

_ROOT = Path(__file__).resolve().parent.parent.parent
SEEN_FILE = _ROOT / "queue" / "seen_videos.json"


def _load_seen() -> set:
    """Load set of previously seen video IDs."""
    if SEEN_FILE.exists():
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def _save_seen(seen: set):
    """Persist seen video IDs."""
    with open(SEEN_FILE, "w") as f:
        json.dump(sorted(seen), f)


# -----------------------------------------------------------
# Method 1: RSS Feed Watcher
# -----------------------------------------------------------

def discover_via_rss(channel_ids: list[str] = None) -> list[dict]:
    """
    Poll YouTube RSS feeds for each monitored channel.
    Returns list of new (unseen) videos with metadata.
    Zero API quota cost.
    """
    log.start("RSS Discovery scan")

    if channel_ids is None:
        channel_ids = get_setting("discovery", "channels", default=[])

    if not channel_ids:
        log.warn("No channels configured in settings.yaml → discovery.channels")
        return []

    seen = _load_seen()
    new_videos = []

    for cid in channel_ids:
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}"
        try:
            resp = requests.get(feed_url, timeout=15)
            resp.raise_for_status()
            videos = _parse_rss_feed(resp.text, seen)
            new_videos.extend(videos)
            log.info(f"Channel {cid}: found {len(videos)} new videos")
        except Exception as e:
            log.error(f"RSS fetch failed for channel {cid}: {e}")

    # Mark as seen
    for v in new_videos:
        seen.add(v["video_id"])
    _save_seen(seen)

    log.success(f"RSS scan complete: {len(new_videos)} new videos found")
    return new_videos


def _parse_rss_feed(xml_text: str, seen: set) -> list[dict]:
    """Parse Atom feed XML and extract video entries."""
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }

    root = ET.fromstring(xml_text)
    videos = []

    for entry in root.findall("atom:entry", ns):
        video_id = entry.find("yt:videoId", ns)
        if video_id is None:
            continue
        vid = video_id.text
        if vid in seen:
            continue

        title_el = entry.find("atom:title", ns)
        published_el = entry.find("atom:published", ns)
        author_el = entry.find("atom:author/atom:name", ns)

        videos.append({
            "video_id": vid,
            "url": f"https://www.youtube.com/watch?v={vid}",
            "title": title_el.text if title_el is not None else "",
            "channel": author_el.text if author_el is not None else "",
            "published": published_el.text if published_el is not None else "",
            "source": "rss",
        })

    return videos


# -----------------------------------------------------------
# Method 2: YouTube Data API v3
# -----------------------------------------------------------

@retry_with_backoff("01_discovery_api")
def discover_via_api(query: str = None) -> list[dict]:
    """
    Search YouTube Data API v3 for videos matching configured keywords.
    Uses API quota — use sparingly or as supplement to RSS.
    """
    log.start("API Discovery search")

    api_key = get_secret("youtube_api_key")
    keywords = get_setting("discovery", "keywords", default=[])

    if query:
        keywords = [query]

    if not keywords:
        log.warn("No keywords configured in settings.yaml → discovery.keywords")
        return []

    seen = _load_seen()
    new_videos = []

    filters = get_setting("discovery", "filters", default={})
    min_dur = filters.get("min_duration_seconds", 120)
    max_dur = filters.get("max_duration_seconds", 14400)
    language = filters.get("language", "en")

    for kw in keywords:
        try:
            url = "https://www.googleapis.com/youtube/v3/search"
            params = {
                "part": "snippet",
                "q": kw,
                "type": "video",
                "order": "date",
                "maxResults": 10,
                "relevanceLanguage": language,
                "publishedAfter": (
                    datetime.now(timezone.utc) - timedelta(days=7)
                ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "key": api_key,
            }

            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            for item in data.get("items", []):
                vid = item["id"]["videoId"]
                if vid in seen:
                    continue

                snippet = item["snippet"]
                new_videos.append({
                    "video_id": vid,
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "title": snippet.get("title", ""),
                    "channel": snippet.get("channelTitle", ""),
                    "published": snippet.get("publishedAt", ""),
                    "description": snippet.get("description", "")[:500],
                    "source": "api",
                    "keyword": kw,
                })

            log.info(f"Keyword '{kw}': found {len(data.get('items', []))} results")

        except Exception as e:
            log.error(f"API search failed for keyword '{kw}': {e}")
            raise

    # Mark as seen
    for v in new_videos:
        seen.add(v["video_id"])
    _save_seen(seen)

    log.success(f"API scan complete: {len(new_videos)} new videos found")
    return new_videos


# -----------------------------------------------------------
# Scoring & Queue
# -----------------------------------------------------------

def score_video(video: dict) -> float:
    """
    Score a video's relevance to OTA content (0.0 - 1.0).
    Based on keyword match density, channel authority, and title signals.
    """
    score = 0.5  # Base score

    keywords = get_setting("discovery", "keywords", default=[])
    title = (video.get("title", "") + " " + video.get("description", "")).lower()

    # Keyword match bonus
    matches = sum(1 for kw in keywords if kw.lower() in title)
    if matches > 0:
        score += min(matches * 0.1, 0.3)

    # Channel authority (placeholder — would use subscriber count from API)
    # For now, returning channels add a small bonus
    monitored = get_setting("discovery", "channels", default=[])
    # Can't directly compare without channel_id in video dict from RSS
    # This gets enhanced when we pull channel stats

    # Recency bonus — newer = higher
    try:
        pub = datetime.fromisoformat(video.get("published", "").replace("Z", "+00:00"))
        age_hours = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
        if age_hours < 24:
            score += 0.15
        elif age_hours < 72:
            score += 0.05
    except (ValueError, TypeError):
        pass

    return round(min(score, 1.0), 3)


def queue_video(video: dict, score: float) -> dict:
    """
    Evaluate score against thresholds and either auto-approve,
    send for manual review, or auto-reject.
    """
    auto_threshold = get_setting("discovery", "auto_approve_threshold", default=0.85)
    manual_threshold = get_setting("discovery", "manual_review_threshold", default=0.50)

    video["score"] = score

    if score >= auto_threshold:
        video["decision"] = "auto_approved"
        log.info(f"Auto-approved: {video['title'][:60]} (score={score})")

        # Emit to Phase 2
        event = emit_next_phase(
            current_phase=Phase.DISCOVERY,
            payload=video,
            video_id=video["video_id"],
            slug=_make_slug(video["title"]),
        )
        return event

    elif score >= manual_threshold:
        video["decision"] = "needs_review"
        log.info(f"Needs review: {video['title'][:60]} (score={score})")

        # Create event as requires_approval
        event = create_event(
            phase=Phase.RIGHTS_GATE,
            status=EventStatus.REQUIRES_APPROVAL,
            payload=video,
            video_id=video["video_id"],
            slug=_make_slug(video["title"]),
        )

        # Slack notification
        notify_slack(
            f"*New video needs review* (score: {score})\n"
            f"*{video['title']}*\n"
            f"{video['url']}\n"
            f"Approve or reject in the queue.",
            emoji=":eyes:",
        )
        return event

    else:
        video["decision"] = "auto_rejected"
        log.info(f"Auto-rejected: {video['title'][:60]} (score={score})")
        return None


def _make_slug(title: str) -> str:
    """Convert title to URL-safe slug."""
    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug)
    return slug[:80]


# -----------------------------------------------------------
# Main Discovery Runner
# -----------------------------------------------------------

@retry_with_backoff("01_discovery")
def run_discovery():
    """Run full discovery cycle: RSS + API → Score → Queue."""
    log.start("Full discovery cycle")

    # 1. Collect videos from all sources
    all_videos = []
    all_videos.extend(discover_via_rss())

    # Only use API if key is available
    try:
        get_secret("youtube_api_key")
        all_videos.extend(discover_via_api())
    except ValueError:
        log.info("No YouTube API key — skipping API discovery, RSS only")

    # 2. Deduplicate
    unique = {}
    for v in all_videos:
        if v["video_id"] not in unique:
            unique[v["video_id"]] = v
    videos = list(unique.values())

    log.info(f"Total unique new videos: {len(videos)}")

    # 3. Score and queue each
    results = {"approved": 0, "review": 0, "rejected": 0}
    for video in videos:
        score = score_video(video)
        result = queue_video(video, score)
        if result:
            if video.get("decision") == "auto_approved":
                results["approved"] += 1
            else:
                results["review"] += 1
        else:
            results["rejected"] += 1

    log.success(
        f"Discovery complete: {results['approved']} approved, "
        f"{results['review']} for review, {results['rejected']} rejected"
    )

    return results


if __name__ == "__main__":
    run_discovery()
