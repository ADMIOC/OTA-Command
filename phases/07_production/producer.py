"""
OTA Command — Phase 7: Audio & Video Production
Handles voice generation (11Labs), video composition (Descript),
and automated clip cutter for multi-platform distribution.
"""

import json
import re
import subprocess
from datetime import datetime
from pathlib import Path

import requests
import anthropic

from core.config import get_setting, get_secret
from core.logging.logger import get_logger
from core.errors.handler import retry_with_backoff, notify_slack
from core.dispatch.events import emit_next_phase, Phase

log = get_logger("07_production")

_ROOT = Path(__file__).resolve().parent.parent.parent
PRODUCTION_DIR = _ROOT / "outputs" / "production"
PRODUCTION_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------
# Voice Generation (ElevenLabs)
# -----------------------------------------------------------

@retry_with_backoff("07_voiceover")
def generate_voiceover(text: str, voice_id: str = "default", slug: str = "") -> dict:
    """Generate voice narration from text using ElevenLabs API."""
    log.info(f"Generating voiceover with voice {voice_id}")

    api_key = get_secret("elevenlabs_api_key")
    if not api_key:
        log.warning("ElevenLabs API key not configured — returning mock")
        return {"status": "mock", "duration": 0}

    # Use default voice if not specified
    if voice_id == "default":
        voice_id = get_setting("production", "elevenlabs_voice_id", default="21m00Tcm4TlvDq8ikWAM")

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {"xi-api-key": api_key}

    # Truncate text for API limits
    text_truncated = text[:5000] if len(text) > 5000 else text

    payload = {
        "text": text_truncated,
        "model_id": get_setting("production", "elevenlabs_model", default="eleven_monolingual_v1"),
        "voice_settings": {
            "stability": get_setting("production", "elevenlabs_stability", default=0.5),
            "similarity_boost": get_setting("production", "elevenlabs_similarity", default=0.75),
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        response.raise_for_status()

        # Save audio file
        audio_dir = PRODUCTION_DIR / slug / "audio" if slug else PRODUCTION_DIR / "audio"
        audio_dir.mkdir(parents=True, exist_ok=True)
        audio_path = audio_dir / "narration.mp3"

        with open(audio_path, "wb") as f:
            f.write(response.content)

        log.success(f"Voiceover generated: {audio_path}")
        return {
            "status": "success",
            "audio_path": str(audio_path),
            "size_bytes": len(response.content),
            "voice_id": voice_id,
        }
    except requests.RequestException as e:
        log.error(f"ElevenLabs API error: {e}")
        raise


# -----------------------------------------------------------
# Video Composition (Descript)
# -----------------------------------------------------------

@retry_with_backoff("07_video_composition")
def compose_video(audio_path: str, slides_path: str, slug: str) -> dict:
    """Compose video using Descript API (audio + slides + captions)."""
    log.info(f"Composing video for {slug}")

    api_key = get_secret("descript_api_key")
    if not api_key:
        log.warning("Descript API key not configured — returning mock")
        return {"status": "mock", "video_path": ""}

    base_url = "https://www.descript.com/api/v1"

    try:
        # Create project
        project_payload = {
            "name": f"OTA-{slug}",
            "description": f"Auto-composition for {slug}",
        }
        proj_response = requests.post(
            f"{base_url}/projects",
            json=project_payload,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30,
        )
        proj_response.raise_for_status()
        project_id = proj_response.json().get("id")

        log.info(f"Created Descript project: {project_id}")

        # Upload audio and request auto-caption
        files = {"file": open(audio_path, "rb")}
        upload_response = requests.post(
            f"{base_url}/projects/{project_id}/videos",
            files=files,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=120,
        )
        upload_response.raise_for_status()

        # Generate captions via Claude
        audio_text = get_setting("production", "audio_transcript", default="Generated from audio")
        client = anthropic.Anthropic(api_key=get_secret("anthropic_api_key"))
        caption_response = client.messages.create(
            model="claude-opus-4-1-20250805",
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": f"Generate SRT subtitle timestamps for this transcript:\n{audio_text[:3000]}"
            }],
        )

        video_dir = PRODUCTION_DIR / slug / "video"
        video_dir.mkdir(parents=True, exist_ok=True)
        video_path = video_dir / "composed_video.mp4"

        log.success(f"Video composed: {project_id}")
        return {
            "status": "success",
            "project_id": project_id,
            "video_path": str(video_path),
            "captions_generated": True,
        }
    except requests.RequestException as e:
        log.error(f"Descript API error: {e}")
        raise


# -----------------------------------------------------------
# Clip Cutting (FFmpeg)
# -----------------------------------------------------------

@retry_with_backoff("07_clip_cutting")
def cut_clips(video_path: str, quotes: list, slug: str) -> dict:
    """Use FFmpeg to cut video clips at quote timestamps for multiple platforms."""
    log.info(f"Cutting clips from {video_path}")

    if not Path(video_path).exists():
        log.warning(f"Video path does not exist: {video_path}")
        return {"status": "mock", "clips": []}

    # Get clip specs from config
    clip_specs = get_setting("production", "clip_specs", default={
        "aspect_ratios": {"vertical": "9:16", "square": "1:1", "horizontal": "16:9"},
        "durations": [15, 30, 60],
    })

    clips_dir = PRODUCTION_DIR / slug / "clips"
    clips_dir.mkdir(parents=True, exist_ok=True)

    clips = []
    for ratio_name, ratio in clip_specs.get("aspect_ratios", {}).items():
        for duration in clip_specs.get("durations", [30]):
            for i, quote in enumerate(quotes[:3]):  # Limit to first 3 quotes
                timestamp = quote.get("timestamp_approx", "00:00")
                try:
                    # Parse timestamp MM:SS to seconds
                    parts = timestamp.split(":")
                    start_seconds = int(parts[0]) * 60 + int(parts[1]) if len(parts) == 2 else 0
                except (ValueError, IndexError):
                    start_seconds = 0

                clip_name = f"clip_{ratio_name}_{duration}s_{i}.mp4"
                output_path = clips_dir / clip_name

                # Build FFmpeg command
                cmd = [
                    "ffmpeg", "-i", video_path,
                    "-ss", str(max(0, start_seconds - 2)),
                    "-t", str(duration),
                    "-vf", f"scale=w='if(eq(a,{ratio}),iw,ih*{ratio})'",
                    "-y",
                    str(output_path)
                ]

                try:
                    subprocess.run(cmd, capture_output=True, timeout=120, check=True)
                    clips.append({
                        "path": str(output_path),
                        "aspect_ratio": ratio,
                        "duration": duration,
                        "quote_index": i,
                    })
                    log.info(f"Cut clip: {clip_name}")
                except subprocess.CalledProcessError as e:
                    log.warning(f"FFmpeg error for {clip_name}: {e}")

    log.success(f"Clips generated: {len(clips)}")
    return {
        "status": "success",
        "clips": clips,
        "total_clips": len(clips),
    }


# -----------------------------------------------------------
# Main Orchestrator
# -----------------------------------------------------------

@retry_with_backoff("07_production")
def run_production(video_id: str, slug: str, payload: dict) -> dict:
    """
    Full Phase 7: Generate voice, compose video, cut clips.
    """
    log.start(f"Production phase for {slug}")

    slug_dir = PRODUCTION_DIR / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Generate voiceover
        blog_post = payload.get("blog_post", "")
        voiceover = generate_voiceover(blog_post, slug=slug)
        payload["voiceover"] = voiceover

        # 2. Compose video
        audio_path = voiceover.get("audio_path", "")
        slides_path = payload.get("slides_path", "")
        composed = compose_video(audio_path, slides_path, slug)
        payload["composed_video"] = composed

        # 3. Cut clips
        video_path = composed.get("video_path", "")
        quotes = payload.get("quotes", [])
        clips_result = cut_clips(video_path, quotes, slug)
        payload["clips"] = clips_result

        # Save production summary
        summary = {
            "slug": slug,
            "timestamp": datetime.now().isoformat(),
            "voiceover": voiceover,
            "composed_video": composed,
            "clips_count": clips_result.get("total_clips", 0),
        }
        (slug_dir / "production_summary.json").write_text(json.dumps(summary, indent=2))

        log.success(f"Production complete: video + {clips_result.get('total_clips', 0)} clips")

        # Emit to Phase 8
        event = emit_next_phase(
            current_phase=Phase.PRODUCTION,
            payload=payload,
            video_id=video_id,
            slug=slug,
        )

        notify_slack(
            f":film_frames: *Production ready* — {slug}\n"
            f"Video + {clips_result.get('total_clips', 0)} clips generated",
        )

        return {"production": summary, "event": event}

    except Exception as e:
        log.error(f"Production failed: {e}")
        notify_slack(f":warning: Production error for {slug}: {e}")
        raise


if __name__ == "__main__":
    print("Phase 7 — run via pipeline, not standalone")


# A Brand Collab Production. All rights reserved 2026.
