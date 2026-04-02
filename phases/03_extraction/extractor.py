"""
OTA Command — Phase 3: Transcript Fetch + Claude Extraction Engine
Built from the ground up. No VidEx dependency.

1. Fetch transcript via yt-dlp (auto/manual captions)
2. Fallback to Whisper if no captions available
3. Claude API processes transcript → 3 output files
4. Emit to Phase 4 (Storage)
"""

import json
import re
import subprocess
import tempfile
from pathlib import Path

import anthropic

from core.config import get_setting, get_secret
from core.logging.logger import get_logger
from core.errors.handler import retry_with_backoff
from core.dispatch.events import emit_next_phase, Phase

log = get_logger("03_extraction")

_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = _ROOT / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)


# -----------------------------------------------------------
# Transcript Fetching
# -----------------------------------------------------------

def fetch_transcript(video_url: str, use_whisper_fallback: bool = True) -> str:
    """
    Fetch transcript from YouTube video.
    Tries yt-dlp subtitles first, falls back to Whisper if configured.
    """
    log.info(f"Fetching transcript: {video_url}")

    # Try yt-dlp auto/manual captions
    transcript = _fetch_via_ytdlp(video_url)

    if transcript:
        log.success(f"Transcript fetched via yt-dlp ({len(transcript)} chars)")
        return transcript

    # Fallback: Whisper
    if use_whisper_fallback and get_setting("extraction", "whisper_fallback", default=True):
        log.info("No captions found — attempting Whisper fallback")
        transcript = _fetch_via_whisper(video_url)
        if transcript:
            log.success(f"Transcript fetched via Whisper ({len(transcript)} chars)")
            return transcript

    log.error("No transcript available from any source")
    return ""


def _fetch_via_ytdlp(video_url: str) -> str:
    """Extract subtitles using yt-dlp."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_template = f"{tmpdir}/subs"

        # Try auto-generated captions first, then manual
        for sub_flag in ["--write-auto-sub", "--write-sub"]:
            try:
                cmd = [
                    "yt-dlp",
                    sub_flag,
                    "--sub-lang", "en",
                    "--sub-format", "vtt",
                    "--skip-download",
                    "--no-warnings",
                    "-o", output_template,
                    video_url,
                ]
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=60
                )

                # Find the subtitle file
                for ext in [".en.vtt", ".en.vtt"]:
                    sub_file = Path(f"{output_template}{ext}")
                    if sub_file.exists():
                        raw = sub_file.read_text()
                        return _clean_vtt(raw)

                # Check for any .vtt file
                import glob
                vtt_files = glob.glob(f"{tmpdir}/*.vtt")
                if vtt_files:
                    raw = Path(vtt_files[0]).read_text()
                    return _clean_vtt(raw)

            except subprocess.TimeoutExpired:
                log.warn(f"yt-dlp timed out with {sub_flag}")
            except Exception as e:
                log.warn(f"yt-dlp failed with {sub_flag}: {e}")

    return ""


def _clean_vtt(vtt_text: str) -> str:
    """Clean VTT subtitle file into plain text transcript."""
    lines = []
    for line in vtt_text.split("\n"):
        line = line.strip()
        # Skip VTT headers, timestamps, and empty lines
        if not line:
            continue
        if line.startswith("WEBVTT"):
            continue
        if line.startswith("Kind:") or line.startswith("Language:"):
            continue
        if re.match(r"^\d{2}:\d{2}", line):
            continue
        if line.startswith("NOTE"):
            continue
        # Remove HTML tags
        line = re.sub(r"<[^>]+>", "", line)
        # Remove duplicate lines (common in auto-captions)
        if lines and line == lines[-1]:
            continue
        lines.append(line)

    return " ".join(lines)


def _fetch_via_whisper(video_url: str) -> str:
    """Download audio and transcribe via Whisper (local or API)."""
    # Placeholder — implement when Whisper integration is needed
    log.warn("Whisper fallback not yet implemented")
    return ""


# -----------------------------------------------------------
# Claude Extraction Engine
# -----------------------------------------------------------

EXTRACTION_SYSTEM_PROMPT = """You are the OTA Command extraction engine. Your job is to analyze a video transcript and produce three structured output files.

You MUST follow these brand rules:
- WYR — never Wire or WIRE
- CRS — never Capital Recovery alone
- FlipLess App — never Flipless
- The VFO — never VFO alone
- Urban Fusion Ai — capital A, lowercase i
- Ai Payment Cloud — same convention (capital A, lowercase i)
- All outputs end with: A Brand Collab Production. All rights reserved 2026.

Output exactly 3 sections separated by ===FILE_SEPARATOR===

SECTION 1: Extraction Analysis (detailed breakdown of the video content)
- Executive Summary
- Key Topics & Themes
- Notable Quotes (with approximate timestamps if available)
- Actionable Insights
- Relevance to OTA Brands (map topics to specific brands from the portfolio)
- Content Hooks (viral-worthy moments or angles)

SECTION 2: NotebookLM Source (optimized for Google NotebookLM ingestion)
- Structured as a comprehensive knowledge document
- Clear headings and subheadings
- Key facts, definitions, and relationships
- Designed to generate high-quality slides when fed to NotebookLM

SECTION 3: Skill File (Claude-compatible skill definition)
- Structured as a reusable knowledge module
- Contains the core expertise from this video
- Can be loaded as context for future Claude conversations
- Includes key terminology, frameworks, and decision criteria

A Brand Collab Production. All rights reserved 2026."""


@retry_with_backoff("03_extraction")
def run_extraction(video_url: str, video_id: str, slug: str, payload: dict) -> dict:
    """
    Full extraction pipeline:
    1. Fetch transcript
    2. Send to Claude for analysis
    3. Parse 3 output files
    4. Emit to Phase 4
    """
    log.start(f"Extraction for {slug}")

    # 1. Fetch transcript
    transcript = fetch_transcript(video_url)
    if not transcript:
        raise ValueError(f"No transcript available for {video_url}")

    log.info(f"Transcript length: {len(transcript)} characters")

    # 2. Claude extraction
    api_key = get_secret("anthropic_api_key")
    model = get_setting("extraction", "model", default="claude-sonnet-4-20250514")

    client = anthropic.Anthropic(api_key=api_key)

    title = payload.get("title", "Unknown Video")
    channel = payload.get("channel", "Unknown Channel")

    user_prompt = f"""Analyze this video transcript and produce the three output files.

Video Title: {title}
Channel: {channel}
URL: {video_url}

TRANSCRIPT:
{transcript[:200000]}"""

    log.info(f"Sending to Claude ({model})...")

    response = client.messages.create(
        model=model,
        max_tokens=16000,
        system=EXTRACTION_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    raw_output = response.content[0].text
    log.info(f"Claude response: {len(raw_output)} characters")

    # 3. Parse into 3 files
    files = _parse_extraction_output(raw_output, slug)

    # 4. Enrich payload
    payload["transcript_length"] = len(transcript)
    payload["extraction_files"] = files
    payload["model_used"] = model

    log.success(f"Extraction complete: {len(files)} files generated")

    # 5. Emit to Phase 4 (Storage)
    event = emit_next_phase(
        current_phase=Phase.EXTRACTION,
        payload=payload,
        video_id=video_id,
        slug=slug,
    )

    return {
        "files": files,
        "event": event,
    }


def _parse_extraction_output(raw: str, slug: str) -> dict:
    """Split Claude's output into 3 separate files."""
    parts = raw.split("===FILE_SEPARATOR===")

    files = {}
    file_map = [
        ("extraction_analysis", f"transcripts/{slug}_extraction_analysis.md"),
        ("notebooklm_source", f"notebooklm/{slug}_notebooklm_source.md"),
        ("skill_file", f"skills/{slug}_skill.md"),
    ]

    for i, (key, path) in enumerate(file_map):
        content = parts[i].strip() if i < len(parts) else f"[Generation failed for {key}]"

        # Ensure output dirs exist
        full_path = OUTPUT_DIR / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(content)

        files[key] = {
            "path": str(path),
            "full_path": str(full_path),
            "size": len(content),
        }
        log.info(f"  → {path} ({len(content)} chars)")

    return files


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        url = sys.argv[1]
        vid_id = url.split("v=")[-1].split("&")[0] if "v=" in url else url.split("/")[-1]
        slug_name = f"manual_{vid_id}"
        result = run_extraction(url, vid_id, slug_name, {"video_id": vid_id, "url": url})
        print(json.dumps(result, indent=2, default=str))
