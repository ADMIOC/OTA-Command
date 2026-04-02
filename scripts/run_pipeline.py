#!/usr/bin/env python3
"""
OTA Command — Manual Pipeline Runner
Run the full pipeline for a single YouTube URL from the command line.

Usage:
    python scripts/run_pipeline.py "https://www.youtube.com/watch?v=VIDEO_ID"

This bypasses the Discovery Bot (Phase 1) and feeds the URL directly
into Phase 2 (Rights Gate) → Phase 3 (Extraction) → Phase 4 (Storage).

For automated discovery, use the GitHub Actions cron workflow instead.
"""

import sys
import re
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.logging.logger import get_logger
from core.dispatch.events import Phase, create_event, EventStatus

# Dynamic imports to avoid Python 3.10 numeric-prefix parsing issue
import importlib
rights_mod = importlib.import_module("phases.02_rights_gate.checker")
check_rights = rights_mod.check_rights

extract_mod = importlib.import_module("phases.03_extraction.extractor")
run_extraction = extract_mod.run_extraction

log = get_logger("runner")


def extract_video_id(url: str) -> str:
    """Extract video ID from various YouTube URL formats."""
    patterns = [
        r"(?:v=|\/v\/|youtu\.be\/)([a-zA-Z0-9_-]{11})",
        r"(?:embed\/)([a-zA-Z0-9_-]{11})",
        r"(?:shorts\/)([a-zA-Z0-9_-]{11})",
    ]
    for p in patterns:
        match = re.search(p, url)
        if match:
            return match.group(1)
    return url  # Assume raw video ID


def make_slug(title: str) -> str:
    """Convert title to URL-safe slug."""
    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug)
    return slug[:80]


def run(url: str):
    """Run pipeline for a single URL."""
    log.start(f"OTA Command — Manual Pipeline Run")
    log.info(f"URL: {url}")

    video_id = extract_video_id(url)
    log.info(f"Video ID: {video_id}")

    # Phase 2: Rights Check
    log.info("=" * 60)
    log.info("PHASE 2: Rights & Relevance Gate")
    log.info("=" * 60)

    payload = {
        "video_id": video_id,
        "url": url,
        "source": "manual",
    }

    rights_result = check_rights(video_id, payload)
    if rights_result is None:
        log.error("Video blocked by rights check. Exiting.")
        sys.exit(1)

    # Phase 3: Extraction
    log.info("=" * 60)
    log.info("PHASE 3: Transcript Fetch & Claude Extraction")
    log.info("=" * 60)

    slug = make_slug(payload.get("title", video_id))
    extraction_result = run_extraction(url, video_id, slug, payload)

    # Report
    log.info("=" * 60)
    log.success("PIPELINE COMPLETE")
    log.info("=" * 60)
    log.info(f"Video: {payload.get('title', 'Unknown')}")
    log.info(f"Slug: {slug}")

    files = extraction_result.get("files", {})
    for key, info in files.items():
        log.info(f"  → {info['path']} ({info['size']} chars)")

    log.info("")
    log.info("Next step: Open NotebookLM")
    log.info("  1. Go to notebooklm.google.com")
    log.info("  2. Click + New Notebook")
    log.info("  3. Add Source → Google Drive → OTA-Pipeline → OTA-NotebookLM-Source")
    log.info(f"  4. Select: {slug}_notebooklm_source.md")
    log.info("  5. Studio panel → Slides → Detailed Deck → English → Default → Generate")
    log.info("  6. Save completed deck to OTA-Pipeline → OTA-NotebookLM-Output in Drive")
    log.info("")
    log.info("A Brand Collab Production. All rights reserved 2026.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_pipeline.py <YOUTUBE_URL>")
        sys.exit(1)
    run(sys.argv[1])
