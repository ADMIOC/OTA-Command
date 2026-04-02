"""
OTA Command — Phase 6: Content Multiplication Engine
Turns 1 video extraction into 12+ content assets.

Generates:
  1. Platform-specific social copy (IG, TikTok, FB, X, LinkedIn, YT)
  2. Long-form blog post (SEO-optimized for ownthealgo.com)
  3. Email newsletter edition
  4. Quote cards / pull quotes with timestamps
  5. Thumbnail prompts for AI image generation
  6. Video clip timestamps for auto-cutting

All generation uses Claude API against the extraction analysis.
"""

import json
from pathlib import Path

import anthropic

from core.config import get_setting, get_secret
from core.logging.logger import get_logger
from core.errors.handler import retry_with_backoff, notify_slack
from core.dispatch.events import emit_next_phase, Phase

log = get_logger("06_multiplication")

_ROOT = Path(__file__).resolve().parent.parent.parent
MULTIPLY_DIR = _ROOT / "outputs" / "multiplied"
MULTIPLY_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------
# Social Copy Generator
# -----------------------------------------------------------

SOCIAL_COPY_PROMPT = """You are the OTA Command social copy engine. Given a video extraction analysis, generate platform-optimized social media content.

Brand rules:
- WYR — never Wire or WIRE
- CRS — never Capital Recovery alone
- FlipLess App — never Flipless
- The VFO — never VFO alone
- Urban Fusion Ai — capital A, lowercase i
- Ai Payment Cloud — same convention
- All outputs end with: A Brand Collab Production. All rights reserved 2026.

Generate copy for each platform in this exact JSON format:
{
  "instagram": {
    "caption": "...(max 2200 chars, emoji-friendly, 20-30 hashtags at end)...",
    "hook": "...(first line that appears before 'more')...",
    "cta": "...",
    "carousel_slides": ["slide1_text", "slide2_text", "...up to 10"]
  },
  "tiktok": {
    "caption": "...(max 300 chars, punchy, 3-5 hashtags)...",
    "hook": "...(first 3 seconds script)...",
    "cta": "..."
  },
  "facebook": {
    "post": "...(conversational, 1-3 paragraphs, link-friendly)...",
    "hook": "...",
    "cta": "..."
  },
  "x_twitter": {
    "tweet": "...(max 280 chars)...",
    "thread": ["tweet1", "tweet2", "...up to 8 tweets"],
    "hook": "..."
  },
  "linkedin": {
    "post": "...(professional tone, thought leadership angle, 1-3 paragraphs)...",
    "hook": "...",
    "cta": "..."
  },
  "youtube_shorts": {
    "title": "...(max 100 chars)...",
    "description": "...(max 5000 chars, SEO keywords)...",
    "tags": ["tag1", "tag2", "..."]
  }
}

Return ONLY valid JSON. No markdown fences. No explanation."""


@retry_with_backoff("06_social_copy")
def generate_social_copy(extraction_content: str, title: str, url: str) -> dict:
    """Generate platform-specific social media copy."""
    log.info("Generating social copy for all platforms")

    client = anthropic.Anthropic(api_key=get_secret("anthropic_api_key"))
    model = get_setting("extraction", "model", default="claude-sonnet-4-20250514")

    response = client.messages.create(
        model=model,
        max_tokens=8000,
        system=SOCIAL_COPY_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Video Title: {title}\nVideo URL: {url}\n\nEXTRACTION ANALYSIS:\n{extraction_content[:50000]}"
        }],
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]

    try:
        social_copy = json.loads(raw)
    except json.JSONDecodeError:
        log.error("Failed to parse social copy JSON — saving raw output")
        social_copy = {"raw": raw, "parse_error": True}

    log.success(f"Social copy generated for {len(social_copy)} platforms")
    return social_copy


# -----------------------------------------------------------
# Blog Post Generator
# -----------------------------------------------------------

BLOG_PROMPT = """You are the OTA Command blog writer. Convert this video extraction into a long-form, SEO-optimized blog post for ownthealgo.com.

Requirements:
- 1500-2500 words
- H1 title (compelling, keyword-rich)
- Meta description (155 chars max)
- Natural keyword integration
- Internal links to relevant OTA properties where topically appropriate:
  ownthealgo.com, podcast.ownthealgo.com, thealgoacademy.com,
  UrbanFusion.ai, TrustBid.App, FlipLess.App, aipayment.cloud, wyrios.com
- Include 2-3 pull quotes from the video
- End with CTA and footer: A Brand Collab Production. All rights reserved 2026.

Output as clean markdown with frontmatter:
---
title: "..."
meta_description: "..."
slug: "..."
tags: [...]
author: "OTA"
---

[article body]"""


@retry_with_backoff("06_blog")
def generate_blog_post(extraction_content: str, title: str, url: str) -> str:
    """Generate SEO-optimized blog post."""
    log.info("Generating blog post")

    client = anthropic.Anthropic(api_key=get_secret("anthropic_api_key"))
    model = get_setting("extraction", "model", default="claude-sonnet-4-20250514")

    response = client.messages.create(
        model=model,
        max_tokens=6000,
        system=BLOG_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Source Video: {title}\nURL: {url}\n\nEXTRACTION:\n{extraction_content[:50000]}"
        }],
    )

    blog = response.content[0].text.strip()
    log.success(f"Blog post generated ({len(blog)} chars)")
    return blog


# -----------------------------------------------------------
# Newsletter Generator
# -----------------------------------------------------------

NEWSLETTER_PROMPT = """You are the OTA Command newsletter writer. Convert this extraction into an email newsletter edition.

Format:
- Subject line (compelling, under 60 chars) + 2 A/B variants
- Preview text (90 chars max)
- Opening hook (1-2 sentences)
- 3-4 key takeaways as short paragraphs (not bullet points)
- Featured quote from the video
- CTA linking to the full blog post
- Footer: A Brand Collab Production. All rights reserved 2026.

Output as markdown with frontmatter:
---
subject: "..."
subject_ab_1: "..."
subject_ab_2: "..."
preview_text: "..."
---

[newsletter body]"""


@retry_with_backoff("06_newsletter")
def generate_newsletter(extraction_content: str, title: str, blog_post: str) -> str:
    """Generate email newsletter edition."""
    log.info("Generating newsletter")

    client = anthropic.Anthropic(api_key=get_secret("anthropic_api_key"))
    model = get_setting("extraction", "model", default="claude-sonnet-4-20250514")

    response = client.messages.create(
        model=model,
        max_tokens=3000,
        system=NEWSLETTER_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Video: {title}\n\nEXTRACTION:\n{extraction_content[:30000]}\n\nBLOG POST:\n{blog_post[:10000]}"
        }],
    )

    newsletter = response.content[0].text.strip()
    log.success(f"Newsletter generated ({len(newsletter)} chars)")
    return newsletter


# -----------------------------------------------------------
# Quote & Clip Extractor
# -----------------------------------------------------------

QUOTES_PROMPT = """You are the OTA Command quote extractor. Identify the most powerful, quotable, shareable moments from this video analysis.

Extract exactly 5-8 quotes. Return this exact JSON format:
{
  "quotes": [
    {
      "text": "The exact quote or paraphrased key insight",
      "speaker": "Speaker name if known",
      "timestamp_approx": "MM:SS (approximate if available)",
      "context": "1-sentence context for why this matters",
      "platforms": ["instagram", "x_twitter"],
      "clip_worthy": true,
      "clip_duration_seconds": 30
    }
  ]
}

Focus on: contrarian statements, specific data points, emotional moments, actionable advice, quotable one-liners.
Return ONLY valid JSON."""


@retry_with_backoff("06_quotes")
def extract_quotes(extraction_content: str, title: str) -> dict:
    """Extract quotable moments and clip timestamps."""
    log.info("Extracting quotes and clip timestamps")

    client = anthropic.Anthropic(api_key=get_secret("anthropic_api_key"))
    model = get_setting("extraction", "model", default="claude-sonnet-4-20250514")

    response = client.messages.create(
        model=model,
        max_tokens=4000,
        system=QUOTES_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Video: {title}\n\nEXTRACTION:\n{extraction_content[:50000]}"
        }],
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]

    try:
        quotes = json.loads(raw)
    except json.JSONDecodeError:
        quotes = {"raw": raw, "parse_error": True}

    log.success(f"Extracted {len(quotes.get('quotes', []))} quotes")
    return quotes


# -----------------------------------------------------------
# Thumbnail Prompt Generator
# -----------------------------------------------------------

THUMBNAIL_PROMPT = """Generate AI image prompts for video thumbnails and quote cards.

Return this exact JSON:
{
  "thumbnail": {
    "prompt": "Detailed image generation prompt for main video thumbnail",
    "text_overlay": "Short text to overlay (max 6 words)",
    "dimensions": "1280x720"
  },
  "quote_cards": [
    {
      "quote": "The quote text",
      "prompt": "Background image prompt for this quote card",
      "dimensions": "1080x1080"
    }
  ],
  "story_cover": {
    "prompt": "Image prompt for Instagram/TikTok story cover",
    "text_overlay": "Short hook text",
    "dimensions": "1080x1920"
  }
}

Use OTA brand colors: primary #7b2fff, secondary #00d4ff, accent #ff2d87.
Return ONLY valid JSON."""


@retry_with_backoff("06_thumbnails")
def generate_thumbnail_prompts(extraction_content: str, title: str, quotes: dict) -> dict:
    """Generate AI image prompts for thumbnails and visual assets."""
    log.info("Generating thumbnail and visual prompts")

    client = anthropic.Anthropic(api_key=get_secret("anthropic_api_key"))
    model = get_setting("extraction", "model", default="claude-sonnet-4-20250514")

    quote_text = json.dumps(quotes.get("quotes", [])[:5], indent=2)

    response = client.messages.create(
        model=model,
        max_tokens=4000,
        system=THUMBNAIL_PROMPT,
        messages=[{
            "role": "user",
            "content": f"Video: {title}\n\nKEY QUOTES:\n{quote_text}\n\nEXTRACTION:\n{extraction_content[:20000]}"
        }],
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]

    try:
        visuals = json.loads(raw)
    except json.JSONDecodeError:
        visuals = {"raw": raw, "parse_error": True}

    log.success("Thumbnail prompts generated")
    return visuals


# -----------------------------------------------------------
# Main Orchestrator
# -----------------------------------------------------------

@retry_with_backoff("06_multiplication")
def run_multiplication(video_id: str, slug: str, payload: dict) -> dict:
    """
    Full Phase 6: Generate all content assets from extraction.
    """
    log.start(f"Content multiplication for {slug}")

    # Load extraction analysis
    extraction_path = _ROOT / "outputs" / "transcripts" / f"{slug}_extraction_analysis.md"
    if not extraction_path.exists():
        files = payload.get("extraction_files", {})
        ea = files.get("extraction_analysis", {})
        extraction_path = Path(ea.get("full_path", ""))

    if not extraction_path.exists():
        raise FileNotFoundError(f"Extraction analysis not found for {slug}")

    extraction_content = extraction_path.read_text()
    title = payload.get("title", slug)
    url = payload.get("url", "")

    results = {}
    slug_dir = MULTIPLY_DIR / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    # 1. Social Copy
    if get_setting("multiplication", "generate_social_copy", default=True):
        social = generate_social_copy(extraction_content, title, url)
        (slug_dir / "social_copy.json").write_text(json.dumps(social, indent=2))
        results["social_copy"] = social

    # 2. Blog Post
    blog = ""
    if get_setting("multiplication", "generate_blog_post", default=True):
        blog = generate_blog_post(extraction_content, title, url)
        (slug_dir / "blog_post.md").write_text(blog)
        results["blog_post"] = {"path": str(slug_dir / "blog_post.md"), "size": len(blog)}

    # 3. Quotes & Clips
    quotes = extract_quotes(extraction_content, title)
    (slug_dir / "quotes.json").write_text(json.dumps(quotes, indent=2))
    results["quotes"] = quotes

    # 4. Newsletter
    if get_setting("multiplication", "generate_newsletter", default=True):
        newsletter = generate_newsletter(extraction_content, title, blog)
        (slug_dir / "newsletter.md").write_text(newsletter)
        results["newsletter"] = {"path": str(slug_dir / "newsletter.md"), "size": len(newsletter)}

    # 5. Thumbnail Prompts
    if get_setting("multiplication", "generate_thumbnails", default=True):
        visuals = generate_thumbnail_prompts(extraction_content, title, quotes)
        (slug_dir / "visual_prompts.json").write_text(json.dumps(visuals, indent=2))
        results["visuals"] = visuals

    # Count total assets
    asset_count = 0
    if "social_copy" in results and not results["social_copy"].get("parse_error"):
        asset_count += len(results["social_copy"])
    asset_count += 1 if "blog_post" in results else 0
    asset_count += 1 if "newsletter" in results else 0
    asset_count += len(quotes.get("quotes", []))
    asset_count += 1 if "visuals" in results else 0

    payload["multiplied_assets"] = results
    payload["asset_count"] = asset_count

    log.success(f"Multiplication complete: {asset_count} assets generated")

    # Emit to Phase 7
    event = emit_next_phase(
        current_phase=Phase.MULTIPLICATION,
        payload=payload,
        video_id=video_id,
        slug=slug,
    )

    notify_slack(
        f":sparkles: *Content multiplied* — {title[:50]}\n"
        f"{asset_count} assets generated from 1 video",
        emoji="",
    )

    return {"assets": results, "count": asset_count, "event": event}


if __name__ == "__main__":
    print("Phase 6 — run via pipeline, not standalone")


# A Brand Collab Production. All rights reserved 2026.
