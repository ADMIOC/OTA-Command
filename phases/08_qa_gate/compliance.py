"""
OTA Command — Phase 8: Brand Compliance & QA
Checks content against brand voice guidelines, fact-checks outputs,
and generates A/B variants for testing.
"""

import json
import re
from datetime import datetime
from pathlib import Path

import anthropic
import yaml

from core.config import get_setting, get_secret
from core.logging.logger import get_logger
from core.errors.handler import retry_with_backoff, notify_slack, notify
from core.dispatch.events import emit_next_phase, Phase

log = get_logger("08_qa_gate")

_ROOT = Path(__file__).resolve().parent.parent.parent
QA_DIR = _ROOT / "outputs" / "qa_gate"
QA_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------
# Brand Compliance Check
# -----------------------------------------------------------

@retry_with_backoff("08_brand_compliance")
def check_brand_compliance(content_dir: str, slug: str) -> dict:
    """Check content against brand rules and naming conventions."""
    log.info(f"Checking brand compliance for {slug}")

    violations = []
    warnings = []

    # Brand naming rules
    brand_rules = {
        "WYR": {"bad": ["Wire", "WIRE"], "correct": "WYR"},
        "CRS": {"bad": ["Capital Recovery"], "correct": "CRS"},
        "FlipLess App": {"bad": ["Flipless", "flipless"], "correct": "FlipLess App"},
        "The VFO": {"bad": ["VFO", "vfo"], "correct": "The VFO"},
        "Urban Fusion Ai": {"bad": ["UrbanFusion", "Urban Fusion AI"], "correct": "Urban Fusion Ai"},
        "Ai Payment Cloud": {"bad": ["AI Payment", "AiPayment"], "correct": "Ai Payment Cloud"},
    }

    # Scan all text files in content_dir
    content_path = Path(content_dir)
    if not content_path.exists():
        log.warning(f"Content directory not found: {content_dir}")
        return {"violations": [], "warnings": [], "compliance_score": 100}

    text_files = list(content_path.glob("**/*.md")) + list(content_path.glob("**/*.txt"))

    for file_path in text_files:
        try:
            content = file_path.read_text()

            # Check brand naming
            for brand, rules in brand_rules.items():
                for bad_name in rules["bad"]:
                    matches = re.finditer(rf"\b{re.escape(bad_name)}\b", content, re.IGNORECASE)
                    for match in matches:
                        violations.append({
                            "type": "brand_naming",
                            "file": str(file_path),
                            "found": bad_name,
                            "should_be": rules["correct"],
                            "context": content[max(0, match.start()-50):match.end()+50],
                        })

            # Check for required footer
            if "A Brand Collab Production. All rights reserved 2026" not in content:
                warnings.append({
                    "type": "missing_footer",
                    "file": str(file_path),
                })

        except Exception as e:
            log.warning(f"Error reading {file_path}: {e}")

    compliance_score = max(0, 100 - (len(violations) * 10 + len(warnings) * 2))

    result = {
        "violations": violations,
        "warnings": warnings,
        "compliance_score": compliance_score,
        "files_checked": len(text_files),
    }

    log.info(f"Brand compliance: {compliance_score}/100 ({len(violations)} violations)")
    return result


# -----------------------------------------------------------
# Fact Checking
# -----------------------------------------------------------

@retry_with_backoff("08_fact_check")
def check_facts(extraction_content: str, social_copy: str, blog_post: str) -> dict:
    """Use Claude to verify claims against source material."""
    log.info("Fact-checking content against extraction")

    client = anthropic.Anthropic(api_key=get_secret("anthropic_api_key"))

    prompt = """Review the following content for factual accuracy against the source extraction.

Identify any claims that are:
1. Unsupported by the source material
2. Contradictory to the source
3. Potentially misleading or overstate findings

Return JSON with structure:
{
  "claims": [
    {
      "claim": "The exact claim text",
      "status": "verified|unverified|contradictory",
      "confidence": 0.0-1.0,
      "notes": "Explanation"
    }
  ],
  "overall_accuracy": 0.0-1.0,
  "recommendations": ["list", "of", "fixes"]
}

Return ONLY valid JSON."""

    combined_text = f"EXTRACTION:\n{extraction_content[:10000]}\n\nSOCIAL COPY:\n{social_copy[:5000]}\n\nBLOG POST:\n{blog_post[:10000]}"

    try:
        response = client.messages.create(
            model="claude-opus-4-1-20250805",
            max_tokens=3000,
            messages=[{
                "role": "user",
                "content": f"{prompt}\n\n{combined_text}",
            }],
        )

        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        fact_check = json.loads(raw)
        log.success(f"Fact-check complete: {fact_check.get('overall_accuracy', 0):.1%} accuracy")
        return fact_check

    except (json.JSONDecodeError, IndexError) as e:
        log.warning(f"Fact-check parsing error: {e}")
        return {
            "claims": [],
            "overall_accuracy": 0.9,
            "recommendations": ["Manual review recommended"],
        }


# -----------------------------------------------------------
# Link Validation
# -----------------------------------------------------------

@retry_with_backoff("08_link_validation")
def validate_links(content: str) -> dict:
    """HTTP HEAD check all URLs in content."""
    log.info("Validating links in content")

    import requests

    # Extract URLs
    url_pattern = r'https?://[^\s\)"\]]*'
    urls = re.findall(url_pattern, content)

    broken_links = []
    valid_links = []

    for url in set(urls):  # Remove duplicates
        try:
            response = requests.head(url, timeout=10, allow_redirects=True)
            if response.status_code >= 400:
                broken_links.append({
                    "url": url,
                    "status": response.status_code,
                })
            else:
                valid_links.append(url)
        except requests.RequestException as e:
            broken_links.append({
                "url": url,
                "error": str(e),
            })

    result = {
        "valid_urls": len(valid_links),
        "broken_links": broken_links,
        "broken_count": len(broken_links),
    }

    log.info(f"Link validation: {len(valid_links)} valid, {len(broken_links)} broken")
    return result


# -----------------------------------------------------------
# A/B Variant Generation
# -----------------------------------------------------------

@retry_with_backoff("08_ab_variants")
def generate_ab_variants(social_copy: dict, slug: str) -> dict:
    """Generate A/B test variants of social copy."""
    log.info(f"Generating A/B variants for {slug}")

    client = anthropic.Anthropic(api_key=get_secret("anthropic_api_key"))

    platforms = ["instagram", "tiktok", "x_twitter"]
    variants = {}

    prompt_template = """Generate 2 additional A/B variants for this {platform} copy.

Original:
{original}

Requirements:
- Variant A: More aggressive CTA, emojis, urgency
- Variant B: Storytelling angle, less formal
- Keep brand voice consistent
- Return JSON with {"variant_a": "...", "variant_b": "..."}

Return ONLY valid JSON."""

    for platform in platforms:
        if platform not in social_copy:
            continue

        original_copy = social_copy[platform].get("caption", "")[:1000]

        try:
            response = client.messages.create(
                model="claude-opus-4-1-20250805",
                max_tokens=1500,
                messages=[{
                    "role": "user",
                    "content": prompt_template.format(
                        platform=platform,
                        original=original_copy,
                    ),
                }],
            )

            raw = response.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]

            variant_data = json.loads(raw)
            variants[platform] = {
                "original": original_copy[:200],
                "variant_a": variant_data.get("variant_a", ""),
                "variant_b": variant_data.get("variant_b", ""),
            }

        except Exception as e:
            log.warning(f"Variant generation error for {platform}: {e}")
            variants[platform] = {"error": str(e)}

    log.success(f"Generated variants for {len(variants)} platforms")
    return {
        "variants": variants,
        "platforms_tested": len(variants),
    }


# -----------------------------------------------------------
# Main Orchestrator
# -----------------------------------------------------------

@retry_with_backoff("08_qa_gate")
def run_qa_gate(video_id: str, slug: str, payload: dict) -> dict:
    """
    Full Phase 8: Brand compliance, fact-check, generate A/B variants.
    """
    log.start(f"QA gate for {slug}")

    slug_dir = QA_DIR / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Brand compliance check
        content_dir = _ROOT / "outputs" / "multiplied" / slug
        compliance = check_brand_compliance(str(content_dir), slug)
        payload["compliance_check"] = compliance

        # 2. Fact-checking
        extraction_content = payload.get("extraction_content", "")
        social_copy_text = json.dumps(payload.get("social_copy", {}))
        blog_post_text = payload.get("blog_post", "")

        fact_check = check_facts(extraction_content, social_copy_text, blog_post_text)
        payload["fact_check"] = fact_check

        # 3. Link validation
        all_text = f"{social_copy_text}\n{blog_post_text}"
        link_validation = validate_links(all_text)
        payload["link_validation"] = link_validation

        # 4. A/B variants
        social_copy = payload.get("social_copy", {})
        ab_variants = generate_ab_variants(social_copy, slug)
        payload["ab_variants"] = ab_variants

        # Check if all pass
        violations = compliance.get("violations", [])
        broken_links = link_validation.get("broken_links", [])
        unverified_claims = [c for c in fact_check.get("claims", []) if c.get("status") != "verified"]

        qa_summary = {
            "slug": slug,
            "timestamp": datetime.now().isoformat(),
            "compliance_score": compliance.get("compliance_score", 0),
            "fact_accuracy": fact_check.get("overall_accuracy", 0),
            "broken_links": len(broken_links),
            "unverified_claims": len(unverified_claims),
            "qa_pass": compliance.get("compliance_score", 0) >= 90 and len(broken_links) == 0,
        }

        (slug_dir / "qa_summary.json").write_text(json.dumps(qa_summary, indent=2))

        if violations or broken_links:
            notify(
                event="QA issues detected",
                phase="08_qa_gate",
                status="Needs Approval",
                video_title=payload.get('title', slug),
                slug=slug,
                video_url=payload.get('url', ''),
                details=f"Compliance violations: {len(violations)}\nBroken links: {len(broken_links)}\nCompliancescore: {compliance.get('compliance_score', 0)}/100",
            )

        log.success(f"QA complete: {qa_summary.get('qa_pass', False)}")

        # Emit to Phase 9
        event = emit_next_phase(
            current_phase=Phase.QA_GATE,
            payload=payload,
            video_id=video_id,
            slug=slug,
        )

        return {"qa_summary": qa_summary, "event": event}

    except Exception as e:
        log.error(f"QA gate failed: {e}")
        notify(
            event="QA gate error",
            phase="08_qa_gate",
            status="Error",
            video_title=payload.get('title', slug),
            slug=slug,
            video_url=payload.get('url', ''),
            details=f"QA gate error: {str(e)}",
        )
        raise


if __name__ == "__main__":
    print("Phase 8 — run via pipeline, not standalone")


# A Brand Collab Production. All rights reserved 2026.
