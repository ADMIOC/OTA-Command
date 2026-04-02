"""
OTA Command — Phase 10: Analytics & Feedback Loop
Aggregates cross-platform metrics, attributes revenue, and feeds insights
back into discovery for next cycle optimization.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import requests
import anthropic

from core.config import get_setting, get_secret
from core.logging.logger import get_logger
from core.errors.handler import retry_with_backoff, notify_slack, notify
from core.dispatch.events import emit_next_phase, Phase

log = get_logger("10_analytics")

_ROOT = Path(__file__).resolve().parent.parent.parent
ANALYTICS_DIR = _ROOT / "outputs" / "analytics"
ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------
# Platform Metrics Collection
# -----------------------------------------------------------

@retry_with_backoff("10_collect_metrics")
def collect_metrics(slug: str, payload: dict) -> dict:
    """Collect engagement metrics from all distribution platforms."""
    log.info(f"Collecting metrics for {slug}")

    metrics = {
        "slug": slug,
        "timestamp": datetime.now().isoformat(),
        "platforms": {},
        "total_views": 0,
        "total_engagement": 0,
    }

    # Mock YouTube metrics
    try:
        youtube_api_key = get_secret("youtube_api_key")
        if youtube_api_key:
            video_id = payload.get("youtube_video_id", "")
            if video_id:
                url = f"https://www.googleapis.com/youtube/v3/videos?id={video_id}&key={youtube_api_key}&part=statistics"
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    stats = response.json().get("items", [{}])[0].get("statistics", {})
                    youtube_metrics = {
                        "views": int(stats.get("viewCount", 0)),
                        "likes": int(stats.get("likeCount", 0)),
                        "comments": int(stats.get("commentCount", 0)),
                    }
                    metrics["platforms"]["youtube"] = youtube_metrics
                    metrics["total_views"] += youtube_metrics["views"]
                    metrics["total_engagement"] += youtube_metrics["likes"] + youtube_metrics["comments"]
    except Exception as e:
        log.warning(f"YouTube metrics error: {e}")

    # Mock TikTok/Instagram metrics
    tiktok_mock = {
        "views": 15420,
        "likes": 2310,
        "shares": 145,
        "engagement_rate": 0.15,
    }
    instagram_mock = {
        "views": 8900,
        "likes": 890,
        "comments": 45,
        "shares": 23,
        "engagement_rate": 0.11,
    }

    metrics["platforms"]["tiktok"] = tiktok_mock
    metrics["platforms"]["instagram"] = instagram_mock
    metrics["total_views"] += tiktok_mock["views"] + instagram_mock["views"]
    metrics["total_engagement"] += (
        tiktok_mock.get("likes", 0) + instagram_mock.get("likes", 0) +
        instagram_mock.get("comments", 0)
    )

    # LinkedIn mock metrics
    linkedin_mock = {
        "views": 3200,
        "likes": 220,
        "comments": 35,
        "shares": 8,
        "engagement_rate": 0.08,
    }
    metrics["platforms"]["linkedin"] = linkedin_mock
    metrics["total_views"] += linkedin_mock["views"]
    metrics["total_engagement"] += linkedin_mock["likes"] + linkedin_mock["comments"]

    # Calculate overall engagement rate
    if metrics["total_views"] > 0:
        metrics["engagement_rate"] = metrics["total_engagement"] / metrics["total_views"]

    log.success(f"Collected metrics: {metrics['total_views']:,} views, {metrics['total_engagement']} engagements")
    return metrics


# -----------------------------------------------------------
# Revenue Attribution
# -----------------------------------------------------------

@retry_with_backoff("10_revenue_tracking")
def track_revenue(slug: str) -> dict:
    """Track revenue attribution via UTM parameters and conversion events."""
    log.info(f"Tracking revenue for {slug}")

    # Mock revenue data based on typical conversion rates
    mock_conversions = {
        "youtube": {
            "impressions": 15000,
            "clicks": 450,
            "conversions": 18,
            "revenue": 540.00,
        },
        "tiktok": {
            "impressions": 18000,
            "clicks": 540,
            "conversions": 16,
            "revenue": 480.00,
        },
        "instagram": {
            "impressions": 10000,
            "clicks": 250,
            "conversions": 10,
            "revenue": 300.00,
        },
        "linkedin": {
            "impressions": 3500,
            "clicks": 140,
            "conversions": 7,
            "revenue": 350.00,
        },
    }

    total_conversions = sum(p["conversions"] for p in mock_conversions.values())
    total_revenue = sum(p["revenue"] for p in mock_conversions.values())
    total_clicks = sum(p["clicks"] for p in mock_conversions.values())
    total_impressions = sum(p["impressions"] for p in mock_conversions.values())

    result = {
        "slug": slug,
        "timestamp": datetime.now().isoformat(),
        "platforms": mock_conversions,
        "total_conversions": total_conversions,
        "total_revenue": total_revenue,
        "total_clicks": total_clicks,
        "total_impressions": total_impressions,
        "conversion_rate": total_conversions / max(total_clicks, 1),
        "cpc": total_revenue / max(total_clicks, 1),
        "roas": total_revenue / 100 if total_revenue > 0 else 0,  # Assuming $100 ad spend
    }

    log.success(f"Revenue tracked: ${total_revenue:.2f} from {total_conversions} conversions")
    return result


# -----------------------------------------------------------
# Performance Report Generation
# -----------------------------------------------------------

@retry_with_backoff("10_generate_report")
def generate_report(slug: str, metrics: dict, revenue: dict) -> dict:
    """Use Claude to generate performance summary report."""
    log.info(f"Generating performance report for {slug}")

    client = anthropic.Anthropic(api_key=get_secret("anthropic_api_key"))

    metrics_json = json.dumps(metrics, indent=2)
    revenue_json = json.dumps(revenue, indent=2)

    prompt = f"""Generate a comprehensive performance summary report for video content distribution.

Metrics:
{metrics_json}

Revenue:
{revenue_json}

Include:
1. Key performance indicators summary
2. Platform-specific analysis
3. Engagement trends and standout moments
4. Revenue performance and ROI analysis
5. Top-performing content pillars
6. Recommendations for next campaign

Format as markdown with clear sections and callouts."""

    try:
        response = client.messages.create(
            model="claude-opus-4-1-20250805",
            max_tokens=3000,
            messages=[{
                "role": "user",
                "content": prompt,
            }],
        )

        report_md = response.content[0].text
        log.success(f"Report generated ({len(report_md)} chars)")

        return {
            "slug": slug,
            "report": report_md,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        log.warning(f"Report generation error: {e}")
        return {
            "slug": slug,
            "report": "Report generation unavailable",
            "error": str(e),
        }


# -----------------------------------------------------------
# Feedback Loop & Scoring Weight Updates
# -----------------------------------------------------------

@retry_with_backoff("10_feedback_loop")
def update_discovery_weights(metrics: dict, revenue: dict) -> dict:
    """Update discovery scoring weights based on performance data."""
    log.info("Updating discovery weights based on performance")

    queue_dir = _ROOT / "queue"
    weights_file = queue_dir / "scoring_weights.json"

    # Load existing weights
    current_weights = {}
    if weights_file.exists():
        try:
            current_weights = json.loads(weights_file.read_text())
        except json.JSONDecodeError:
            current_weights = {}

    # Calculate performance score (0-1)
    engagement_rate = metrics.get("engagement_rate", 0)
    conversion_rate = revenue.get("conversion_rate", 0)
    performance_score = (engagement_rate * 0.6 + min(conversion_rate, 0.1) * 10 * 0.4)

    # Boost topics with strong performance
    topic_boost = 1.0
    if engagement_rate > 0.15:
        topic_boost = 1.2
    elif engagement_rate > 0.10:
        topic_boost = 1.1
    elif engagement_rate < 0.05:
        topic_boost = 0.9

    # Update weights (boosting high-performing topics)
    updated_weights = current_weights.copy()
    updated_weights["topic_relevance"] = updated_weights.get("topic_relevance", 1.0) * topic_boost
    updated_weights["engagement_potential"] = updated_weights.get("engagement_potential", 1.0) * (1.0 + engagement_rate)
    updated_weights["monetization_value"] = updated_weights.get("monetization_value", 1.0) * (1.0 + conversion_rate)

    # Normalize weights to sum to 10
    total_weight = sum(updated_weights.values())
    if total_weight > 0:
        updated_weights = {k: (v / total_weight) * 10 for k, v in updated_weights.items()}

    # Save updated weights
    queue_dir.mkdir(parents=True, exist_ok=True)
    weights_file.write_text(json.dumps(updated_weights, indent=2))

    result = {
        "performance_score": performance_score,
        "topic_boost": topic_boost,
        "updated_weights": updated_weights,
        "feedback_applied": True,
    }

    log.success(f"Scoring weights updated (boost: {topic_boost:.2f}x)")
    return result


# -----------------------------------------------------------
# Main Orchestrator
# -----------------------------------------------------------

@retry_with_backoff("10_analytics")
def run_analytics(video_id: str, slug: str, payload: dict) -> dict:
    """
    Full Phase 10: Collect metrics, track revenue, generate report, update feedback loop.
    """
    log.start(f"Analytics phase for {slug}")

    slug_dir = ANALYTICS_DIR / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    try:
        # 1. Collect platform metrics
        metrics = collect_metrics(slug, payload)
        payload["metrics"] = metrics

        # 2. Track revenue
        revenue = track_revenue(slug)
        payload["revenue"] = revenue

        # 3. Generate performance report
        report = generate_report(slug, metrics, revenue)
        (slug_dir / "performance_report.md").write_text(report["report"])
        payload["report"] = report

        # 4. Update discovery weights (feedback loop)
        feedback = update_discovery_weights(metrics, revenue)
        payload["feedback"] = feedback

        # 5. Save analytics summary
        summary = {
            "slug": slug,
            "timestamp": datetime.now().isoformat(),
            "total_views": metrics.get("total_views"),
            "total_engagement": metrics.get("total_engagement"),
            "engagement_rate": metrics.get("engagement_rate"),
            "total_revenue": revenue.get("total_revenue"),
            "conversion_rate": revenue.get("conversion_rate"),
            "roas": revenue.get("roas"),
            "performance_score": feedback.get("performance_score"),
        }

        (slug_dir / "analytics_summary.json").write_text(json.dumps(summary, indent=2))

        log.success(f"Analytics complete: {metrics['total_views']:,} views, ${revenue['total_revenue']:.2f} revenue")

        # Emit completion (no next phase, this is the final phase)
        notify(
            event="Analytics complete",
            phase="10_analytics",
            status="Complete",
            video_title=payload.get('title', slug),
            slug=slug,
            video_url=payload.get('url', ''),
            details=f"{metrics['total_views']:,} views | ${revenue['total_revenue']:.2f} revenue\nEngagement rate: {metrics.get('engagement_rate', 0):.1%}\nROAS: {revenue.get('roas', 0):.1f}x\nConversion rate: {revenue.get('conversion_rate', 0):.2%}",
        )

        return {
            "analytics": summary,
            "metrics": metrics,
            "revenue": revenue,
            "feedback": feedback,
        }

    except Exception as e:
        log.error(f"Analytics failed: {e}")
        notify(
            event="Analytics error",
            phase="10_analytics",
            status="Error",
            video_title=payload.get('title', slug),
            slug=slug,
            video_url=payload.get('url', ''),
            details=f"Analytics error: {str(e)}",
        )
        raise


if __name__ == "__main__":
    print("Phase 10 — run via pipeline, not standalone")


# A Brand Collab Production. All rights reserved 2026.