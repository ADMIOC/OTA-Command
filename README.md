# OTA Command

**Full automation platform for the OTA content ecosystem.**

YouTube Discovery → Extraction → Content Multiplication → Production → Distribution → Revenue.

10 phases. 28 steps. One system.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        OTA COMMAND                           │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   Phase 1: Discovery Bot                                     │
│   ├── YouTube RSS Feed Watcher                               │
│   ├── YouTube Data API v3 Search                             │
│   ├── WebSub Push Notifications                              │
│   └── Relevance Scoring → Queue                              │
│                    ▼                                         │
│   Phase 2: Rights & Relevance Gate                           │
│   ├── Copyright / License Check                              │
│   ├── Content ID Detection                                   │
│   └── Caption Availability                                   │
│                    ▼                                         │
│   Phase 3: Extraction Engine                                 │
│   ├── yt-dlp Transcript Fetch                                │
│   ├── Whisper Fallback                                       │
│   └── Claude API → 3 Files                                   │
│        ├── extraction_analysis.md                            │
│        ├── notebooklm_source.md                              │
│        └── skill.md                                          │
│                    ▼                                         │
│   Phase 4: Storage & Sync                                    │
│   ├── GitHub Commit                                          │
│   └── Google Drive Sync                                      │
│                    ▼                                         │
│   Phase 5: NotebookLM (Manual Gate)                          │
│   └── Human: Generate deck in NotebookLM                     │
│                    ▼                                         │
│   Phase 6: Content Multiplication                            │
│   ├── Social Copy (per-platform)                             │
│   ├── Blog Post / Newsletter                                 │
│   ├── Thumbnails / Quote Cards                               │
│   └── Quote & Clip Extraction                                │
│                    ▼                                         │
│   Phase 7: Production Studio                                 │
│   ├── ElevenLabs Voice Generation                            │
│   ├── Descript Video Composition                             │
│   └── Auto-Clip Cutter (15s/30s/60s)                         │
│                    ▼                                         │
│   Phase 8: Brand Compliance & QA                             │
│   ├── Brand Voice Checker                                    │
│   ├── Fact Checker & Link Validator                          │
│   └── A/B Variant Generator                                  │
│                    ▼                                         │
│   Phase 9: Distribution via Restream                         │
│   ├── Content Calendar Engine                                │
│   ├── Restream Multicast                                     │
│   └── Instagram / TikTok / FB / X / LinkedIn / YouTube       │
│                    ▼                                         │
│   Phase 10: Analytics & Feedback Loop                        │
│   ├── Cross-Platform Metrics                                 │
│   ├── Revenue Attribution (UTM → Stripe)                     │
│   └── Discovery Bot Feedback Loop                            │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## Quick Start

### Manual Pipeline Run

```bash
# Set environment variables
export ANTHROPIC_API_KEY=sk-ant-...
export YOUTUBE_API_KEY=AIza...

# Install dependencies
pip install -r requirements.txt

# Run pipeline for a single video
python scripts/run_pipeline.py "https://www.youtube.com/watch?v=VIDEO_ID"
```

### Automated Discovery (GitHub Actions)

The Discovery Bot runs every 15 minutes via GitHub Actions. Configure channels and keywords in `config/settings.yaml`, set secrets in the repo settings, and the pipeline runs itself.

## Project Structure

```
OTA-Command/
├── .github/workflows/       # GitHub Actions (event-driven)
│   ├── discovery.yml        # Phase 1 — cron every 15 min
│   ├── rights_gate.yml      # Phase 2 — triggered by dispatch
│   └── extraction.yml       # Phase 3 — triggered by dispatch
├── config/
│   ├── settings.yaml        # All configuration
│   └── secrets.env.example  # Template for secrets
├── core/
│   ├── config.py            # Config loader
│   ├── dispatch/events.py   # Event-driven phase orchestration
│   ├── errors/handler.py    # Retry, dead-letter queue, Slack alerts
│   └── logging/logger.py    # Structured phase logging
├── phases/
│   ├── 01_discovery/        # YouTube monitor + scoring + queue
│   ├── 02_rights_gate/      # Copyright + license + caption check
│   ├── 03_extraction/       # Transcript fetch + Claude analysis
│   ├── 04_storage/          # GitHub commit + Drive sync
│   ├── 05_notebooklm/       # Manual gate
│   ├── 06_multiplication/   # 1 video → 12+ content assets
│   ├── 07_production/       # 11Labs + Descript + clip cutter
│   ├── 08_qa_gate/          # Brand compliance + fact check + A/B
│   ├── 09_distribution/     # Restream multicast + calendar
│   └── 10_analytics/        # Metrics + revenue + feedback loop
├── brand/
│   └── rules.yaml           # Brand names, routing, visual identity
├── scripts/
│   └── run_pipeline.py      # Manual single-video runner
├── tests/                   # Test suite
├── docs/                    # Documentation
└── requirements.txt
```

## Event-Driven Orchestration

Each phase emits a `repository_dispatch` event on completion that triggers the next phase's GitHub Action. If a phase fails, it retries with exponential backoff (30s → 2min → 10min). After 3 failures, the job moves to the dead-letter queue and Slack gets alerted.

```
Phase 1 (Discovery) ──dispatch──→ Phase 2 (Rights)
Phase 2 (Rights)    ──dispatch──→ Phase 3 (Extraction)
Phase 3 (Extraction)──dispatch──→ Phase 4 (Storage)
Phase 4 (Storage)   ──dispatch──→ Phase 5 (NotebookLM) ← MANUAL GATE
Phase 5 (Manual)    ──dispatch──→ Phase 6 (Multiplication)
...and so on
```

## Configuration

All settings live in `config/settings.yaml`. Secrets are injected via environment variables (locally via `secrets.env`, in CI via GitHub repo secrets).

## Brand Rules

The brand compliance engine (`brand/rules.yaml`) enforces OTA naming conventions across all generated content. Key rules:

- **WYR** — never Wire or WIRE
- **CRS** — never Capital Recovery alone
- **FlipLess App** — never Flipless
- **The VFO** — never VFO alone
- **Urban Fusion Ai** — capital A, lowercase i
- **Ai Payment Cloud** — same convention

All outputs end with: **A Brand Collab Production. All rights reserved 2026.**

---

A Brand Collab Production. All rights reserved 2026.
