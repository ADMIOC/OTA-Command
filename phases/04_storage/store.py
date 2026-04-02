"""
OTA Command — Phase 4: Storage & Sync
Commits extracted files to the OTA-Command GitHub repo and syncs
the NotebookLM source to Google Drive for deck generation.

Flow:
  1. Commit 3 extraction files to GitHub via API (no git clone needed)
  2. Sync notebooklm_source.md to Google Drive OTA-NotebookLM-Source folder
  3. Emit event to Phase 5 (NotebookLM manual gate)
"""

import base64
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

from core.config import get_setting, get_secret
from core.logging.logger import get_logger
from core.errors.handler import retry_with_backoff, notify_slack, notify
from core.dispatch.events import emit_next_phase, Phase

log = get_logger("04_storage")

_ROOT = Path(__file__).resolve().parent.parent.parent
REPO = "ADMIOC/OTA-Command"


# -----------------------------------------------------------
# GitHub Storage
# -----------------------------------------------------------

@retry_with_backoff("04_storage_github")
def store_to_github(slug: str, files: dict, payload: dict) -> dict:
    """
    Commit extraction files to the OTA-Command repo via GitHub Contents API.
    No git clone needed — pure API commits.

    Args:
        slug: Video slug (used for filenames)
        files: Dict from Phase 3 with file paths and content locations
        payload: Full pipeline payload

    Returns:
        dict with commit_shas and file URLs
    """
    log.start("Committing files to GitHub")

    token = get_secret("github_token")
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    committed = {}
    commit_message = (
        f"[OTA Command] Extract: {payload.get('title', slug)[:60]}\n\n"
        f"Video: {payload.get('url', '')}\n"
        f"Channel: {payload.get('channel', '')}\n"
        f"Pipeline: automated\n\n"
        f"A Brand Collab Production. All rights reserved 2026."
    )

    for key, info in files.items():
        file_path = info["path"]  # e.g., "transcripts/slug_extraction_analysis.md"
        full_path = Path(info["full_path"])

        if not full_path.exists():
            log.error(f"File not found: {full_path}")
            continue

        content = full_path.read_text()
        content_b64 = base64.b64encode(content.encode()).decode()

        # Check if file exists (to get SHA for update)
        api_url = f"https://api.github.com/repos/{REPO}/contents/{file_path}"
        existing = requests.get(api_url, headers=headers, timeout=10)
        sha = None
        if existing.status_code == 200:
            sha = existing.json().get("sha")

        # Commit file
        commit_data = {
            "message": commit_message,
            "content": content_b64,
            "branch": "main",
        }
        if sha:
            commit_data["sha"] = sha

        resp = requests.put(api_url, headers=headers, json=commit_data, timeout=15)

        if resp.status_code in (200, 201):
            result = resp.json()
            committed[key] = {
                "path": file_path,
                "sha": result.get("content", {}).get("sha", ""),
                "url": result.get("content", {}).get("html_url", ""),
                "size": len(content),
            }
            log.info(f"  ✓ {file_path}")
        else:
            log.error(f"  ✗ {file_path}: {resp.status_code} {resp.text[:200]}")

    log.success(f"GitHub commit complete: {len(committed)}/{len(files)} files")
    return committed


# -----------------------------------------------------------
# Google Drive Sync
# -----------------------------------------------------------

@retry_with_backoff("04_storage_drive")
def sync_to_drive(slug: str, files: dict) -> dict:
    """
    Upload NotebookLM source file to Google Drive shared folder.
    Uses Google Drive API v3 with service account auth.

    Args:
        slug: Video slug
        files: Dict of committed files

    Returns:
        dict with drive_file_id and web_link
    """
    log.start("Syncing to Google Drive")

    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not sa_json:
        log.warn("No GOOGLE_SERVICE_ACCOUNT_JSON — skipping Drive sync")
        return {"status": "skipped", "reason": "no_service_account"}

    # Target: notebooklm source file
    nlm_file = files.get("notebooklm_source")
    if not nlm_file:
        log.warn("No notebooklm_source file to sync")
        return {"status": "skipped", "reason": "no_nlm_file"}

    source_path = Path(nlm_file.get("full_path", ""))
    if not source_path.exists():
        # Try reading from the output dir
        source_path = _ROOT / "outputs" / nlm_file["path"]

    if not source_path.exists():
        log.error(f"NLM source file not found at {source_path}")
        return {"status": "error", "reason": "file_not_found"}

    try:
        # Decode service account JSON from base64
        import base64 as b64
        sa_data = json.loads(b64.b64decode(sa_json))

        # Get access token via service account JWT
        access_token = _get_drive_access_token(sa_data)

        # Upload to OTA-NotebookLM-Source folder
        nlm_source_folder_id = "16U-G6Rs-aTeskSYIYsaRAaJkBe_Upit5"

        file_content = source_path.read_text()
        filename = f"{slug}_notebooklm_source.md"

        # Check if file already exists in folder
        search_url = "https://www.googleapis.com/drive/v3/files"
        search_params = {
            "q": f"name='{filename}' and '{nlm_source_folder_id}' in parents and trashed=false",
            "fields": "files(id,name)",
        }
        search_resp = requests.get(
            search_url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=search_params,
            timeout=10,
        )

        existing_id = None
        if search_resp.status_code == 200:
            existing_files = search_resp.json().get("files", [])
            if existing_files:
                existing_id = existing_files[0]["id"]

        if existing_id:
            # Update existing file
            upload_url = f"https://www.googleapis.com/upload/drive/v3/files/{existing_id}"
            resp = requests.patch(
                upload_url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "text/markdown",
                },
                params={"uploadType": "media"},
                data=file_content.encode(),
                timeout=30,
            )
        else:
            # Create new file
            metadata = {
                "name": filename,
                "parents": [nlm_source_folder_id],
                "mimeType": "text/markdown",
            }

            # Multipart upload
            import io
            boundary = "ota_command_boundary"
            body = (
                f"--{boundary}\r\n"
                f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
                f"{json.dumps(metadata)}\r\n"
                f"--{boundary}\r\n"
                f"Content-Type: text/markdown\r\n\r\n"
                f"{file_content}\r\n"
                f"--{boundary}--"
            )

            resp = requests.post(
                "https://www.googleapis.com/upload/drive/v3/files",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": f"multipart/related; boundary={boundary}",
                },
                params={"uploadType": "multipart", "fields": "id,webViewLink"},
                data=body.encode(),
                timeout=30,
            )

        if resp.status_code in (200, 201):
            result = resp.json()
            log.success(f"Drive sync complete: {filename}")
            return {
                "status": "synced",
                "file_id": result.get("id", existing_id or ""),
                "web_link": result.get("webViewLink", ""),
                "filename": filename,
            }
        else:
            log.error(f"Drive upload failed: {resp.status_code} {resp.text[:200]}")
            return {"status": "error", "reason": resp.text[:200]}

    except Exception as e:
        log.error(f"Drive sync failed: {e}")
        return {"status": "error", "reason": str(e)}


def _get_drive_access_token(sa_data: dict) -> str:
    """Get OAuth2 access token from service account credentials via JWT."""
    import time
    import hashlib
    import hmac

    # Build JWT
    now = int(time.time())
    header = base64.urlsafe_b64encode(json.dumps({
        "alg": "RS256", "typ": "JWT"
    }).encode()).decode().rstrip("=")

    claims = base64.urlsafe_b64encode(json.dumps({
        "iss": sa_data["client_email"],
        "scope": "https://www.googleapis.com/auth/drive.file",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }).encode()).decode().rstrip("=")

    # Sign with RSA private key
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    private_key = serialization.load_pem_private_key(
        sa_data["private_key"].encode(), password=None
    )

    signature_input = f"{header}.{claims}".encode()
    signature = private_key.sign(signature_input, padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = base64.urlsafe_b64encode(signature).decode().rstrip("=")

    jwt_token = f"{header}.{claims}.{sig_b64}"

    # Exchange JWT for access token
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_token,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# -----------------------------------------------------------
# Main Orchestrator
# -----------------------------------------------------------

@retry_with_backoff("04_storage")
def run_storage(video_id: str, slug: str, files: dict, payload: dict) -> dict:
    """
    Full Phase 4: commit to GitHub, sync to Drive, emit next phase.
    """
    log.start(f"Storage for {slug}")

    # 1. GitHub commit
    github_result = store_to_github(slug, files, payload)

    # 2. Drive sync
    drive_result = sync_to_drive(slug, files)

    # 3. Enrich payload
    payload["github_files"] = github_result
    payload["drive_sync"] = drive_result

    # 4. Notify
    notify(
        event="Extraction stored",
        phase="04_storage",
        status="Complete",
        video_title=payload.get('title', slug),
        slug=slug,
        video_url=payload.get('url', ''),
        asset_count=len(github_result),
        details=f"GitHub: {len(github_result)} files committed\nDrive: {drive_result.get('status', 'unknown')}",
    )

    log.success(f"Storage complete: {len(github_result)} files on GitHub, Drive={drive_result.get('status')}")

    # 5. Emit to Phase 5
    event = emit_next_phase(
        current_phase=Phase.STORAGE,
        payload=payload,
        video_id=video_id,
        slug=slug,
    )

    return {
        "github": github_result,
        "drive": drive_result,
        "event": event,
    }


if __name__ == "__main__":
    import sys
    print("Phase 4 — run via pipeline, not standalone")


# A Brand Collab Production. All rights reserved 2026.
