"""
google_drive_upload.py
Uploads adviser report PDFs to Google Drive using a service account.

Environment variables required:
    GOOGLE_SERVICE_ACCOUNT_JSON   — Full JSON contents of the service account key
    GOOGLE_DRIVE_FOLDER_ID        — Target folder ID in Google Drive

Usage:
    from google_drive_upload import upload_report, upload_all_reports
"""

import os
import json
import io
import tempfile
from datetime import datetime


def _get_credentials():
    """Load Google service account credentials from environment."""
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set")
    try:
        return json.loads(sa_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON: {e}")


def _get_access_token(credentials):
    """Get OAuth2 access token from service account credentials."""
    import time
    import base64
    import hmac
    import hashlib
    import requests

    # Build JWT
    now = int(time.time())
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "RS256", "typ": "JWT"}).encode()
    ).rstrip(b"=").decode()

    payload = base64.urlsafe_b64encode(json.dumps({
        "iss": credentials["client_email"],
        "scope": "https://www.googleapis.com/auth/drive",
        "aud": "https://oauth2.googleapis.com/token",
        "exp": now + 3600,
        "iat": now,
    }).encode()).rstrip(b"=").decode()

    # Sign with private key
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend

    private_key = serialization.load_pem_private_key(
        credentials["private_key"].encode(),
        password=None,
        backend=default_backend()
    )

    signing_input = f"{header}.{payload}".encode()
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()

    jwt = f"{header}.{payload}.{sig_b64}"

    # Exchange JWT for access token
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt,
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def upload_report(pdf_path, adviser_name, month, year, folder_id=None):
    """
    Upload a single PDF report to Google Drive.

    Args:
        pdf_path: Local path to the PDF file
        adviser_name: Adviser's full name (used in filename)
        month: Report month (int)
        year: Report year (int)
        folder_id: Google Drive folder ID (defaults to env var)

    Returns:
        dict with 'file_id', 'file_name', 'web_link'
    """
    import requests

    folder_id = folder_id or os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        raise ValueError("GOOGLE_DRIVE_FOLDER_ID environment variable not set")

    credentials = _get_credentials()
    access_token = _get_access_token(credentials)

    # Build filename: "February 2026 - John Rojas.pdf"
    month_name = datetime(year, month, 1).strftime("%B")
    file_name = f"{month_name} {year} - {adviser_name}.pdf"

    # Check if file already exists (to update instead of duplicate)
    headers = {"Authorization": f"Bearer {access_token}"}
    search_resp = requests.get(
        "https://www.googleapis.com/drive/v3/files",
        headers=headers,
        params={
            "q": f"name='{file_name}' and '{folder_id}' in parents and trashed=false",
            "fields": "files(id, name)",
        }
    )
    search_resp.raise_for_status()
    existing = search_resp.json().get("files", [])

    with open(pdf_path, "rb") as f:
        pdf_content = f.read()

    if existing:
        # Update existing file
        file_id = existing[0]["id"]
        resp = requests.patch(
            f"https://www.googleapis.com/upload/drive/v3/files/{file_id}",
            headers={**headers, "Content-Type": "application/pdf"},
            params={"uploadType": "media"},
            data=pdf_content,
        )
        action = "updated"
    else:
        # Create new file
        metadata = json.dumps({
            "name": file_name,
            "parents": [folder_id],
        })
        resp = requests.post(
            "https://www.googleapis.com/upload/drive/v3/files",
            headers=headers,
            params={"uploadType": "multipart", "fields": "id,name,webViewLink"},
            files=[
                ("metadata", (None, metadata, "application/json")),
                ("file", (file_name, pdf_content, "application/pdf")),
            ],
        )
        action = "created"

    resp.raise_for_status()
    result = resp.json()
    file_id = result.get("id", existing[0]["id"] if existing else "unknown")

    # Get web link
    link_resp = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{file_id}",
        headers=headers,
        params={"fields": "webViewLink"},
    )
    web_link = link_resp.json().get("webViewLink", "") if link_resp.ok else ""

    print(f"  ☁️  {action.capitalize()}: {file_name} → Google Drive")

    return {
        "file_id": file_id,
        "file_name": file_name,
        "web_link": web_link,
        "action": action,
    }


def upload_all_reports(results, month, year):
    """
    Upload all successfully generated reports to Google Drive.

    Args:
        results: List of result dicts from run_pipeline.run_single()
        month: Report month
        year: Report year

    Returns:
        List of upload results
    """
    successes = [r for r in results if r.get("success") and r.get("pdf_path")]
    print(f"\n☁️  Uploading {len(successes)} reports to Google Drive...")

    upload_results = []
    for r in successes:
        try:
            result = upload_report(
                pdf_path=r["pdf_path"],
                adviser_name=r["name"],
                month=month,
                year=year,
            )
            upload_results.append({"adviser": r["name"], "success": True, **result})
        except Exception as e:
            print(f"  ❌ Upload failed for {r['name']}: {e}")
            upload_results.append({"adviser": r["name"], "success": False, "error": str(e)})

    uploaded = [u for u in upload_results if u["success"]]
    failed = [u for u in upload_results if not u["success"]]
    print(f"  ✅ Uploaded: {len(uploaded)}/{len(successes)}")
    if failed:
        print(f"  ❌ Failed: {[u['adviser'] for u in failed]}")

    return upload_results
