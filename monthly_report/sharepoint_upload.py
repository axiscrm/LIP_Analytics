"""
sharepoint_upload.py
Uploads adviser report PDFs to OneDrive for Business via Microsoft Graph API.

Authentication: Azure AD app-only (client credentials flow).
The app must have Files.ReadWrite.All or Sites.ReadWrite.All application permission
granted in Azure portal (Admin consent required).

Environment variables:
    SHAREPOINT_TENANT_ID      — Azure AD tenant ID
    SHAREPOINT_CLIENT_ID      — App (client) ID
    SHAREPOINT_CLIENT_SECRET  — Client secret value
    SHAREPOINT_USER           — OneDrive owner UPN (e.g. james@axiscrm.com.au)
    SHAREPOINT_FOLDER         — Folder path in OneDrive (e.g. Monthly Performance Report/Testing)
"""

import os
import requests
from datetime import datetime


def _get_access_token():
    """Obtain an app-only access token from Azure AD."""
    tenant_id     = os.getenv("SHAREPOINT_TENANT_ID")
    client_id     = os.getenv("SHAREPOINT_CLIENT_ID")
    client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        raise ValueError("SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID and SHAREPOINT_CLIENT_SECRET must all be set")
    if client_secret == "PASTE_SECRET_VALUE_HERE":
        raise ValueError("SHAREPOINT_CLIENT_SECRET is still a placeholder — paste the actual secret value from Azure portal")

    resp = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         "https://graph.microsoft.com/.default",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def upload_report(pdf_path, adviser_name, month, year, folder=None, user=None):
    """
    Upload a single PDF to OneDrive for Business via Graph API.

    Args:
        pdf_path:     Local path to the PDF file
        adviser_name: Used to build the remote filename
        month:        Report month (int)
        year:         Report year (int)
        folder:       Remote folder path (defaults to SHAREPOINT_FOLDER env var)
        user:         OneDrive UPN (defaults to SHAREPOINT_USER env var)

    Returns:
        dict with 'file_name', 'web_url', 'action'
    """
    user   = user   or os.getenv("SHAREPOINT_USER",  "")
    folder = folder or os.getenv("SHAREPOINT_FOLDER", "")

    if not user:
        raise ValueError("SHAREPOINT_USER environment variable not set")

    month_name = datetime(year, month, 1).strftime("%B")
    file_name  = f"{month_name} {year} - {adviser_name}.pdf"

    # Graph API upload path: /users/{upn}/drive/root:/{folder}/{file}:/content
    folder_clean = folder.strip("/")
    upload_url = (
        f"https://graph.microsoft.com/v1.0"
        f"/users/{user}/drive/root:/{folder_clean}/{file_name}:/content"
    )

    token = _get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/pdf",
    }

    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    # PUT replaces if file exists, creates if not
    resp = requests.put(upload_url, headers=headers, data=pdf_bytes, timeout=60)
    resp.raise_for_status()

    result   = resp.json()
    web_url  = result.get("webUrl", "")
    action   = "updated" if resp.status_code == 200 else "created"

    print(f"  ☁️  SharePoint {action}: {file_name}")
    if web_url:
        print(f"      {web_url}")

    return {
        "file_name": file_name,
        "web_url":   web_url,
        "action":    action,
    }


def upload_all_reports(results, month, year):
    """
    Upload all successfully generated reports to SharePoint/OneDrive.

    Args:
        results: List of result dicts from run_pipeline.run_single()
        month:   Report month (int)
        year:    Report year (int)

    Returns:
        List of upload result dicts
    """
    successes = [r for r in results if r.get("success") and r.get("pdf_path")]
    if not successes:
        print("\n☁️  No reports to upload to SharePoint.")
        return []

    print(f"\n☁️  Uploading {len(successes)} report(s) to SharePoint...")

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
    failed   = [u for u in upload_results if not u["success"]]
    print(f"  ✅ Uploaded: {len(uploaded)}/{len(successes)}")
    if failed:
        print(f"  ❌ Failed:   {[u['adviser'] for u in failed]}")

    return upload_results
