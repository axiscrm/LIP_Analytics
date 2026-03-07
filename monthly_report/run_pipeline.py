"""
run_pipeline.py
Main orchestrator for monthly adviser report generation.
Runs on Railway cron (1st of each month) or manually.

Pipeline:
  1. Query DB → build data config for each adviser
  2. Call Claude API → generate personalised narratives
  3. Render PDF → 12-page adviser report
  4. Upload → Google Drive

Usage:
    # Single adviser (testing)
    python run_pipeline.py --user_id 80 --month 2 --year 2026

    # All advisers (production)
    python run_pipeline.py --all --month 2 --year 2026

    # Auto-detect previous month (cron mode)
    python run_pipeline.py --all

Environment variables:
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME    — MySQL connection
    ANTHROPIC_API_KEY                          — Claude API for narratives
    GOOGLE_SERVICE_ACCOUNT_JSON               — Google service account key (JSON string)
    GOOGLE_DRIVE_FOLDER_ID                    — Target Google Drive folder ID
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

# Load .env from project root (parent of monthly_report/) for API keys
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    load_dotenv(os.path.abspath(_env_path), override=True)
except ImportError:
    pass

from build_config import build_all, build_all_practice, write_config, get_connection, query, get_all_practices


def get_all_adviser_ids(conn):
    """Get all active adviser user IDs that should receive reports."""
    rows = query(conn, """
        SELECT DISTINCT ugu.user_id
        FROM account_usergroup_users ugu
        JOIN account_usergroup ug ON ug.id = ugu.usergroup_id
            AND ug.real = 1 AND ug.is_active = 1
        JOIN auth_user au ON au.id = ugu.user_id AND au.is_active = 1
        WHERE ugu.user_id NOT IN (88, 118, 172)
        ORDER BY ugu.user_id
    """)
    return [r["user_id"] for r in rows]


def get_previous_month():
    """Return (month, year) for the previous month."""
    today = datetime.now()
    first_of_month = today.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    return last_month.month, last_month.year


def run_single(user_id, month, year, output_dir="output", conn=None, api_key=None):
    """Generate report for a single adviser."""
    t0 = time.time()

    # Step 1: Build data config
    print(f"\n{'═' * 60}")
    print(f"  Adviser {user_id} — {month}/{year}")
    print(f"{'═' * 60}")

    config = build_all(user_id, month, year, conn=conn, api_key=api_key)

    adviser_name = config.get("ADVISER_NAME", f"user_{user_id}")
    safe_name = adviser_name.lower().replace(" ", "_")
    month_name = config.get("REPORT_MONTH_NAME", str(month)).lower()

    # Step 2: Write config to report_config.py (section modules import from it)
    project_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(project_dir, "report_config.py")
    write_config(config, config_path)

    # Also save a copy in output dir for reference
    os.makedirs(output_dir, exist_ok=True)
    archive_config = os.path.join(output_dir, f"config_{safe_name}_{month_name}{year}.py")
    write_config(config, archive_config)

    # Step 3: Reload report_config module so section files pick up new data
    if "report_config" in sys.modules:
        del sys.modules["report_config"]

    # Step 4: Render PDF — add short timestamp suffix to avoid overwriting previous runs
    _ts = time.strftime("%Y%m%d%H%M%S")
    pdf_path = os.path.join(output_dir, f"adviser_report_{safe_name}_{month_name}{year}_{_ts}.pdf")

    from generate_report import build_report
    build_report(user_id, month, year, pdf_path)

    elapsed = time.time() - t0
    size_kb = os.path.getsize(pdf_path) / 1024 if os.path.exists(pdf_path) else 0

    print(f"  ✅ {adviser_name}: {pdf_path} ({size_kb:.0f} KB, {elapsed:.1f}s)")

    # Upload to Google Drive if credentials are available
    drive_result = None
    if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") and os.getenv("GOOGLE_DRIVE_FOLDER_ID"):
        try:
            from google_drive_upload import upload_report
            drive_result = upload_report(pdf_path, adviser_name, month, year)
        except Exception as e:
            print(f"  ⚠️  Google Drive upload failed for {adviser_name}: {e}")

    # Upload to SharePoint/OneDrive if credentials are available
    sp_result = None
    sp_secret = os.getenv("SHAREPOINT_CLIENT_SECRET", "")
    if os.getenv("SHAREPOINT_TENANT_ID") and os.getenv("SHAREPOINT_CLIENT_ID") and sp_secret and sp_secret != "PASTE_SECRET_VALUE_HERE":
        try:
            from sharepoint_upload import upload_report as sp_upload
            sp_result = sp_upload(pdf_path, adviser_name, month, year)
        except Exception as e:
            print(f"  ⚠️  SharePoint upload failed for {adviser_name}: {e}")

    return {
        "user_id": user_id,
        "name": adviser_name,
        "pdf_path": pdf_path,
        "config_path": archive_config,
        "size_kb": size_kb,
        "elapsed": elapsed,
        "success": True,
        "drive_result": drive_result,
        "sp_result": sp_result,
    }


def run_practice(practice_name, month, year, output_dir="output", api_key=None):
    """Generate a practice-level aggregate report."""
    t0 = time.time()
    print(f"\n{'═' * 60}")
    print(f"  Practice: {practice_name} — {month}/{year}")
    print(f"{'═' * 60}")

    config = build_all_practice(practice_name, month, year, api_key=api_key)

    safe_name  = practice_name.lower().replace(" ", "_").replace("/", "_")
    month_name = config.get("REPORT_MONTH_NAME", str(month)).lower()

    project_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(project_dir, "report_config.py")
    write_config(config, config_path)

    os.makedirs(output_dir, exist_ok=True)
    archive_config = os.path.join(output_dir, f"config_{safe_name}_{month_name}{year}.py")
    write_config(config, archive_config)

    if "report_config" in sys.modules:
        del sys.modules["report_config"]
    for mod_name in list(sys.modules.keys()):
        if mod_name.startswith("test_section"):
            del sys.modules[mod_name]

    _ts = time.strftime("%Y%m%d%H%M%S")
    pdf_path = os.path.join(output_dir, f"practice_report_{safe_name}_{month_name}{year}_{_ts}.pdf")
    from generate_report import build_report
    build_report(None, month, year, pdf_path)

    elapsed = time.time() - t0
    size_kb = os.path.getsize(pdf_path) / 1024 if os.path.exists(pdf_path) else 0
    print(f"  ✅ {practice_name}: {pdf_path} ({size_kb:.0f} KB, {elapsed:.1f}s)")

    sp_secret = os.getenv("SHAREPOINT_CLIENT_SECRET", "")
    if os.getenv("SHAREPOINT_TENANT_ID") and os.getenv("SHAREPOINT_CLIENT_ID") and sp_secret and sp_secret != "PASTE_SECRET_VALUE_HERE":
        try:
            from sharepoint_upload import upload_report as sp_upload
            sp_upload(pdf_path, practice_name, month, year)
        except Exception as e:
            print(f"  ⚠️  SharePoint upload failed for {practice_name}: {e}")

    return {"practice": practice_name, "pdf_path": pdf_path, "size_kb": size_kb, "elapsed": elapsed, "success": True}


def run_all(month, year, output_dir="output", api_key=None):
    """Generate reports for all active advisers."""
    conn = get_connection()
    try:
        adviser_ids = get_all_adviser_ids(conn)
        print(f"\n🚀 Generating reports for {len(adviser_ids)} advisers ({month}/{year})")
        print(f"   Output: {output_dir}/")

        os.makedirs(output_dir, exist_ok=True)

        results = []
        for i, uid in enumerate(adviser_ids, 1):
            try:
                # Clear cached section modules so they re-import fresh config
                for mod_name in list(sys.modules.keys()):
                    if mod_name.startswith("test_section") or mod_name == "report_config":
                        del sys.modules[mod_name]

                print(f"\n[{i}/{len(adviser_ids)}]", end="")
                result = run_single(uid, month, year, output_dir, conn=conn, api_key=api_key)
                results.append(result)
            except Exception as e:
                print(f"  ❌ User {uid}: {e}")
                results.append({
                    "user_id": uid, "success": False, "error": str(e)
                })

        # Summary
        successes = [r for r in results if r.get("success")]
        failures = [r for r in results if not r.get("success")]
        total_time = sum(r.get("elapsed", 0) for r in successes)

        print(f"\n{'═' * 60}")
        print(f"  COMPLETE: {len(successes)}/{len(results)} reports generated")
        print(f"  Failed: {len(failures)}")
        print(f"  Total time: {total_time:.0f}s ({total_time/60:.1f}min)")
        if failures:
            print(f"  Failed users: {[r['user_id'] for r in failures]}")
        print(f"{'═' * 60}\n")

        return results

    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate monthly adviser reports")
    parser.add_argument("--user_id", type=int, help="Single adviser user ID")
    parser.add_argument("--practice", type=str, help="Practice name for aggregate report")
    parser.add_argument("--list_practices", action="store_true", help="List all practices with 2+ advisers")
    parser.add_argument("--all", action="store_true", help="Generate for all advisers")
    parser.add_argument("--month", type=int, help="Report month (1-12)")
    parser.add_argument("--year", type=int, help="Report year")
    parser.add_argument("--output_dir", default="output", help="Output directory")
    parser.add_argument("--api_key", default=None, help="Anthropic API key")
    args = parser.parse_args()

    # Determine month/year
    if args.month and args.year:
        month, year = args.month, args.year
    else:
        month, year = get_previous_month()
        print(f"Auto-detected report period: {month}/{year}")

    api_key = args.api_key or os.getenv("ANTHROPIC_API_KEY")

    if args.list_practices:
        conn = get_connection()
        try:
            practices = get_all_practices(conn)
            print(f"\nPractices with 2+ advisers ({len(practices)} total):")
            for name, ids in practices:
                print(f"  {name!r:40s} — user_ids: {ids}")
        finally:
            conn.close()
        sys.exit(0)

    os.makedirs(args.output_dir, exist_ok=True)

    if args.all:
        run_all(month, year, args.output_dir, api_key=api_key)
    elif args.practice:
        run_practice(args.practice, month, year, args.output_dir, api_key=api_key)
    elif args.user_id:
        run_single(args.user_id, month, year, args.output_dir, api_key=api_key)
    else:
        parser.error("Specify --user_id <ID>, --practice <name>, --list_practices, or --all")
