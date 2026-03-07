"""
slack_server.py
Flask server that receives Slack slash commands and triggers report generation.

Slash command usage:
    /report john_rojas               → generates last month's report for John Rojas
    /report john_rojas feb 2026      → generates specific month
    /report all                      → generates all advisers (last month)
    /report list                     → lists all active advisers

Environment variables:
    SLACK_SIGNING_SECRET   — from Slack app settings (to verify requests)
    DB_USER, DB_PASS etc   — same as pipeline
    GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_DRIVE_FOLDER_ID
"""

import os
import sys
import hmac
import hashlib
import time
import threading
import json
from datetime import datetime, timedelta
from flask import Flask, request, jsonify

sys.path.insert(0, os.path.dirname(__file__))

app = Flask(__name__)


# ═══════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════

def verify_slack_request(req):
    signing_secret = os.getenv("SLACK_SIGNING_SECRET", "")
    if not signing_secret:
        return True
    slack_signature = req.headers.get("X-Slack-Signature", "")
    slack_timestamp = req.headers.get("X-Slack-Request-Timestamp", "")
    if abs(time.time() - int(slack_timestamp)) > 300:
        return False
    sig_basestring = f"v0:{slack_timestamp}:{req.get_data(as_text=True)}"
    my_signature = "v0=" + hmac.new(
        signing_secret.encode(), sig_basestring.encode(), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(my_signature, slack_signature)


def get_previous_month():
    today = datetime.now()
    first = today.replace(day=1)
    last = first - timedelta(days=1)
    return last.month, last.year


def parse_month_year(tokens):
    month_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'june': 6, 'july': 7, 'august': 8, 'september': 9,
        'october': 10, 'november': 11, 'december': 12,
    }
    month, year = get_previous_month()
    for token in tokens:
        if token.lower() in month_map:
            month = month_map[token.lower()]
        elif token.isdigit() and len(token) == 4:
            year = int(token)
    return month, year


def post_to_slack(response_url, text):
    import requests as req_lib
    try:
        req_lib.post(response_url, json={"text": text, "response_type": "in_channel"})
    except Exception as e:
        print(f"Failed to post to Slack: {e}")


# ═══════════════════════════════════════════════════
#  Background workers
# ═══════════════════════════════════════════════════

def bg_list(response_url):
    from build_config import get_connection, query
    try:
        conn = get_connection()
        rows = query(conn, """
            SELECT DISTINCT u.first_name, u.last_name, u.id
            FROM auth_user u
            JOIN account_usergroup_users ugu ON ugu.user_id = u.id
            JOIN account_usergroup ug ON ug.id = ugu.usergroup_id
                AND ug.real = 1 AND ug.is_active = 1
            WHERE u.is_active = 1 AND u.id NOT IN (88, 118, 172)
            ORDER BY u.last_name
        """)
        conn.close()
        names = "\n".join([f"• {r['first_name']} {r['last_name']} — `/report {r['first_name'].lower()}_{r['last_name'].lower()}`" for r in rows])
        post_to_slack(response_url, f"👥 *Active Advisers ({len(rows)}):*\n{names}")
    except Exception as e:
        post_to_slack(response_url, f"❌ Failed to fetch adviser list: {e}")


def bg_single(user_id, adviser_name, month, year, response_url):
    from run_pipeline import run_single
    from build_config import get_connection
    try:
        conn = get_connection()
        api_key = os.getenv("ANTHROPIC_API_KEY")
        result = run_single(user_id, month, year, conn=conn, api_key=api_key)
        conn.close()
        month_name = datetime(year, month, 1).strftime("%B")
        drive_link = ""
        if result.get("drive_result") and result["drive_result"].get("web_link"):
            drive_link = f"\n📁 <{result['drive_result']['web_link']}|View in Google Drive>"
        post_to_slack(response_url, f"✅ *{month_name} {year} — {adviser_name}* report ready! ({result['size_kb']:.0f} KB){drive_link}")
    except Exception as e:
        post_to_slack(response_url, f"❌ Failed to generate report for *{adviser_name}*: {e}")


def bg_find_and_run(name_slug, month, year, response_url):
    from build_config import get_connection, query
    try:
        conn = get_connection()
        name_parts = name_slug.replace("_", " ").replace("-", " ").strip()
        rows = query(conn, """
            SELECT DISTINCT u.id, u.first_name, u.last_name
            FROM auth_user u
            JOIN account_usergroup_users ugu ON ugu.user_id = u.id
            JOIN account_usergroup ug ON ug.id = ugu.usergroup_id
                AND ug.real = 1 AND ug.is_active = 1
            WHERE u.is_active = 1
              AND CONCAT(u.first_name, ' ', u.last_name) LIKE %s
            LIMIT 5
        """, (f"%{name_parts}%",))
        conn.close()

        if not rows:
            post_to_slack(response_url, f"❌ No adviser found matching *{name_slug}*. Try `/report list`.")
            return

        if len(rows) > 1:
            names = "\n".join([f"• {r['first_name']} {r['last_name']} — `/report {r['first_name'].lower()}_{r['last_name'].lower()}`" for r in rows])
            post_to_slack(response_url, f"⚠️ Multiple matches for *{name_slug}*:\n{names}\nPlease be more specific.")
            return

        adviser = rows[0]
        adviser_name = f"{adviser['first_name']} {adviser['last_name']}"
        month_name = datetime(year, month, 1).strftime("%B")
        post_to_slack(response_url, f"🚀 Generating *{month_name} {year}* report for *{adviser_name}*...")
        bg_single(adviser['id'], adviser_name, month, year, response_url)

    except Exception as e:
        post_to_slack(response_url, f"❌ Error: {e}")


def bg_all(month, year, response_url):
    from run_pipeline import run_all
    try:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        results = run_all(month, year, api_key=api_key)
        successes = [r for r in results if r.get("success")]
        failures = [r for r in results if not r.get("success")]
        month_name = datetime(year, month, 1).strftime("%B")
        msg = (
            f"✅ *{month_name} {year}* — All reports complete!\n"
            f"• Generated: {len(successes)}/{len(results)}\n"
            f"• Failed: {len(failures)}"
            + (f"\n• Failed: {', '.join(str(r['user_id']) for r in failures)}" if failures else "")
            + "\n📁 Check Google Drive for all reports"
        )
        post_to_slack(response_url, msg)
    except Exception as e:
        post_to_slack(response_url, f"❌ Failed to run all reports: {e}")


# ═══════════════════════════════════════════════════
#  Slack slash command: /report
# ═══════════════════════════════════════════════════

@app.route("/slack/report", methods=["POST"])
def slack_report():
    if not verify_slack_request(request):
        return jsonify({"error": "Invalid signature"}), 403

    text = request.form.get("text", "").strip().lower()
    response_url = request.form.get("response_url", "")
    tokens = text.split()

    # No args → show help immediately
    if not tokens:
        return jsonify({
            "response_type": "ephemeral",
            "text": (
                "📊 *Report Bot Usage:*\n"
                "• `/report john_rojas` — last month's report\n"
                "• `/report john_rojas feb 2026` — specific month\n"
                "• `/report all` — all advisers\n"
                "• `/report list` — list all active advisers"
            )
        })

    command = tokens[0]

    # Always respond to Slack within 3s, do work in background
    if command == "list":
        threading.Thread(target=bg_list, args=(response_url,), daemon=True).start()
        return jsonify({"response_type": "ephemeral", "text": "⏳ Fetching adviser list..."})

    if command == "all":
        month, year = parse_month_year(tokens[1:])
        month_name = datetime(year, month, 1).strftime("%B")
        threading.Thread(target=bg_all, args=(month, year, response_url), daemon=True).start()
        return jsonify({"response_type": "in_channel", "text": f"🚀 Generating *all {month_name} {year}* reports... I'll post here when done!"})

    # Single adviser
    month, year = parse_month_year(tokens[1:])
    threading.Thread(target=bg_find_and_run, args=(command, month, year, response_url), daemon=True).start()
    return jsonify({"response_type": "in_channel", "text": f"⏳ Looking up *{command.replace('_', ' ').title()}*..."})


# ═══════════════════════════════════════════════════
#  Health check
# ═══════════════════════════════════════════════════

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "adviser-report-bot"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    print(f"🤖 Report bot listening on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
