import os
import json
import time
import logging
from datetime import date, datetime, timedelta
from flask import Flask, render_template, request, Response, stream_with_context, session, redirect, url_for
from dotenv import load_dotenv
from db import get_connection
from collections import defaultdict

load_dotenv()

log = logging.getLogger("lip_analytics.app")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-in-production")
GROUP_ID = int(os.environ.get("LIP_GROUP_ID", 56))
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")

# Stamp changes every time the password is updated and the app restarts,
# invalidating all sessions created with a previous password.
_password_stamp = hash(DASHBOARD_PASSWORD) if DASHBOARD_PASSWORD else None

def login_required(f):
    """Decorator — redirects to /login if not authenticated."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not DASHBOARD_PASSWORD:
            return f(*args, **kwargs)
        if session.get("authenticated") and session.get("pw_stamp") == _password_stamp:
            return f(*args, **kwargs)
        session.clear()
        return redirect(url_for("login", next=request.url))
    return decorated

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw and pw == DASHBOARD_PASSWORD:
            session["authenticated"] = True
            session["pw_stamp"] = _password_stamp
            return redirect(request.args.get("next") or url_for("index"))
        error = "Incorrect password."
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ── Persistent settings (targets & thresholds) ───────────────────────────────
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "settings.json")

def load_settings():
    try:
        with open(SETTINGS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_settings(data):
    with open(SETTINGS_FILE, "w") as f:
        json.dump(data, f)

@app.route("/api/settings", methods=["GET"])
@login_required
def get_settings():
    from flask import jsonify
    return jsonify(load_settings())

@app.route("/api/settings", methods=["POST"])
@login_required
def post_settings():
    from flask import jsonify
    data = request.get_json(force=True) or {}
    save_settings(data)
    return jsonify({"ok": True})

SHOW_USER_IDS = {181, 182, 183, 152, 53}
MIN_DATE = "2025-01-01"
TZ_OFFSET = "+11:00"   # AEDT — single place to change if needed
CONTACT_THRESHOLD_US = 45_000_000  # 45 seconds in microseconds
_USER_IDS_SQL = ",".join(str(u) for u in sorted(SHOW_USER_IDS))

CRM_BASE_URL = "https://crm.slife.com.au"
REMED_TYPE_IDS_SQL = "138,139,140,141,142,143,144,145,162,163,164,165,189,197"

AVATAR_COLORS = {181:"#6366f1",182:"#ec4899",152:"#f59e0b",183:"#10b981",53:"#3b82f6"}
AVATAR_FILES  = {181:"Nataniel.jpeg",182:"Sam.jpeg",152:"Rebel.jpeg",183:"Gary.jpeg",53:""}

# ── Helpers ──────────────────────────────────────────────────────────────────

def _utc_range(start, end):
    """Convert local date range to UTC datetime range for index-friendly WHERE clauses.

    Instead of  DATE(CONVERT_TZ(col,'+00:00','+11:00')) BETWEEN start AND end
    which wraps the column in functions and prevents index usage, we convert the
    boundaries once:  col >= utc_start AND col < utc_end_exclusive
    """
    utc_start = f"CONVERT_TZ('{start.isoformat()}','{TZ_OFFSET}','+00:00')"
    # end+1 day exclusive so we cover the full last day
    end_excl  = (end + timedelta(days=1)).isoformat()
    utc_end   = f"CONVERT_TZ('{end_excl}','{TZ_OFFSET}','+00:00')"
    return utc_start, utc_end

def _timed(label, fn, *args, **kwargs):
    """Run fn, log elapsed time, return result."""
    t0 = time.monotonic()
    result = fn(*args, **kwargs)
    elapsed = (time.monotonic() - t0) * 1000
    log.info("[%s] %.0f ms", label, elapsed)
    return result

def fmt_hms(seconds):
    s=int(seconds or 0); h,rem=divmod(s,3600); m,sc=divmod(rem,60)
    return f"{h}:{m:02d}:{sc:02d}"

def biz_days_in_range(start, end):
    return sum(1 for i in range((end-start).days+1)
               if (start+timedelta(days=i)).weekday()<5)

def last_biz_day(ref=None):
    d=(ref or date.today())-timedelta(days=1)
    while d.weekday()>=5: d-=timedelta(days=1)
    return max(d, date.fromisoformat(MIN_DATE))

def color_talk(secs):
    if secs>=9000: return "green"
    if secs>=7200: return "orange"
    return "red"

def color_quotes(qpd):
    if qpd>=6: return "green"
    if qpd>=4: return "orange"
    return "red"

def color_apps(apd):
    if apd>=2: return "green"
    if apd>=1: return "orange"
    return "red"

def color_inforce(monthly):
    if monthly>=20000: return "green"
    if monthly>=15000: return "orange"
    return "red"

def get_advisers(cursor):
    cursor.execute("""
        SELECT u.id, CONCAT(u.first_name,' ',u.last_name) AS name,
               u.first_name, u.last_name
        FROM auth_user u
        JOIN account_usergroup_users ugu ON u.id=ugu.user_id
        WHERE ugu.usergroup_id=%s ORDER BY u.last_name, u.first_name
    """, (GROUP_ID,))
    return [{"id":r["id"],"name":r["name"].strip(),
             "first_name":r["first_name"],"last_name":r["last_name"]}
            for r in cursor.fetchall() if r["id"] in SHOW_USER_IDS]

def get_performance_stats(cursor, start, end):
    utc_start, utc_end = _utc_range(start, end)

    # Talk time from noojee_callrecord via extension → user_id
    cursor.execute(f"""
        SELECT up.user_id,
               COALESCE(SUM(ncr.duration),0)/1000000 AS talk_secs
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.status = 'Hungup'
          AND ncr.duration > 10000000
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY up.user_id
    """)
    rows = {r["user_id"]: {"talk_secs": float(r["talk_secs"]),
                           "apps_count": 0, "apps_value": 0.0,
                           "inforce_count": 0, "inforce_value": 0.0,
                           "days_worked": 0}
            for r in cursor.fetchall()}

    # Apps, inforce, days worked from reports_userstats
    cursor.execute(f"""
        SELECT user_id,
               SUM(app_add)       AS apps_count,
               SUM(app_add_value) AS apps_value,
               SUM(app_com)       AS inforce_count,
               SUM(app_com_value) AS inforce_value,
               SUM(CASE WHEN (contact>0 OR qut_add>0 OR app_add>0) THEN 1 ELSE 0 END) AS days_worked
        FROM reports_userstats
        WHERE user_id IN ({_USER_IDS_SQL})
          AND date BETWEEN %s AND %s
        GROUP BY user_id
    """, (start.isoformat(), end.isoformat()))
    for r in cursor.fetchall():
        uid = r["user_id"]
        if uid not in rows:
            rows[uid] = {"talk_secs": 0.0}
        rows[uid]["apps_count"]    = int(r["apps_count"] or 0)
        rows[uid]["apps_value"]    = float(r["apps_value"] or 0)
        rows[uid]["inforce_count"] = int(r["inforce_count"] or 0)
        rows[uid]["inforce_value"] = float(r["inforce_value"] or 0)
        rows[uid]["days_worked"]   = int(r["days_worked"] or 0)

    # Quotes from leads_leadquote
    cursor.execute(f"""
        SELECT lq.user_id, COUNT(*) AS quotes_count, COALESCE(SUM(lq.value),0) AS quotes_value
        FROM leads_leadquote lq
        JOIN (
            SELECT user_id, lead_id, MAX(created) AS max_created
            FROM leads_leadquote
            WHERE sent=1 AND deleted=0
              AND created >= {utc_start}
              AND created < {utc_end}
            GROUP BY user_id, lead_id
        ) latest ON lq.user_id=latest.user_id AND lq.lead_id=latest.lead_id
               AND lq.created=latest.max_created
        WHERE lq.sent=1 AND lq.deleted=0 GROUP BY lq.user_id
    """)
    for r in cursor.fetchall():
        uid=r["user_id"]
        if uid in rows:
            rows[uid]["quotes_count"]=int(r["quotes_count"])
            rows[uid]["quotes_value"]=float(r["quotes_value"])
    return rows

def get_pipeline_stats(cursor, start, end):
    """
    Leads funnel logic — all relative to leads ASSIGNED in the period.
    """
    utc_start, utc_end = _utc_range(start, end)

    # 1. Assigned
    cursor.execute("""
        SELECT user_id, COUNT(*) AS assigned FROM leads_lead
        WHERE assigned >= %s AND assigned < %s GROUP BY user_id
    """, (start.isoformat(), (end + timedelta(days=1)).isoformat()))
    assigned = {r["user_id"]: int(r["assigned"]) for r in cursor.fetchall()}

    # 2. Contacted = calls >= 5 seconds duration
    cursor.execute(f"""
        SELECT up.user_id, COUNT(*) AS contacted
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.duration >= {CONTACT_THRESHOLD_US}
          AND ncr.status = 'Hungup'
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY up.user_id
    """)
    contacted = {r["user_id"]: int(r["contacted"] or 0) for r in cursor.fetchall()}

    # 2b. No Contact = calls < 5 seconds duration
    cursor.execute(f"""
        SELECT up.user_id, COUNT(*) AS no_contact
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.duration < {CONTACT_THRESHOLD_US}
          AND ncr.status = 'Hungup'
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY up.user_id
    """)
    no_contact_totals = {r["user_id"]: int(r["no_contact"] or 0) for r in cursor.fetchall()}

    # 3. Booked — of assigned leads, received LIQ doc (anytime on that lead)
    cursor.execute("""
        SELECT l.user_id, COUNT(DISTINCT l.id) AS booked
        FROM leads_lead l
        WHERE l.assigned >= %s AND l.assigned < %s
          AND EXISTS (
            SELECT 1 FROM leads_leadaction la
            WHERE la.object_id=l.id AND la.object_type='lead'
              AND la.action_type='doccreate'
              AND la.note LIKE '%%Life Insurance Questions%%'
          )
        GROUP BY l.user_id
    """, (start.isoformat(), (end + timedelta(days=1)).isoformat()))
    booked = {r["user_id"]: int(r["booked"]) for r in cursor.fetchall()}

    # 4. Called = total calls >= 5s (for daily checks tab)
    called = contacted  # same query result

    return {"assigned":assigned,"contacted":contacted,"no_contact":no_contact_totals,"booked":booked,"called":called}

def get_schedule_appointments(cursor, today_dt):
    """Count today's and future appointments by type (Discussion / Follow-up / Questions)."""
    _empty = lambda: {"disc":0,"fu":0,"q":0}
    appt_today  = defaultdict(_empty)
    appt_future = defaultdict(_empty)
    try:
        utc_today_start = f"CONVERT_TZ('{today_dt.isoformat()}','{TZ_OFFSET}','+00:00')"
        cursor.execute(f"""
            SELECT user_id,
                   DATE(CONVERT_TZ(date,'+00:00','{TZ_OFFSET}')) AS appt_date,
                   CASE
                     WHEN text LIKE '%%Discussion%%' THEN 'disc'
                     WHEN text LIKE '%%Follow%%'     THEN 'fu'
                     WHEN text LIKE '%%Question%%'   THEN 'q'
                     ELSE 'other'
                   END AS appt_type,
                   COUNT(*) AS cnt
            FROM leads_leadschedule
            WHERE date >= {utc_today_start}
              AND user_id IS NOT NULL
            GROUP BY user_id, appt_date, appt_type
        """)
        for r in cursor.fetchall():
            dt = str(r["appt_date"]) if not hasattr(r["appt_date"], 'isoformat') else r["appt_date"].isoformat()
            uid = r["user_id"]
            atype = r["appt_type"]
            if atype == "other":
                continue
            if dt == today_dt.isoformat():
                appt_today[uid][atype] += int(r["cnt"])
            else:
                appt_future[uid][atype] += int(r["cnt"])
    except Exception as e:
        log.warning("[appt] %s", e)
    return dict(appt_today), dict(appt_future)


def get_hourly_series(cursor, day):
    """Hourly performance series for a single day (6am–10pm AEDT)."""
    HOURS = list(range(6, 23))  # 6..22
    day_iso = day.isoformat()
    next_day_iso = (day + timedelta(days=1)).isoformat()
    utc_start = f"CONVERT_TZ('{day_iso}','{TZ_OFFSET}','+00:00')"
    utc_end   = f"CONVERT_TZ('{next_day_iso}','{TZ_OFFSET}','+00:00')"

    # Talk time per hour from noojee_callrecord
    cursor.execute(f"""
        SELECT HOUR(CONVERT_TZ(ncr.created,'+00:00','{TZ_OFFSET}')) AS hr,
               up.user_id,
               SUM(ncr.duration)/1000000 AS talk_secs
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.status = 'Hungup'
          AND ncr.duration > 10000000
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY hr, up.user_id
    """)
    talk_hour = defaultdict(lambda: defaultdict(float))
    for r in cursor.fetchall():
        talk_hour[r["user_id"]][int(r["hr"])] = float(r["talk_secs"] or 0)

    # Quotes per hour from leads_leadquote
    cursor.execute(f"""
        SELECT HOUR(CONVERT_TZ(lq.created,'+00:00','{TZ_OFFSET}')) AS hr,
               lq.user_id, COUNT(*) AS cnt
        FROM leads_leadquote lq
        JOIN (
            SELECT user_id, lead_id, MAX(created) AS max_created
            FROM leads_leadquote
            WHERE sent=1 AND deleted=0
              AND created >= {utc_start}
              AND created < {utc_end}
            GROUP BY user_id, lead_id
        ) latest ON lq.user_id=latest.user_id AND lq.lead_id=latest.lead_id
                AND lq.created=latest.max_created
        WHERE lq.sent=1 AND lq.deleted=0
        GROUP BY hr, lq.user_id
    """)
    quotes_hour = defaultdict(lambda: defaultdict(int))
    for r in cursor.fetchall():
        quotes_hour[r["user_id"]][int(r["hr"])] = int(r["cnt"])

    # Contacted (calls >= 5s) per hour
    cursor.execute(f"""
        SELECT HOUR(CONVERT_TZ(ncr.created,'+00:00','{TZ_OFFSET}')) AS hr,
               up.user_id, COUNT(*) AS cnt
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.duration >= {CONTACT_THRESHOLD_US}
          AND ncr.status = 'Hungup'
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY hr, up.user_id
    """)
    calls_hour = defaultdict(lambda: defaultdict(int))
    for r in cursor.fetchall():
        calls_hour[r["user_id"]][int(r["hr"])] = int(r["cnt"] or 0)

    # Build series per user keyed by hour strings
    hours_list = [str(h) for h in HOURS]
    user_hour = defaultdict(dict)
    for uid in SHOW_USER_IDS:
        for h in HOURS:
            hs = str(h)
            user_hour[uid][hs] = {
                "talk_time_seconds": talk_hour[uid].get(h, 0),
                "leads_quoted": quotes_hour[uid].get(h, 0),
                "apps_count": 0,
                "apps_value": 0,
                "inforce_count": 0,
                "inforce_value": 0,
            }

    calls_day_hourly = defaultdict(dict)
    for uid in SHOW_USER_IDS:
        for h in HOURS:
            calls_day_hourly[uid][str(h)] = calls_hour[uid].get(h, 0)

    return hours_list, dict(user_hour), dict(calls_day_hourly)


def get_hourly_pipeline_series(cursor, day):
    """Hourly pipeline series for a single day (6am–10pm AEDT)."""
    HOURS = list(range(6, 23))
    day_iso = day.isoformat()
    next_day_iso = (day + timedelta(days=1)).isoformat()
    utc_start = f"CONVERT_TZ('{day_iso}','{TZ_OFFSET}','+00:00')"
    utc_end   = f"CONVERT_TZ('{next_day_iso}','{TZ_OFFSET}','+00:00')"

    # Assigned per hour
    cursor.execute("""
        SELECT user_id, HOUR(assigned) AS hr, COUNT(*) AS cnt
        FROM leads_lead
        WHERE assigned >= %s AND assigned < %s
        GROUP BY user_id, HOUR(assigned)
    """, (day_iso, next_day_iso))
    assigned_h = defaultdict(lambda: defaultdict(int))
    for r in cursor.fetchall(): assigned_h[r["user_id"]][int(r["hr"])] = int(r["cnt"])

    # Contacted per hour (calls >= 5s)
    cursor.execute(f"""
        SELECT HOUR(CONVERT_TZ(ncr.created,'+00:00','{TZ_OFFSET}')) AS hr,
               up.user_id, COUNT(*) AS cnt
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.duration >= {CONTACT_THRESHOLD_US}
          AND ncr.status = 'Hungup'
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY hr, up.user_id
    """)
    contacted_h = defaultdict(lambda: defaultdict(int))
    for r in cursor.fetchall(): contacted_h[r["user_id"]][int(r["hr"])] = int(r["cnt"] or 0)

    # No Contact per hour (calls < 5s)
    cursor.execute(f"""
        SELECT HOUR(CONVERT_TZ(ncr.created,'+00:00','{TZ_OFFSET}')) AS hr,
               up.user_id, COUNT(*) AS cnt
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.duration < {CONTACT_THRESHOLD_US}
          AND ncr.status = 'Hungup'
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY hr, up.user_id
    """)
    no_contact_h = defaultdict(lambda: defaultdict(int))
    for r in cursor.fetchall(): no_contact_h[r["user_id"]][int(r["hr"])] = int(r["cnt"] or 0)

    hours_list = [str(h) for h in HOURS]
    assigned_d, contacted_d, no_contact_d, booked_d = {}, {}, {}, {}
    for uid in SHOW_USER_IDS:
        assigned_d[uid]   = {str(h): assigned_h[uid].get(h, 0) for h in HOURS}
        contacted_d[uid]  = {str(h): contacted_h[uid].get(h, 0) for h in HOURS}
        no_contact_d[uid] = {str(h): no_contact_h[uid].get(h, 0) for h in HOURS}
        booked_d[uid]     = {str(h): 0 for h in HOURS}

    return assigned_d, contacted_d, no_contact_d, booked_d


def get_daily_series(cursor, start, end):
    """Performance + call-contact daily series per adviser."""
    utc_start, utc_end = _utc_range(start, end)

    # Daily stats from reports_userstats
    cursor.execute(f"""
        SELECT DATE(date) AS stat_date, user_id,
               contact, qut_add AS leads_quoted,
               app_add AS apps_count, app_add_value AS apps_value,
               app_com AS inforce_count, app_com_value AS inforce_value
        FROM reports_userstats
        WHERE user_id IN ({_USER_IDS_SQL})
          AND date BETWEEN %s AND %s
          AND DAYOFWEEK(date) NOT IN (1,7)
        ORDER BY stat_date, user_id
    """, (start.isoformat(), end.isoformat()))
    rows = cursor.fetchall()
    dates_set = sorted(set(str(r["stat_date"])[:10] for r in rows))
    user_day = defaultdict(dict)
    for r in rows:
        d = str(r["stat_date"])[:10]
        user_day[r["user_id"]][d] = {
            "talk_time_seconds": 0,  # filled below
            "leads_quoted":   int(r["leads_quoted"] or 0),
            "apps_count":     int(r["apps_count"] or 0),
            "apps_value":     float(r["apps_value"] or 0),
            "inforce_count":  int(r["inforce_count"] or 0),
            "inforce_value":  float(r["inforce_value"] or 0),
        }

    # Talk time per day from noojee_callrecord
    cursor.execute(f"""
        SELECT DATE(CONVERT_TZ(ncr.created,'+00:00','{TZ_OFFSET}')) AS dt,
               up.user_id,
               SUM(ncr.duration)/1000000 AS talk_secs
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.status = 'Hungup'
          AND ncr.duration > 10000000
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY dt, up.user_id
    """)
    for r in cursor.fetchall():
        d = str(r["dt"])[:10]
        uid = r["user_id"]
        if d not in user_day[uid]:
            user_day[uid][d] = {"leads_quoted":0,"apps_count":0,"apps_value":0,"inforce_count":0,"inforce_value":0}
            if d not in dates_set:
                dates_set.append(d)
        user_day[uid][d]["talk_time_seconds"] = float(r["talk_secs"] or 0)

    dates_set = sorted(set(dates_set))

    # Daily contacted count — already in reports_userstats.contact, no join needed
    cursor.execute(f"""
        SELECT DATE(date) AS dt, user_id, contact AS cnt
        FROM reports_userstats
        WHERE user_id IN ({_USER_IDS_SQL})
          AND date BETWEEN %s AND %s
          AND DAYOFWEEK(date) NOT IN (1,7)
    """, (start.isoformat(), end.isoformat()))
    calls_day = defaultdict(dict)
    for r in cursor.fetchall():
        calls_day[r["user_id"]][str(r["dt"])[:10]] = int(r["cnt"] or 0)

    return dates_set, dict(user_day), dict(calls_day)

def get_daily_pipeline_series(cursor, start, end, dates_list):
    """Daily funnel series for Leads charts (assigned, contacted, no_contact, booked)."""
    utc_start, utc_end = _utc_range(start, end)

    # Assigned per day
    cursor.execute("""
        SELECT user_id, DATE(assigned) AS dt, COUNT(*) AS cnt
        FROM leads_lead
        WHERE assigned >= %s AND assigned < %s AND DAYOFWEEK(assigned) NOT IN (1,7)
        GROUP BY user_id, DATE(assigned)
    """, (start.isoformat(), (end + timedelta(days=1)).isoformat()))
    assigned_d = defaultdict(lambda: defaultdict(int))
    for r in cursor.fetchall(): assigned_d[r["user_id"]][str(r["dt"])[:10]]=int(r["cnt"])

    # Contacted per day = calls >= 5 seconds duration
    cursor.execute(f"""
        SELECT DATE(CONVERT_TZ(ncr.created,'+00:00','{TZ_OFFSET}')) AS dt,
               up.user_id, COUNT(*) AS cnt
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.duration >= {CONTACT_THRESHOLD_US}
          AND ncr.status = 'Hungup'
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY dt, up.user_id
    """)
    contacted_d = defaultdict(lambda: defaultdict(int))
    for r in cursor.fetchall(): contacted_d[r["user_id"]][str(r["dt"])[:10]] = int(r["cnt"] or 0)

    # No Contact per day = calls < 5 seconds duration
    cursor.execute(f"""
        SELECT DATE(CONVERT_TZ(ncr.created,'+00:00','{TZ_OFFSET}')) AS dt,
               up.user_id, COUNT(*) AS cnt
        FROM noojee_callrecord ncr
        JOIN account_userprofile up ON up.extension = ncr.extension
        WHERE up.user_id IN ({_USER_IDS_SQL})
          AND ncr.duration < {CONTACT_THRESHOLD_US}
          AND ncr.status = 'Hungup'
          AND ncr.created >= {utc_start}
          AND ncr.created < {utc_end}
        GROUP BY dt, up.user_id
    """)
    no_contact_d = defaultdict(lambda: defaultdict(int))
    for r in cursor.fetchall(): no_contact_d[r["user_id"]][str(r["dt"])[:10]] = int(r["cnt"] or 0)

    # Booked per day (leads assigned that day that received LIQ doc)
    cursor.execute("""
        SELECT l.user_id, DATE(l.assigned) AS dt, COUNT(DISTINCT l.id) AS cnt
        FROM leads_lead l
        WHERE l.assigned >= %s AND l.assigned < %s AND DAYOFWEEK(l.assigned) NOT IN (1,7)
          AND EXISTS (
            SELECT 1 FROM leads_leadaction la
            WHERE la.object_id=l.id AND la.object_type='lead'
              AND la.action_type='doccreate'
              AND la.note LIKE '%%Life Insurance Questions%%'
          )
        GROUP BY l.user_id, DATE(l.assigned)
    """, (start.isoformat(), (end + timedelta(days=1)).isoformat()))
    booked_d = defaultdict(lambda: defaultdict(int))
    for r in cursor.fetchall(): booked_d[r["user_id"]][str(r["dt"])[:10]]=int(r["cnt"])

    return dict(assigned_d), dict(contacted_d), dict(no_contact_d), dict(booked_d)


def get_remediation_stats(cursor, start, end):
    """Remediation aggregate counts per adviser + pending detail for popup."""
    utc_start, utc_end = _utc_range(start, end)

    # 1. Aggregate counts: Total (any status) and Pending (status 0/1) in date range
    cursor.execute(f"""
        SELECT l.user_id,
               COUNT(*)                                       AS total_remed,
               SUM(CASE WHEN lr.status IN (0,1) THEN 1 ELSE 0 END) AS pending_remed
        FROM leads_leadrequirement lr
        JOIN leads_lead l ON l.id = lr.lead_id
        WHERE lr.type_id IN ({REMED_TYPE_IDS_SQL})
          AND l.user_id IN ({_USER_IDS_SQL})
          AND lr.created >= {utc_start}
          AND lr.created <  {utc_end}
        GROUP BY l.user_id
    """)
    counts = {}
    for r in cursor.fetchall():
        counts[r["user_id"]] = {
            "total": int(r["total_remed"]),
            "pending": int(r["pending_remed"] or 0),
        }

    # 2. Detailed records — all statuses (pending + resolved) for slide-in panel
    cursor.execute(f"""
        SELECT lr.id            AS req_id,
               lr.lead_id,
               lr.object_type,
               lr.object_id,
               lr.name          AS task_name,
               lr.description,
               lr.last_note,
               lr.status,
               DATE(CONVERT_TZ(lr.created,'+00:00','{TZ_OFFSET}')) AS created_date,
               l.user_id        AS adviser_id,
               CONCAT(l.first_name,' ',l.last_name)               AS client_name,
               CASE WHEN lr.object_type='application' THEN lr.object_id ELSE NULL END AS app_id
        FROM leads_leadrequirement lr
        JOIN leads_lead l ON l.id = lr.lead_id
        WHERE lr.type_id IN ({REMED_TYPE_IDS_SQL})
          AND l.user_id IN ({_USER_IDS_SQL})
          AND lr.created >= {utc_start}
          AND lr.created <  {utc_end}
        ORDER BY lr.created ASC
    """)
    details = defaultdict(list)
    for r in cursor.fetchall():
        details[r["adviser_id"]].append({
            "req_id": r["req_id"],
            "lead_id": r["lead_id"],
            "object_type": r["object_type"],
            "object_id": r["object_id"],
            "task_name": r["task_name"],
            "description": (r["description"] or "").strip(),
            "last_note": (r["last_note"] or "").strip(),
            "status": int(r["status"]),
            "created_date": str(r["created_date"]),
            "client_name": (r["client_name"] or "").strip(),
            "app_id": r["app_id"],
        })

    return counts, dict(details)


@app.route("/")
@login_required
def index():
    req_t0 = time.monotonic()
    today     = date.today()
    lbd       = last_biz_day()
    min_date_obj = date.fromisoformat(MIN_DATE)
    data_updated_str = datetime.now().strftime("%d/%m/%y")
    db_max_date = lbd  # picker upper bound — set properly below after DB query

    # ── Query DB for actual refresh time and last full data day ──────────────
    try:
        _conn = get_connection()
        _cur  = _conn.cursor(dictionary=True)
        try:
            _cur.execute("""
                SELECT MAX(created) AS max_utc FROM noojee_callrecord
            """)
            _ref = _cur.fetchone()
            if _ref and _ref["max_utc"]:
                raw_utc = _ref["max_utc"]
                if hasattr(raw_utc, 'strftime'):
                    # Convert UTC to local manually
                    raw_dt = raw_utc + timedelta(hours=11)
                    m = raw_dt.month
                    tz_abbr = 'AEDT' if (m >= 10 or m <= 4) else 'AEST'
                    data_updated_str = raw_dt.strftime("%d/%m/%y · %I:%M %p ").lstrip('0') + tz_abbr
                    db_max_date = raw_dt.date()
        except Exception as _e:
            log.warning("[refresh_dt] %s", _e)
        try:
            # Last full day of data — use index-friendly range scan from recent dates
            _cur.execute(f"""
                SELECT DATE(CONVERT_TZ(created,'+00:00','{TZ_OFFSET}')) AS day, COUNT(*) AS cnt
                FROM noojee_callrecord
                WHERE created >= DATE_SUB(NOW(), INTERVAL 14 DAY)
                GROUP BY day HAVING cnt > 10
                ORDER BY day DESC LIMIT 1
            """)
            _mx = _cur.fetchone()
            if _mx and _mx["day"]:
                raw = _mx["day"]
                last_full_day = raw if hasattr(raw, 'year') else date.fromisoformat(str(raw)[:10])
                while last_full_day.weekday() >= 5:
                    last_full_day -= timedelta(days=1)
                lbd = last_full_day
        except Exception as _e:
            log.warning("[last_full_day] %s", _e)
        _cur.close(); _conn.close()
    except Exception as _e:
        log.warning("[db_init] %s", _e)
    # ────────────────────────────────────────────────────────────────────────

    # Default view = M0 (month-to-date)
    default_end   = today
    default_start = max(today.replace(day=1), min_date_obj)
    if default_start > default_end:
        default_start = max(default_end-timedelta(days=20), min_date_obj)

    start_str        = request.args.get("start", default_start.isoformat())
    end_str          = request.args.get("end",   default_end.isoformat())
    active_tab       = request.args.get("tab","perf")

    try:
        start=date.fromisoformat(start_str); end=date.fromisoformat(end_str)
    except ValueError:
        start,end=default_start,default_end

    start=max(start,min_date_obj); end=max(end,min_date_obj)
    if start>end: start,end=end,start
    # Hard cap — never allow end beyond today to prevent pool exhaustion
    if end > today: end = today
    if start > today: start = today

    log.info("Dashboard request: %s to %s", start, end)

    # Single connection for ALL queries — avoids pool exhaustion from multiple connections
    conn=None
    try:
        conn=get_connection()
    except Exception as e:
        return render_template("error.html", error_msg=str(e)), 503

    is_single_day = (start == end)

    cursor=conn.cursor(dictionary=True)
    try:
        advisers       = _timed("advisers",    get_advisers, cursor)
        perf           = _timed("perf_stats",  get_performance_stats, cursor, start, end)
        pipeline       = _timed("pipeline",    get_pipeline_stats, cursor, start, end)
        biz_days       = biz_days_in_range(start, end)
        if is_single_day:
            dates_list, daily_by_user, calls_day = _timed("hourly_series", get_hourly_series, cursor, start)
            assigned_d, contacted_d, no_contact_d, booked_d = _timed("hourly_pipeline", get_hourly_pipeline_series, cursor, start)
        else:
            dates_list, daily_by_user, calls_day = _timed("daily_series", get_daily_series, cursor, start, end)
            # Ensure ALL calendar dates are in dates_list (not just dates with data)
            all_dates = [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
            dates_list = sorted(set(dates_list) | set(all_dates))
            assigned_d, contacted_d, no_contact_d, booked_d = _timed("daily_pipeline", get_daily_pipeline_series, cursor, start, end, dates_list)

        # For weekly mode keep weekdays (Mon-Fri); for daily/monthly also strip weekends
        if not is_single_day:
            dates_list = [d for d in dates_list if date.fromisoformat(d).weekday() < 5]
        appt_today, appt_future = _timed("appointments", get_schedule_appointments, cursor, today)
        remed_counts, remed_details = _timed("remediations", get_remediation_stats, cursor, start, end)
    finally:
        cursor.close(); conn.close()

    # Determine chart axis mode
    if is_single_day:
        chart_mode = "hourly"
    else:
        span = (end - start).days
        if span <= 4 and start.weekday() == 0:  # starts on Monday, ≤5 days
            chart_mode = "weekly"
        else:
            chart_mode = "daily"

    months=biz_days/20 if biz_days else 1
    perf_rows,checks_rows=[],[]

    for adv in advisers:
        uid=adv["id"]; name=adv["name"]
        initials=(adv["first_name"][0]+adv["last_name"][0]).upper()
        avatar_color=AVATAR_COLORS.get(uid,"#6b7280")
        avatar_file=AVATAR_FILES.get(uid,"")
        avatar_url=f"/static/avatars/{avatar_file}" if avatar_file else ""

        p=perf.get(uid,{})
        days_worked=int(p.get("days_worked",0))
        talk_secs=int(p.get("talk_secs",0))
        quotes_count=int(p.get("quotes_count",0))
        quotes_value=float(p.get("quotes_value",0))
        apps_count=int(p.get("apps_count",0))
        apps_value=float(p.get("apps_value",0))
        inforce_count=int(p.get("inforce_count",0))
        inforce_value=float(p.get("inforce_value",0))
        quote_avg=quotes_value/quotes_count if quotes_count else 0
        quotes_per_day=quotes_count/days_worked if days_worked else 0
        q2a_pct=apps_count/quotes_count*100 if quotes_count else 0
        talk_per_day_s=talk_secs/days_worked if days_worked else 0
        apps_per_day=apps_count/days_worked if days_worked else 0
        apps_avg=round(apps_value/apps_count,2) if apps_count else 0
        monthly_inf=inforce_value/months if months else 0

        assigned  = pipeline["assigned"].get(uid,0)
        contacted = pipeline["contacted"].get(uid,0)
        booked    = pipeline["booked"].get(uid,0)
        no_contact  = pipeline["no_contact"].get(uid,0)
        conv_ac = round(contacted/assigned*100,1) if assigned else 0
        conv_cb = round(booked/contacted*100,1)   if contacted else 0
        conv_ab = round(booked/assigned*100,1)    if assigned else 0

        _at = appt_today.get(uid,{})  if isinstance(appt_today.get(uid), dict) else {"disc":0,"fu":0,"q":0}
        _af = appt_future.get(uid,{}) if isinstance(appt_future.get(uid), dict) else {"disc":0,"fu":0,"q":0}

        base={"name":name,"user_id":uid,"initials":initials,
              "avatar_color":avatar_color,"avatar_url":avatar_url}

        perf_rows.append({**base,
            "days_worked":days_worked,
            "talk_time":fmt_hms(talk_secs), "talk_per_day":fmt_hms(talk_per_day_s),
            "talk_per_day_s":talk_per_day_s, "talk_time_s":talk_secs,
            "talk_color":color_talk(talk_per_day_s),
            "quotes_count":quotes_count,"quote_total":quotes_value,"quote_avg":quote_avg,
            "quotes_per_day":round(quotes_per_day,1),"quotes_color":color_quotes(quotes_per_day),
            "apps_count":apps_count,"apps_value":apps_value,"apps_avg":apps_avg,
            "apps_per_day":round(apps_per_day,1),"apps_color":color_apps(apps_per_day),
            "q2a_pct":round(q2a_pct,1),
            "inforce_count":inforce_count,"inforce_value":inforce_value,
            "inforce_color":color_inforce(monthly_inf),
            "assigned":assigned,
            "remed_pending":remed_counts.get(uid,{}).get("pending",0),
            "remed_total":remed_counts.get(uid,{}).get("total",0),
        })
        checks_rows.append({**base,
            "assigned":assigned,"contacted":contacted,"not_contacted":no_contact,
            "booked":booked,
            "quotes_count":quotes_count,"apps_count":apps_count,"apps_value":apps_value,
            "inforce_count":inforce_count,"inforce_value":inforce_value,
            "conv_ac":conv_ac,"conv_cb":conv_cb,"conv_ab":conv_ab,
            "today_disc":_at.get("disc",0),"today_fu":_at.get("fu",0),"today_q":_at.get("q",0),
            "future_disc":_af.get("disc",0),"future_fu":_af.get("fu",0),"future_q":_af.get("q",0),
        })

    chart_advisers=[]
    for adv in advisers:
        uid=adv["id"]; udata=daily_by_user.get(uid,{}); ucalls=calls_day.get(uid,{})
        ucont=contacted_d.get(uid,{})
        series={
            "uid":uid,"name":adv["name"],
            "initials":(adv["first_name"][0]+adv["last_name"][0]).upper(),
            "talk_mins":[],"quotes_cnt":[],"apps_cnt":[],"apps_val":[],"inforce_val":[],"calls_cnt":[],
        }
        for d in dates_list:
            row=udata.get(d,{})
            series["talk_mins"].append(round(int(row.get("talk_time_seconds",0))/60,1))
            series["quotes_cnt"].append(int(row.get("leads_quoted",0)))
            series["apps_cnt"].append(int(row.get("apps_count",0)))
            series["apps_val"].append(float(row.get("apps_value",0)))
            series["inforce_val"].append(float(row.get("inforce_value",0)))
            series["calls_cnt"].append(ucont.get(d,0))
        chart_advisers.append(series)

    # ── Quick-filter presets (D0, D1, W0, W1, M0, M1) ────────────────────
    # D0 = today, D1 = yesterday
    d0_start = today;           d0_end = today
    d1_start = today - timedelta(days=1); d1_end = d1_start

    # W0 = week-to-date (Monday–Friday), W1 = prior full week Mon–Fri
    days_since_mon = today.weekday()              # Mon=0 … Sun=6
    w0_start = max(today - timedelta(days=days_since_mon), min_date_obj)
    w0_end   = today
    w1_end   = w0_start - timedelta(days=3)       # Friday before current week
    w1_start = max(w1_end - timedelta(days=4), min_date_obj)  # Monday of prior week

    # M0 = month-to-date, M1 = prior full month
    m0_start = max(today.replace(day=1), min_date_obj)
    m0_end   = today
    m1_end   = m0_start - timedelta(days=1)       # last day of previous month
    m1_start = max(m1_end.replace(day=1), min_date_obj)

    # Pre-compute team averages matching the tfoot row exactly
    n_adv = len(perf_rows)
    if n_adv:
        avg_talk_s  = sum(r["talk_per_day_s"]   for r in perf_rows) / n_adv
        avg_qpd     = sum(r["quotes_per_day"]    for r in perf_rows) / n_adv
        avg_apd     = sum(r["apps_per_day"]      for r in perf_rows) / n_adv
        avg_talk_hm = f"{int(avg_talk_s//3600)}:{int((avg_talk_s%3600)//60):02d}"
    else:
        avg_talk_s = avg_qpd = avg_apd = 0
        avg_talk_hm = "0:00"
    team_avgs = {"talk_mins": round(avg_talk_s/60, 2), "talk_fmt": avg_talk_hm,
                 "qpd": round(avg_qpd, 2), "apd": round(avg_apd, 2)}

    # Parse multi-select adviser param (default excludes Lucas 53)
    selected_adviser_raw = request.args.get("adviser", "")
    if selected_adviser_raw:
        selected_advisers = [s.strip() for s in selected_adviser_raw.split(",") if s.strip()]
    else:
        selected_advisers = [str(uid) for uid in sorted(SHOW_USER_IDS) if uid != 53]

    total_ms = (time.monotonic() - req_t0) * 1000
    log.info("Dashboard total: %.0f ms", total_ms)

    return render_template("dashboard.html",
        start=start.isoformat(), end=end.isoformat(), min_date=MIN_DATE, max_date=db_max_date.isoformat(),
        biz_days=biz_days, months=round(months, 2),
        perf_rows=perf_rows, checks_rows=checks_rows,
        dates_list=dates_list,
        chart_advisers=chart_advisers,
        selected_advisers=selected_advisers, active_tab=active_tab,
        last_refresh=data_updated_str,
        lbd=lbd.isoformat(), today_iso=today.isoformat(),
        d0_start=d0_start.isoformat(), d0_end=d0_end.isoformat(),
        d1_start=d1_start.isoformat(), d1_end=d1_end.isoformat(),
        w0_start=w0_start.isoformat(), w0_end=w0_end.isoformat(),
        w1_start=w1_start.isoformat(), w1_end=w1_end.isoformat(),
        m0_start=m0_start.isoformat(), m0_end=m0_end.isoformat(),
        m1_start=m1_start.isoformat(), m1_end=m1_end.isoformat(),
        team_avgs=team_avgs,
        chart_mode=chart_mode,
        remed_details=remed_details,
        crm_base_url=CRM_BASE_URL,
    )


@app.errorhandler(500)
def internal_error(e):
    return render_template("error.html", error_msg="An unexpected server error occurred. Please try again."), 500

@app.errorhandler(503)
def service_unavailable(e):
    return render_template("error.html", error_msg="Database temporarily unavailable. Please try again shortly."), 503

if __name__=="__main__":
    app.run(debug=True, port=5001)
