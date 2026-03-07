"""
build_config.py
Queries the live database and generates a populated report_config.py
for any adviser + month/year combination.

Usage:
    python build_config.py --user_id 80 --month 2 --year 2026

Requires: mysql-connector-python, python-dotenv
"""

import argparse
import calendar
import json
import statistics
import os
from datetime import datetime, timedelta
from textwrap import dedent

import mysql.connector

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass


# ═══════════════════════════════════════════════════════════════════
#  DB CONNECTION
# ═══════════════════════════════════════════════════════════════════

def get_connection():
    """Connect to the reporting replica."""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "prod-slife-crm-db-reporting.cjte8bbhwgp7.ap-southeast-2.rds.amazonaws.com"),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", os.getenv("DB_PASS", "")),
        database=os.getenv("DB_NAME", "lifeinsurancepartners"),
        connect_timeout=30,
    )


def query(conn, sql, params=None):
    """Execute a query and return list of dicts."""
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    cur.close()
    return rows


# ═══════════════════════════════════════════════════════════════════
#  SECTION BUILDERS
# ═══════════════════════════════════════════════════════════════════

def build_identity(conn, user_id, month, year):
    """Section: Adviser name, practice name, report metadata."""
    row = query(conn, """
        SELECT u.first_name, u.last_name, ug.name AS practice_name
        FROM auth_user u
        JOIN account_usergroup_users ugu ON ugu.user_id = u.id
        JOIN account_usergroup ug ON ug.id = ugu.usergroup_id
        WHERE u.id = %s
        LIMIT 1
    """, (user_id,))[0]

    month_name = calendar.month_name[month]
    # Report date = last day of the month
    import calendar as cal
    last_day = cal.monthrange(year, month)[1]
    report_date = f"{last_day} {month_name} {year}"

    return {
        "ADVISER_NAME": f"{row['first_name']} {row['last_name']}",
        "PRACTICE_NAME": row["practice_name"],
        "REPORT_DATE": report_date,
        "REPORT_MONTH": month,
        "REPORT_YEAR": year,
        "REPORT_MONTH_NAME": month_name,
        "TOTAL_PAGES": 13,
        "HAS_PAGE6": True,
    }


def build_12month_performance(conn, user_id, month, year):
    """Sections 1 & 2: 12-month premium trend + KPI tiles."""
    # Calculate 12-month window ending at report month
    end_date = datetime(year, month, 1) + timedelta(days=32)
    end_date = end_date.replace(day=1)  # 1st of next month
    start_date = datetime(year - 1, month, 1) + timedelta(days=32)
    start_date = start_date.replace(day=1)  # 12 months back

    rows = query(conn, """
        SELECT YEAR(submitted) AS y, MONTH(submitted) AS m,
               COUNT(*) AS apps, ROUND(SUM(premium)) AS prem
        FROM applications_application
        WHERE adviser_id = %s
          AND submitted >= %s AND submitted < %s
        GROUP BY YEAR(submitted), MONTH(submitted)
        ORDER BY y, m
    """, (user_id, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))

    months_data = [{"y": r["y"], "m": r["m"], "apps": r["apps"], "prem": int(r["prem"] or 0)} for r in rows]

    # Current month data
    current = next((m for m in months_data if m["y"] == year and m["m"] == month), None)
    if not current:
        current = {"y": year, "m": month, "apps": 0, "prem": 0}

    total_prem = current["prem"]
    total_apps = current["apps"]
    avg_prem = round(total_prem / total_apps) if total_apps > 0 else 0

    # Find historical best for comparison labels
    prior_months = [m for m in months_data if not (m["y"] == year and m["m"] == month)]
    best_prior_prem = max((m["prem"] for m in prior_months), default=0)
    best_prior_apps = max((m["apps"] for m in prior_months), default=0)

    # KPI labels
    prem_label = ""
    if total_prem > best_prior_prem and total_prem > 0:
        prem_label = "Personal best"
        if total_prem >= 100000:
            prem_label += " \u2013 $100K+ milestone"
    apps_label = "Highest volume month" if total_apps > best_prior_apps else ""
    avg_label = ""
    if total_apps > 0:
        prior_avgs = [m["prem"] / m["apps"] for m in prior_months if m["apps"] > 0]
        if prior_avgs and avg_prem > max(prior_avgs):
            avg_label = "Highest ever average"

    return {
        "MONTHS_DATA": months_data,
        "KPI_TOTAL_SUBMITTED": f"${total_prem:,}",
        "KPI_TOTAL_SUBMITTED_RAW": total_prem,
        "KPI_APPLICATIONS": total_apps,
        "KPI_AVG_PREMIUM": f"${avg_prem:,}",
        "KPI_AVG_PREMIUM_RAW": avg_prem,
        "KPI_TOTAL_SUB_LABEL": prem_label,
        "KPI_APPS_LABEL": apps_label,
        "KPI_AVG_LABEL": avg_label,
    }


def build_benchmarking(conn, user_id, month, year):
    """Section 3: Network benchmarking — 12-month total premium submitted across all practices."""
    # 12-month window ending at report month (same window as sections 1 & 2)
    end_dt = datetime(year, month, 1) + timedelta(days=32)
    end_dt = end_dt.replace(day=1)
    start_dt = datetime(year - 1, month, 1) + timedelta(days=32)
    start_dt = start_dt.replace(day=1)

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")
    bench_period = f"{calendar.month_name[start_dt.month]} {start_dt.year} to {calendar.month_name[month]} {year}"

    # All active practices: 12-month total premium submitted
    all_practices = query(conn, """
        SELECT a.adviser_id AS user_id, ROUND(SUM(a.premium)) AS total_prem
        FROM applications_application a
        WHERE a.submitted >= %s AND a.submitted < %s
          AND a.adviser_id IN (
            SELECT DISTINCT ugu.user_id
            FROM account_usergroup_users ugu
            JOIN account_usergroup ug ON ug.id=ugu.usergroup_id AND ug.real=1 AND ug.is_active=1
            JOIN auth_user au ON au.id=ugu.user_id AND au.is_active=1
            WHERE ugu.user_id NOT IN (88, 118, 172)
          )
        GROUP BY a.adviser_id
        ORDER BY total_prem DESC
    """, (start_str, end_str))

    # Total active practices (including those with 0 submissions)
    total_row = query(conn, """
        SELECT COUNT(DISTINCT ugu.user_id) as total_practices
        FROM account_usergroup_users ugu
        JOIN account_usergroup ug ON ug.id=ugu.usergroup_id AND ug.real=1 AND ug.is_active=1
        JOIN auth_user au ON au.id=ugu.user_id AND au.is_active=1
        WHERE ugu.user_id NOT IN (88, 118, 172)
    """)[0]
    total_practices = total_row["total_practices"]

    premiums = []
    adviser_prem = 0
    for p in all_practices:
        prem = int(p["total_prem"] or 0)
        premiums.append(prem)
        if p["user_id"] == user_id:
            adviser_prem = prem

    # Add zero-premium practices
    zero_count = total_practices - len(premiums)
    premiums.extend([0] * max(zero_count, 0))
    premiums.sort(reverse=True)

    n = len(premiums)
    rank = sum(1 for p in premiums if p > adviser_prem) + 1
    percentile = round((1 - rank / n) * 100) if n > 0 else 0

    avg_prem = round(statistics.mean(premiums)) if premiums else 0
    med_prem = round(statistics.median(premiums)) if premiums else 0
    sorted_asc = sorted(premiums)
    top_q_prem = sorted_asc[int(n * 0.75)] if n > 0 else 0

    # Quartile thresholds for percentile bar labels ($)
    prem_q1 = sorted_asc[int(n * 0.25)] if n > 0 else 0
    prem_q2 = med_prem
    prem_q3 = top_q_prem

    # Histogram bins in $k ranges
    bin_edges_k = [(0, 50), (50, 100), (100, 150), (150, 200), (200, 250), (250, 300)]
    bin_labels = ["$0–50k", "$50–100k", "$100–150k", "$150–200k", "$200–250k", "$250–300k", "$300k+"]
    hist_data = {lbl: 0 for lbl in bin_labels}
    for p in premiums:
        p_k = p / 1000
        placed = False
        for (lo, hi), lbl in zip(bin_edges_k, bin_labels[:-1]):
            if lo <= p_k < hi:
                hist_data[lbl] += 1
                placed = True
                break
        if not placed:
            hist_data["$300k+"] += 1

    return {
        "ADVISER_PREMIUM_12M": adviser_prem,
        "ADVISER_RANK": rank,
        "TOTAL_PRACTICES": n,
        "PERCENTILE": percentile,
        "NETWORK_AVG_PREM": avg_prem,
        "MEDIAN_PREM": med_prem,
        "TOP_QUARTILE_PREM": top_q_prem,
        "PREM_Q1": prem_q1,
        "PREM_Q2": prem_q2,
        "PREM_Q3": prem_q3,
        "BENCH_PERIOD": bench_period,
        "HIST_DATA": hist_data,
    }


def build_referral_partners(conn, user_id, month, year):
    """Section 4: Referral partner performance (12-month window)."""
    end_dt = datetime(year, month, 1) + timedelta(days=32)
    end_dt = end_dt.replace(day=1)
    start_dt = datetime(year, month, 1)
    for _ in range(11):
        start_dt = (start_dt - timedelta(days=1)).replace(day=1)

    rows = query(conn, """
        SELECT
          ls.name as source_name,
          l.tags_cache as contact_tag,
          COUNT(DISTINCT l.id) as leads,
          COUNT(DISTINCT CASE WHEN a.id IS NOT NULL THEN l.id END) as apps,
          ROUND(SUM(CASE WHEN a.premium IS NOT NULL THEN a.premium ELSE 0 END)) as total_prem,
          ROUND(COUNT(DISTINCT CASE WHEN a.id IS NOT NULL THEN l.id END) /
            NULLIF(COUNT(DISTINCT l.id), 0) * 100) as conv
        FROM leads_lead l
        LEFT JOIN leads_leadsource ls ON ls.id = l.source_id
        LEFT JOIN applications_application a ON a.lead_id = l.id AND a.submitted IS NOT NULL
        WHERE l.user_id = %s
          AND l.created >= %s AND l.created < %s
        GROUP BY ls.name, l.tags_cache
        HAVING leads >= 2
        ORDER BY leads DESC
        LIMIT 25
    """, (user_id, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")))

    # First pass: identify real org groups
    # (a) prefix appears 2+ times in "prefix - individual" tags
    prefix_counts = {}
    suffix_counts = {}
    for r in rows:
        tag = r["contact_tag"] or ""
        if " - " in tag:
            prefix = tag.split(" - ", 1)[0].strip().title()
            suffix = tag.split(" - ", 1)[1].strip().title()
            prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
            suffix_counts[suffix] = suffix_counts.get(suffix, 0) + 1
    known_groups = {p for p, cnt in prefix_counts.items() if cnt >= 2}
    # Also detect reversed "Individual - Group" format (suffix appears 2+ times)
    known_groups |= {s for s, cnt in suffix_counts.items() if cnt >= 2}

    # (b) standalone tags (no " - ") that share the same first-two-word prefix → merge into that prefix
    standalone_tag_map = {}  # titled_tag -> merged_group_name
    prefix2_map = {}
    for r in rows:
        tag = r["contact_tag"] or ""
        if tag and " - " not in tag:
            titled = tag.strip().title()
            words = titled.split()
            if len(words) >= 2:
                p2 = " ".join(words[:2])
                prefix2_map.setdefault(p2, set()).add(titled)
    for p2, tag_set in prefix2_map.items():
        if len(tag_set) >= 2:
            for t in tag_set:
                standalone_tag_map[t] = p2

    partners = []   # individual rows for the breakdown table
    groups = {}     # group_name -> aggregated totals

    for r in rows:
        tag = r["contact_tag"] or ""
        source = r["source_name"] or "Other"
        leads = r["leads"]
        apps_count = int(r["apps"] or 0)
        prem = int(r["total_prem"] or 0)

        if " - " in tag:
            prefix = tag.split(" - ", 1)[0].strip().title()
            suffix = tag.split(" - ", 1)[1].strip().title()
            if prefix in known_groups:
                # Normal "Group - Individual" format
                group_name = prefix
                individual = tag.split(" - ", 1)[1].strip()
            elif suffix in known_groups:
                # Reversed "Individual - Group" format (e.g. "Emma Reiterer - Nectar")
                group_name = suffix
                individual = tag.split(" - ", 1)[0].strip()
            else:
                # One-off referrer — "Person - Org" or "Org - Person"
                # Shorter side is the person's name; if equal, left=person, right=org
                left  = tag.split(" - ", 1)[0].strip()
                right = tag.split(" - ", 1)[1].strip()
                left_words  = len(left.split())
                right_words = len(right.split())
                if right_words < left_words:
                    person, org = right, left
                else:
                    person, org = left, right
                group_name = org.title()
                individual = person.title()
        elif tag and " VIA " in tag.upper():
            # e.g. "TAL REFERRAL VIA CAM BLUNT" → org="Tal", person="Cam Blunt"
            via_idx = tag.upper().index(" VIA ")
            org_part = tag[:via_idx].strip().split()[0]  # first word before VIA
            person_part = tag[via_idx + 5:].strip()
            group_name = org_part.title()
            individual = person_part.title()
        elif tag:
            tag_titled = tag.strip().title()
            if tag_titled in known_groups:
                # Exact match to a known group (e.g. bare "Nectar") — fold into group totals
                group_name = tag_titled
                if group_name not in groups:
                    groups[group_name] = {"name": group_name, "leads": 0, "apps": 0, "prem": 0}
                groups[group_name]["leads"] += leads
                groups[group_name]["apps"] += apps_count
                groups[group_name]["prem"] += prem
                continue  # no individual partner row
            elif tag_titled in standalone_tag_map:
                # Shares a 2-word prefix with another standalone tag (e.g. "Shield Life Insurance"
                # and "Shield Life Website" → merged under "Shield Life")
                group_name = standalone_tag_map[tag_titled]
                individual = tag.strip()
            else:
                group_name = tag_titled
                individual = tag.strip()
        else:
            group_name = source.title()
            individual = source

        partners.append({
            "name": individual,
            "group": group_name,
            "leads": leads,
            "apps": apps_count,
            "prem": prem,
            "conv": int(r["conv"] or 0),
        })

        if group_name not in groups:
            groups[group_name] = {"name": group_name, "leads": 0, "apps": 0, "prem": 0}
        groups[group_name]["leads"] += leads
        groups[group_name]["apps"] += apps_count
        groups[group_name]["prem"] += prem

    def merge_into(canonical, victims):
        """Merge victim group names into canonical group in both dicts."""
        if canonical not in groups:
            groups[canonical] = {"name": canonical, "leads": 0, "apps": 0, "prem": 0}
        for gn in victims:
            if gn not in groups or gn == canonical:
                continue
            groups[canonical]["leads"] += groups[gn]["leads"]
            groups[canonical]["apps"]  += groups[gn]["apps"]
            groups[canonical]["prem"]  += groups[gn]["prem"]
            del groups[gn]
            for p in partners:
                if p["group"] == gn:
                    p["group"] = canonical

    # Pass 1: two-word prefix merge
    # e.g. "Shield Life Insurance" + "Shield Life Website" → "Shield Life"
    prefix2_map2 = {}
    for name in list(groups.keys()):
        words = name.split()
        if len(words) >= 2:
            prefix2_map2.setdefault(" ".join(words[:2]), []).append(name)
    for p2, gnames in prefix2_map2.items():
        if len(gnames) >= 2:
            merge_into(p2, gnames)

    # Pass 2: first-word prefix merge into real orgs
    # e.g. "Nectar Mortgages - Abhi Dutta" → "Nectar" (because "Nectar" has 2+ members)
    member_counts = {}
    for p in partners:
        member_counts[p["group"]] = member_counts.get(p["group"], 0) + 1
    real_orgs = {name for name, cnt in member_counts.items() if cnt >= 2}

    for real_org in list(real_orgs):
        org_words = real_org.split()
        victims = [
            name for name in list(groups.keys())
            if name != real_org
            and name not in real_orgs
            and name.split()[:len(org_words)] == org_words
        ]
        if victims:
            merge_into(real_org, victims)

    # Build group list (for chart) — exclude solo individual referrers
    # (exactly 1 partner with a "Person - Org" style group name)
    final_member_counts = {}
    for p in partners:
        final_member_counts[p["group"]] = final_member_counts.get(p["group"], 0) + 1

    group_list = []
    for g in groups.values():
        g["conv"] = round(g["apps"] / g["leads"] * 100) if g["leads"] > 0 else 0
        member_count = final_member_counts.get(g["name"], 0)
        # Exclude solo individual referrers from the chart:
        # any group with 1 partner whose name contains " - " OR whose name reads like
        # a personal referral description (all words, no obvious org identity)
        is_solo_individual = (
            member_count == 1 and (
                " - " in g["name"]
                or all(w[0].isupper() for w in g["name"].split() if w)
                # ^^ all-caps-initial-word names (e.g. "Tal Referral Via Cam Blunt")
                # are likely personal descriptions, not org names
            )
        ) and g["name"] not in ("Other",)
        if not is_solo_individual:
            group_list.append(g)
    group_list.sort(key=lambda x: -x["leads"])
    partners.sort(key=lambda x: -x["leads"])

    return {"PARTNERS": partners, "PARTNER_GROUPS": group_list}


def build_insurers_and_submissions(conn, user_id, month, year):
    """Sections 5 & 6: Insurer diversification + full submissions table."""
    start_str = f"{year}-{month:02d}-01"
    end_dt = datetime(year, month, 1) + timedelta(days=32)
    end_str = end_dt.replace(day=1).strftime("%Y-%m-%d")

    apps_raw = query(conn, """
        SELECT
          a.customer_name, ROUND(a.premium) as premium, a.company_name as insurer,
          a.status, a.submitted, a.commenced,
          a.life, a.tpd, a.trauma, a.ip
        FROM applications_application a
        WHERE a.adviser_id = %s
          AND a.submitted >= %s AND a.submitted < %s
        ORDER BY a.premium DESC
    """, (user_id, start_str, end_str))

    # Status mapping: 0=In Progress, 4=Inforced, 5=Completed, 6=Cancelled
    status_map = {0: "In Progress", 4: "Inforced", 5: "Completed", 6: "Cancelled"}

    # Build insurer counts
    insurer_counts = {}
    apps_list = []
    for a in apps_raw:
        insurer = a["insurer"] or "Unknown"
        insurer_counts[insurer] = insurer_counts.get(insurer, 0) + 1

        # Product string
        products = []
        if a["life"]:
            products.append("Life")
        if a["tpd"]:
            products.append("TPD")
        if a["trauma"]:
            products.append("Trauma")
        if a["ip"]:
            products.append("IP")

        # Clean customer name (strip Mr/Mrs/Ms)
        name = (a["customer_name"] or "").strip()
        for prefix in ["Mr ", "Mrs ", "Ms ", "Miss ", "Dr "]:
            if name.startswith(prefix):
                name = name[len(prefix):]

        sub_date = a["submitted"]
        date_str = sub_date.strftime("%-d %b") if sub_date else ""

        status_code = a["status"] or 0
        status_text = status_map.get(status_code, f"Status {status_code}")

        prem_val = int(a["premium"]) if a["premium"] else "TBC"

        apps_list.append({
            "client": name,
            "prem": prem_val,
            "insurer": insurer,
            "status": status_text,
            "products": ", ".join(products),
            "date": date_str,
            "green": False,  # Could flag same-day submissions
        })

    # Sort insurers by count descending
    insurers = sorted(insurer_counts.items(), key=lambda x: -x[1])

    # Inforced vs In Progress tiles
    inforced_apps = [a for a in apps_list if a["status"] == "Inforced"]
    ip_apps = [a for a in apps_list if a["status"] == "In Progress"]
    inforced_prem = sum(a["prem"] for a in inforced_apps if isinstance(a["prem"], int))
    ip_prem = sum(a["prem"] for a in ip_apps if isinstance(a["prem"], int))

    return {
        "INSURERS": insurers,
        "APPS": apps_list,
        "SUBMISSIONS_FOOTNOTE": "",
        "_inforced_count": len(inforced_apps),
        "_inforced_prem": inforced_prem,
        "_ip_count": len(ip_apps),
        "_ip_prem": ip_prem,
    }


def build_speed_to_contact(conn, user_id, month, year):
    """Section 7: Call activity vs conversion rate (8-month window)."""
    end_dt = datetime(year, month, 1) + timedelta(days=32)
    end_dt = end_dt.replace(day=1)
    start_dt = datetime(year, month, 1)
    for _ in range(11):
        start_dt = (start_dt - timedelta(days=1)).replace(day=1)

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    rows = query(conn, """
        SELECT
          CASE
            WHEN COALESCE(cc.consultant_calls, 0) = 0 THEN '0 calls'
            WHEN COALESCE(cc.consultant_calls, 0) = 1 THEN '1 call'
            WHEN COALESCE(cc.consultant_calls, 0) = 2 THEN '2 calls'
            ELSE '3+ calls'
          END as bucket,
          COUNT(DISTINCT l.id) as leads,
          COUNT(DISTINCT CASE WHEN l.status = 5 THEN l.id END) as converted,
          ROUND(AVG(CASE WHEN l.status = 5 THEN a.app_value END)) as avg_case
        FROM leads_lead l
        LEFT JOIN (
          SELECT la.object_id, COUNT(*) as consultant_calls
          FROM leads_leadaction la
          WHERE la.object_type = 'lead' AND la.action_type = 'call'
            AND la.deleted = 0
            AND la.user_id IN (
              SELECT user_id FROM account_userrole_users WHERE userrole_id = 2
            )
          GROUP BY la.object_id
        ) cc ON cc.object_id = l.id
        LEFT JOIN applications_application a ON a.lead_id = l.id
        WHERE l.user_id = %s
          AND l.created >= %s AND l.created < %s
        GROUP BY bucket
        ORDER BY FIELD(bucket, '0 calls', '1 call', '2 calls', '3+ calls')
    """, (user_id, start_str, end_str))

    buckets = ["0 calls", "1 call", "2 calls", "3+ calls"]
    conv_rates = []
    avg_values = []
    total_leads = 0
    bucket_lead_counts = []

    for b in buckets:
        row = next((r for r in rows if r["bucket"] == b), None)
        if row:
            leads = row["leads"]
            converted = row["converted"]
            rate = round(converted / leads * 100, 1) if leads > 0 else 0
            conv_rates.append(rate)
            avg_values.append(int(row["avg_case"] or 0))
            total_leads += leads
            bucket_lead_counts.append(leads)
        else:
            conv_rates.append(0)
            avg_values.append(0)
            bucket_lead_counts.append(0)

    # Detect face-to-face advisers: <5% of leads have any phone calls made by a consultant
    leads_with_calls = sum(bucket_lead_counts[1:])  # 1 call, 2 calls, 3+ calls
    is_face_to_face = (total_leads > 10 and leads_with_calls / total_leads < 0.05)

    # Quoted vs unquoted conversion
    quoted_row = query(conn, """
        SELECT
          ROUND(COUNT(DISTINCT CASE WHEN l.status = 5 AND lq.id IS NOT NULL THEN l.id END) /
                NULLIF(COUNT(DISTINCT CASE WHEN lq.id IS NOT NULL THEN l.id END), 0) * 100, 1) as quoted_conv,
          ROUND(COUNT(DISTINCT CASE WHEN l.status = 5 AND lq.id IS NULL THEN l.id END) /
                NULLIF(COUNT(DISTINCT CASE WHEN lq.id IS NULL THEN l.id END), 0) * 100, 1) as unquoted_conv
        FROM leads_lead l
        LEFT JOIN leads_leadquote lq ON lq.lead_id = l.id
        WHERE l.user_id = %s
          AND l.created >= %s AND l.created < %s
    """, (user_id, start_str, end_str))[0]

    return {
        "CALL_BUCKETS": buckets,
        "CONV_RATES": conv_rates,
        "AVG_CASE_VALUES": avg_values,
        "TOTAL_LEADS_STC": total_leads,
        "STC_PERIOD": "12 months",
        "QUOTED_CONV_RATE_STC": float(quoted_row["quoted_conv"] or 0),
        "UNQUOTED_CONV_RATE_STC": float(quoted_row["unquoted_conv"] or 0),
        "IS_FACE_TO_FACE": is_face_to_face,
    }


def build_completion_forecast(conn, user_id, month, year):
    """Section 8: In-progress → inforced journey.
    Denominator = apps submitted >120 days ago (had full time to inforce).
    Completion indicator = status=4. Timing = submitted→commenced where available.
    """
    all_apps = query(conn, """
        SELECT a.submitted, a.commenced, a.status
        FROM applications_application a
        WHERE a.adviser_id = %s AND a.submitted IS NOT NULL
    """, (user_id,))

    def _as_dt(val):
        from datetime import date
        if isinstance(val, datetime):
            return val
        if isinstance(val, date):
            return datetime(val.year, val.month, val.day)
        return None

    # Eligible = apps submitted >120 days before report end
    # (gives enough time for the full in-progress→inforced journey)
    report_end = datetime(year, month, 1) + timedelta(days=32)
    report_end = report_end.replace(day=1)
    eligible_cutoff = report_end - timedelta(days=120)

    eligible_submitted = sum(
        1 for a in all_apps
        if _as_dt(a["submitted"]) and _as_dt(a["submitted"]) < eligible_cutoff
    )
    eligible_completed = sum(
        1 for a in all_apps
        if int(a["status"] or 0) == 4
        and _as_dt(a["submitted"]) and _as_dt(a["submitted"]) < eligible_cutoff
    )
    completion_rate = round(eligible_completed / eligible_submitted * 100) if eligible_submitted > 0 else 0

    # Timing chart: days from submitted to inforced (uses apps where commenced IS NOT NULL)
    week_buckets = {"Week 1": 0, "Week 2": 0, "Week 3": 0, "Week 4": 0, "Month 2": 0, "60+ days": 0}
    days_list = []
    for a in all_apps:
        commenced_dt = _as_dt(a["commenced"])
        submitted_dt = _as_dt(a["submitted"])
        if commenced_dt and submitted_dt:
            days = (commenced_dt - submitted_dt).days
            if days >= 0:
                days_list.append(days)
            if days <= 7:
                week_buckets["Week 1"] += 1
            elif days <= 14:
                week_buckets["Week 2"] += 1
            elif days <= 21:
                week_buckets["Week 3"] += 1
            elif days <= 28:
                week_buckets["Week 4"] += 1
            elif days <= 60:
                week_buckets["Month 2"] += 1
            else:
                week_buckets["60+ days"] += 1

    total_dated = sum(week_buckets.values())  # apps with timing data (for chart)
    per_period = []
    cumulative = []
    running = 0
    for lbl in ["Week 1", "Week 2", "Week 3", "Week 4", "Month 2", "60+ days"]:
        pct = round(week_buckets[lbl] / total_dated * 100) if total_dated > 0 else 0
        per_period.append(pct)
        running += pct
        cumulative.append(min(running, 100))

    avg_days = round(statistics.mean(days_list)) if days_list else 0

    # Current month in-progress
    start_str = f"{year}-{month:02d}-01"
    end_dt = datetime(year, month, 1) + timedelta(days=32)
    end_str = end_dt.replace(day=1).strftime("%Y-%m-%d")

    ip_apps = query(conn, """
        SELECT COUNT(*) as cnt, ROUND(SUM(premium)) as prem
        FROM applications_application
        WHERE adviser_id = %s AND submitted >= %s AND submitted < %s
          AND commenced IS NULL AND status = 0
    """, (user_id, start_str, end_str))[0]

    comm_apps = query(conn, """
        SELECT ROUND(SUM(premium)) as prem
        FROM applications_application
        WHERE adviser_id = %s AND submitted >= %s AND submitted < %s
          AND status = 4
    """, (user_id, start_str, end_str))[0]

    feb_ip = int(ip_apps["cnt"] or 0)
    feb_ip_prem = int(ip_apps["prem"] or 0)
    feb_comm_prem = int(comm_apps["prem"] or 0)
    expected_completions = round(feb_ip * completion_rate / 100)
    expected_prem = round(feb_ip_prem * completion_rate / 100 / 1000) * 1000

    return {
        "COMPLETION_BUCKETS": ["Week 1", "Week 2", "Week 3", "Week 4", "Month 2", "60+ days"],
        "PER_PERIOD_PCT": per_period,
        "CUMULATIVE_PCT": cumulative,
        "TOTAL_COMPLETED": eligible_completed,
        "TOTAL_SUBMITTED_HIST": eligible_submitted,
        "TOTAL_DATED": total_dated,
        "COMPLETION_RATE": completion_rate,
        "AVG_DAYS": avg_days,
        "FEB_IN_PROGRESS": feb_ip,
        "FEB_IP_PREMIUM": feb_ip_prem,
        "FEB_INFORCED_PREM": feb_comm_prem,
        "EXPECTED_COMPLETIONS": expected_completions,
        "EXPECTED_PREM": expected_prem,
        "TOTAL_FORECAST": feb_comm_prem + expected_prem,
    }


def build_quoted_pipeline(conn, user_id):
    """Section 9: Currently quoted leads (status=3, no close reason).
    Leads with 0 calls in 3CX are classified as face-to-face."""
    rows = query(conn, """
        SELECT
          CONCAT(l.first_name, ' ', l.last_name) as client,
          ROUND(COALESCE(lq.last_premium, 0)) as last_quoted,
          ls.name as source,
          l.calls_made
        FROM leads_lead l
        LEFT JOIN (
          SELECT lqq.lead_id, ROUND(lqq.value) as last_premium
          FROM leads_leadquote lqq
          INNER JOIN (
            SELECT lead_id, MAX(created) as latest
            FROM leads_leadquote
            WHERE deleted = 0
            GROUP BY lead_id
          ) mx ON mx.lead_id = lqq.lead_id AND mx.latest = lqq.created
          WHERE lqq.deleted = 0
        ) lq ON lq.lead_id = l.id
        LEFT JOIN leads_leadsource ls ON ls.id = l.source_id
        WHERE l.user_id = %s AND l.status = 3 AND l.close_reason_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM applications_application a
            WHERE a.lead_id = l.id AND a.submitted IS NOT NULL
          )
        ORDER BY ISNULL(lq.last_premium), lq.last_premium DESC
        LIMIT 20
    """, (user_id,))

    pipeline = []
    for r in rows:
        calls = int(r["calls_made"] or 0)
        pipeline.append({
            "client": " ".join(r["client"].split()),
            "last_quoted": int(r["last_quoted"] or 0),
            "source": r["source"] or "Other",
            "calls_made": calls,
            "f2f": calls == 0,
        })

    return {"PIPELINE": pipeline}


def build_conversion_drivers(conn, user_id, month, year):
    """Sections 10 & 11: What your data says works + pipeline segments."""
    # 12-month window
    end_dt = datetime(year, month, 1) + timedelta(days=32)
    end_dt = end_dt.replace(day=1)
    start_dt = datetime(year - 1, month, 1) + timedelta(days=32)
    start_dt = start_dt.replace(day=1)

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    # Conversion by call count (12-month) — consultant-role calls only
    call_rows = query(conn, """
        SELECT
          CASE
            WHEN COALESCE(cc.consultant_calls, 0) = 0 THEN '0 calls'
            WHEN COALESCE(cc.consultant_calls, 0) = 1 THEN '1 call'
            WHEN COALESCE(cc.consultant_calls, 0) = 2 THEN '2 calls'
            ELSE '3+ calls'
          END as bucket,
          COUNT(DISTINCT l.id) as leads,
          COUNT(DISTINCT CASE WHEN l.status = 5 THEN l.id END) as converted,
          ROUND(AVG(CASE WHEN l.status = 5 THEN a.app_value END)) as avg_case
        FROM leads_lead l
        LEFT JOIN (
          SELECT la.object_id, COUNT(*) as consultant_calls
          FROM leads_leadaction la
          WHERE la.object_type = 'lead' AND la.action_type = 'call'
            AND la.deleted = 0
            AND la.user_id IN (
              SELECT user_id FROM account_userrole_users WHERE userrole_id = 2
            )
          GROUP BY la.object_id
        ) cc ON cc.object_id = l.id
        LEFT JOIN applications_application a ON a.lead_id = l.id
        WHERE l.user_id = %s AND l.created >= %s AND l.created < %s
        GROUP BY bucket
        ORDER BY FIELD(bucket, '0 calls', '1 call', '2 calls', '3+ calls')
    """, (user_id, start_str, end_str))

    buckets_4 = ["0 calls", "1 call", "2 calls", "3+ calls"]
    call_row_map = {r["bucket"]: r for r in call_rows}
    total_leads_12m = sum(r["leads"] for r in call_rows)
    conv_by_calls = []
    avg_case_0 = 0
    avg_case_3 = 0
    table_data = []

    # Current open pipeline grouped by call-activity bucket (for TABLE_DATA_10)
    current_pipeline_rows = query(conn, """
        SELECT
          CASE
            WHEN COALESCE(cc.calls, 0) = 0 THEN '0 calls'
            WHEN COALESCE(cc.calls, 0) = 1 THEN '1 call'
            WHEN COALESCE(cc.calls, 0) = 2 THEN '2 calls'
            ELSE '3+ calls'
          END as bucket,
          COUNT(*) as cnt
        FROM leads_lead l
        LEFT JOIN (
          SELECT la.object_id, COUNT(*) as calls
          FROM leads_leadaction la
          WHERE la.object_type = 'lead' AND la.action_type = 'call'
            AND la.deleted = 0
            AND la.user_id IN (
              SELECT user_id FROM account_userrole_users WHERE userrole_id = 2
            )
          GROUP BY la.object_id
        ) cc ON cc.object_id = l.id
        WHERE l.user_id = %s AND l.status NOT IN (5, 6, 7)
          AND l.close_reason_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM applications_application a
            WHERE a.lead_id = l.id AND a.submitted IS NOT NULL
          )
        GROUP BY bucket
    """, (user_id,))
    current_pipeline = {r["bucket"]: int(r["cnt"] or 0) for r in current_pipeline_rows}

    # For the "0 calls" / face-to-face row, only count leads that have been quoted —
    # these have genuine in-person engagement. Leads with 0 calls AND no quote are
    # simply untouched, not face-to-face.
    f2f_pipeline = query(conn, """
        SELECT COUNT(*) as cnt FROM leads_lead l
        WHERE l.user_id = %s AND l.status NOT IN (5, 6, 7)
          AND l.close_reason_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM applications_application a
            WHERE a.lead_id = l.id AND a.submitted IS NOT NULL
          )
          AND NOT EXISTS (
            SELECT 1 FROM leads_leadaction la
            WHERE la.object_id = l.id AND la.object_type = 'lead'
              AND la.action_type = 'call' AND la.deleted = 0
              AND la.user_id IN (SELECT user_id FROM account_userrole_users WHERE userrole_id = 2)
          )
          AND EXISTS (
            SELECT 1 FROM leads_leadquote lq
            WHERE lq.lead_id = l.id AND lq.deleted = 0
          )
    """, (user_id,))[0]["cnt"]
    current_pipeline["0 calls"] = f2f_pipeline

    for b in buckets_4:
        r = call_row_map.get(b)
        if r:
            rate = round(r["converted"] / r["leads"] * 100, 1) if r["leads"] > 0 else 0
            avg_c = int(r["avg_case"] or 0)
        else:
            rate = 0
            avg_c = 0

        conv_by_calls.append(rate)
        if b == "0 calls":
            avg_case_0 = avg_c
        elif b == "3+ calls":
            avg_case_3 = avg_c

        curr_count = current_pipeline.get(b, 0)
        current_str = str(curr_count) if curr_count > 0 else "—"
        table_data.append([b, f"{rate}%", f"${avg_c:,}", current_str])

    # Quoted vs unquoted (12-month)
    qv = query(conn, """
        SELECT
          ROUND(COUNT(DISTINCT CASE WHEN l.status = 5 AND lq.id IS NOT NULL THEN l.id END) /
                NULLIF(COUNT(DISTINCT CASE WHEN lq.id IS NOT NULL THEN l.id END), 0) * 100, 1) as q_conv,
          ROUND(COUNT(DISTINCT CASE WHEN l.status = 5 AND lq.id IS NULL THEN l.id END) /
                NULLIF(COUNT(DISTINCT CASE WHEN lq.id IS NULL THEN l.id END), 0) * 100, 1) as uq_conv
        FROM leads_lead l
        LEFT JOIN leads_leadquote lq ON lq.lead_id = l.id
        WHERE l.user_id = %s AND l.created >= %s AND l.created < %s
    """, (user_id, start_str, end_str))[0]

    quoted_conv = float(qv["q_conv"] or 0)
    unquoted_conv = float(qv["uq_conv"] or 0)

    call_mult = (f"{conv_by_calls[-1] / conv_by_calls[0]:.1f}x"
                 if conv_by_calls[0] > 0 and conv_by_calls[-1] > 0 else "N/A")
    quote_mult = f"{quoted_conv / unquoted_conv:.1f}x" if unquoted_conv > 0 else "N/A"

    # Current pipeline segments for section 11 — consultant-role calls only
    seg_0 = query(conn, """
        SELECT COUNT(*) as cnt FROM leads_lead l
        WHERE l.user_id = %s AND l.status NOT IN (5, 6, 7)
          AND l.close_reason_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM applications_application a
            WHERE a.lead_id = l.id AND a.submitted IS NOT NULL
          )
          AND NOT EXISTS (
            SELECT 1 FROM leads_leadaction la
            WHERE la.object_id = l.id AND la.object_type = 'lead'
              AND la.action_type = 'call' AND la.deleted = 0
              AND la.user_id IN (
                SELECT user_id FROM account_userrole_users WHERE userrole_id = 2
              )
          )
          AND EXISTS (
            SELECT 1 FROM leads_leadquote lq
            WHERE lq.lead_id = l.id AND lq.deleted = 0
          )
    """, (user_id,))[0]["cnt"]

    seg_3plus = query(conn, """
        SELECT COUNT(*) as cnt FROM leads_lead l
        WHERE l.user_id = %s AND l.status NOT IN (5, 6, 7)
          AND l.close_reason_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM applications_application a
            WHERE a.lead_id = l.id AND a.submitted IS NOT NULL
          )
          AND (
            SELECT COUNT(*) FROM leads_leadaction la
            WHERE la.object_id = l.id AND la.object_type = 'lead'
              AND la.action_type = 'call' AND la.deleted = 0
              AND la.user_id IN (
                SELECT user_id FROM account_userrole_users WHERE userrole_id = 2
              )
          ) >= 3
    """, (user_id,))[0]["cnt"]

    quoted_followed = query(conn, """
        SELECT COUNT(*) as cnt FROM leads_lead l
        WHERE l.user_id = %s AND l.status = 3 AND l.close_reason_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM applications_application a
            WHERE a.lead_id = l.id AND a.submitted IS NOT NULL
          )
          AND EXISTS (
            SELECT 1 FROM leads_leadaction la
            WHERE la.object_id = l.id AND la.object_type = 'lead'
              AND la.action_type = 'call' AND la.deleted = 0
              AND la.user_id IN (
                SELECT user_id FROM account_userrole_users WHERE userrole_id = 2
              )
          )
    """, (user_id,))[0]["cnt"]

    # Quoted leads awaiting follow-up (status=3, stale > 5 days, no submitted application)
    stale_quoted = query(conn, """
        SELECT COUNT(*) as cnt FROM leads_lead l
        WHERE l.user_id = %s AND l.status = 3 AND l.close_reason_id IS NULL
          AND l.last_action_time < DATE_SUB(NOW(), INTERVAL 5 DAY)
          AND NOT EXISTS (
            SELECT 1 FROM applications_application a
            WHERE a.lead_id = l.id AND a.submitted IS NOT NULL
          )
    """, (user_id,))[0]["cnt"]

    # Estimated premium for stale quoted leads
    stale_prem_row = query(conn, """
        SELECT ROUND(SUM(lq.total_premium)) as est_prem
        FROM leads_lead l
        LEFT JOIN (
          SELECT lead_id, SUM(value) as total_premium
          FROM leads_leadquote WHERE deleted = 0 GROUP BY lead_id
        ) lq ON lq.lead_id = l.id
        WHERE l.user_id = %s AND l.status = 3 AND l.close_reason_id IS NULL
          AND l.last_action_time < DATE_SUB(NOW(), INTERVAL 5 DAY)
          AND NOT EXISTS (
            SELECT 1 FROM applications_application a
            WHERE a.lead_id = l.id AND a.submitted IS NOT NULL
          )
    """, (user_id,))[0]
    stale_est_prem = int(stale_prem_row["est_prem"] or 0)

    # Weighted average case value across historically converted leads (for pipeline $ estimates)
    total_converted_sum = sum(int(r["converted"] or 0) for r in call_rows)
    total_value_sum = sum(int(r["avg_case"] or 0) * int(r["converted"] or 0) for r in call_rows)
    overall_avg_case = round(total_value_sum / total_converted_sum) if total_converted_sum > 0 else 2000

    pipeline_segments = [
        (f"Leads with\n3+ calls", seg_3plus, f"{conv_by_calls[-1]}%",
         round(seg_3plus * conv_by_calls[-1] / 100 * overall_avg_case / 1000) if conv_by_calls else 0),
        (f"Quoted leads\n(follow-up done)", quoted_followed, f"{quoted_conv}%",
         round(quoted_followed * quoted_conv / 100 * overall_avg_case / 1000)),
        (f"Quoted leads\n(awaiting follow-up)", stale_quoted, f"{quoted_conv}%",
         round(stale_quoted * quoted_conv / 100 * overall_avg_case / 1000)),
        (f"Face-to-face\nleads", seg_0, f"{conv_by_calls[0]}%" if conv_by_calls else "0%",
         round(seg_0 * conv_by_calls[0] / 100 * overall_avg_case / 1000) if conv_by_calls else 0),
    ]

    return {
        "CONV_BY_CALLS_12M": conv_by_calls,
        "QUOTED_VS_UNQUOTED": [unquoted_conv, quoted_conv],
        "CALL_MULTIPLIER": call_mult,
        "QUOTE_MULTIPLIER": quote_mult,
        "TOTAL_LEADS_12M": total_leads_12m,
        "AVG_CASE_0_CALLS": avg_case_0,
        "AVG_CASE_3_PLUS": avg_case_3,
        "TABLE_DATA_10": table_data,
        "PIPELINE_SEGMENTS": pipeline_segments,
        "STALE_QUOTED_COUNT": stale_quoted,
        "STALE_EST_PREMIUM": stale_est_prem,
        "QUOTED_CONV": quoted_conv,
        "UNTOUCHED_LEADS": seg_0,
        "UNTOUCHED_CONV": f"{conv_by_calls[0]}%" if conv_by_calls else "0%",
        "STALE_QUOTES": stale_quoted,
        "STALE_QUOTES_CONV": f"{quoted_conv}%",
        "EST_PIPELINE_VALUE": f"${stale_est_prem:,}",
        "UNQUOTED_CONV": unquoted_conv,
    }


def build_summary(conn, user_id):
    """Section 12: CRM hygiene + closing summary."""
    # Stale appointments (scheduled > 7 days ago, not updated)
    stale_appts = query(conn, """
        SELECT COUNT(*) as cnt FROM leads_leadschedule ls
        JOIN leads_lead l ON l.id = ls.object_id AND ls.object_type = 'lead'
        WHERE l.user_id = %s
          AND ls.date < DATE_SUB(NOW(), INTERVAL 7 DAY)
          AND l.status NOT IN (5, 6, 7)
          AND l.close_reason_id IS NULL
    """, (user_id,))[0]["cnt"]

    return {
        "STALE_APPOINTMENTS": stale_appts,
    }


# ═══════════════════════════════════════════════════════════════════
#  FALLBACK NARRATIVES (when Claude API key is unavailable)
# ═══════════════════════════════════════════════════════════════════

def _generate_fallback_narratives(config):
    """Generate basic data-driven narratives from available config data.
    Used when ANTHROPIC_API_KEY is not set."""
    import calendar as _cal

    prem = config.get("KPI_TOTAL_SUBMITTED_RAW", 0)
    apps = config.get("KPI_APPLICATIONS", 0)
    avg_prem = config.get("KPI_AVG_PREMIUM_RAW", 0)
    month_name = config.get("REPORT_MONTH_NAME", "")
    year = config.get("REPORT_YEAR", "")
    months = config.get("MONTHS_DATA", [])
    is_f2f = config.get("IS_FACE_TO_FACE", False)
    quoted_conv = config.get("QUOTED_CONV", 0)
    stale_count = config.get("STALE_QUOTED_COUNT", 0)
    stale_prem = config.get("STALE_EST_PREMIUM", 0)
    stale_appts = config.get("STALE_APPOINTMENTS", 0)
    total_leads = config.get("TOTAL_LEADS_12M", 0)
    completion_rate = config.get("COMPLETION_RATE", 0)
    untouched = config.get("UNTOUCHED_LEADS", 0)

    # Previous month comparison
    prev_prem = months[-2]["prem"] if len(months) >= 2 else 0
    prev_apps = months[-2]["apps"] if len(months) >= 2 else 0
    total_12m = sum(m.get("prem", 0) for m in months)
    total_12m_apps = sum(m.get("apps", 0) for m in months)

    # Prem direction
    if prev_prem > 0 and prem > prev_prem:
        direction = f"up from <b>${prev_prem:,}</b> last month"
    elif prev_prem > 0 and prem < prev_prem:
        direction = f"down from <b>${prev_prem:,}</b> last month"
    else:
        direction = ""

    narr = {}

    # EXEC_NARRATIVE
    parts = [f"You delivered <b>${prem:,}</b> in premium across <b>{apps} applications</b> in {month_name}."]
    if direction:
        parts.append(f"This is {direction}.")
    if total_12m:
        parts.append(f"Your <b>12-month total of ${total_12m:,}</b> across <b>{total_12m_apps} applications</b> reflects your ongoing production.")
    narr["EXEC_NARRATIVE"] = " ".join(parts)

    # EXEC_DRIVING
    if apps > 0 and avg_prem > 0:
        narr["EXEC_DRIVING"] = f"Your results were driven by <b>{apps} applications</b> at an average case value of <b>${avg_prem:,}</b>."
    else:
        narr["EXEC_DRIVING"] = ""

    # STC_NARRATIVE
    if is_f2f:
        narr["STC_NARRATIVE"] = (
            f"Your face-to-face engagement model continues to show strong results with "
            f"<b>{untouched} leads</b> in your pipeline. "
            f"When you get leads to the quoting stage, they convert at <b>{quoted_conv}%</b>, "
            f"demonstrating the effectiveness of your in-person consultation approach."
        )
        if completion_rate:
            narr["STC_NARRATIVE"] += f" Your <b>{completion_rate}% completion rate</b> reflects solid follow-through."
    else:
        conv_calls = config.get("CONV_BY_CALLS_12M", [])
        mult = config.get("CALL_MULTIPLIER", "")
        if conv_calls and len(conv_calls) >= 4 and conv_calls[3] > 0:
            narr["STC_NARRATIVE"] = (
                f"Your data across <b>{total_leads} leads</b> shows that repeated contact drives results. "
                f"Leads with 3+ calls convert at <b>{conv_calls[3]}%</b> compared to <b>{conv_calls[0]}%</b> for leads with no calls — "
                f"a <b>{mult}</b> improvement."
            )
        else:
            narr["STC_NARRATIVE"] = f"Your engagement data across <b>{total_leads} leads</b> highlights the value of consistent client contact."

    # WHAT_WORKS_INTRO
    narr["WHAT_WORKS_INTRO"] = f"Your conversion data reveals clear patterns about what drives your best results."

    # WHAT_WORKS_NARRATIVE
    if is_f2f:
        narr["WHAT_WORKS_NARRATIVE"] = (
            f"Your face-to-face engagement approach generates consistently strong outcomes across your "
            f"<b>{total_leads} total leads</b> over the past 12 months. "
            f"Your average case value of <b>${avg_prem:,}</b> reflects quality consultations. "
            f"Your <b>{quoted_conv}% conversion rate on quoted leads</b> demonstrates that when prospects "
            f"meet with you in person and receive a quote, they are highly likely to proceed."
        )
    else:
        conv_calls = config.get("CONV_BY_CALLS_12M", [])
        avg0 = config.get("AVG_CASE_0_CALLS", 0)
        avg3 = config.get("AVG_CASE_3_PLUS", 0)
        if conv_calls and len(conv_calls) >= 4:
            narr["WHAT_WORKS_NARRATIVE"] = (
                f"Across <b>{total_leads} leads</b> over 12 months, your data shows that persistent follow-up pays off. "
                f"Leads with 3+ calls convert at <b>{conv_calls[3]}%</b> with an average case value of <b>${avg3:,}</b>, "
                f"compared to <b>{conv_calls[0]}%</b> and <b>${avg0:,}</b> for leads with no calls."
            )
        else:
            narr["WHAT_WORKS_NARRATIVE"] = f"Your data across <b>{total_leads} leads</b> highlights patterns in conversion and engagement."

    # PREDICTOR_NARRATIVE_1
    if quoted_conv > 0:
        approx = round(quoted_conv / 10)
        narr["PREDICTOR_NARRATIVE_1"] = (
            f"Getting to a quote is your strongest predictor of success, with <b>{quoted_conv}% of quoted leads</b> "
            f"converting to applications. This means roughly <b>{approx} out of every 10 prospects</b> who receive "
            f"a quote from you will move forward."
        )
    else:
        narr["PREDICTOR_NARRATIVE_1"] = "Getting leads to the quoting stage is the strongest predictor of conversion in your pipeline."

    # PREDICTOR_NARRATIVE_2_TEMPLATE
    narr["PREDICTOR_NARRATIVE_2_TEMPLATE"] = (
        "You have <b>{stale_count} quoted leads</b> in your pipeline representing approximately "
        "<b>${est_premium:,.0f}</b> in potential premium. "
        f"At your <b>{quoted_conv}% quoted conversion rate</b>, this pipeline could deliver meaningful results with focused follow-up."
    )

    # PREDICTOR_CLOSING
    if is_f2f:
        narr["PREDICTOR_CLOSING"] = (
            "The pattern is clear: deeper engagement through face-to-face meetings and comprehensive quoting "
            "drives your highest conversion rates. Consider applying this thorough approach to your lighter-touch "
            "pipeline segments to unlock similar results."
        )
    else:
        narr["PREDICTOR_CLOSING"] = (
            "The data shows that persistent follow-up and comprehensive quoting are your strongest conversion drivers. "
            "Applying consistent contact to your lighter-touch leads could unlock significant additional premium."
        )

    # CRM_NOTE
    if stale_appts == 0:
        narr["CRM_NOTE"] = (
            "Excellent data hygiene with <b>zero stale appointments</b> in your system. "
            "This clean record-keeping ensures these reports deliver accurate insights."
        )
    else:
        narr["CRM_NOTE"] = (
            f"You have <b>{stale_appts} appointment(s)</b> that could be updated. "
            "Keeping your appointments current ensures these reports deliver the sharpest possible insights."
        )

    # FORMULA_TEXT
    if is_f2f:
        narr["FORMULA_TEXT"] = (
            f"Your winning formula centres on <b>face-to-face engagement</b> and <b>comprehensive quoting</b>. "
            f"The data shows that when you meet prospects in person and provide detailed quotes, "
            f"you convert at an impressive <b>{quoted_conv}% rate</b>."
        )
    else:
        narr["FORMULA_TEXT"] = (
            f"Your winning formula is <b>persistent follow-up</b> and <b>getting leads to a quote</b>. "
            f"Your <b>{quoted_conv}% quoted conversion rate</b> shows the power of thorough engagement."
        )

    # TREND_NARRATIVE
    if months and len(months) >= 3:
        avg_12m = total_12m / len(months) if months else 0
        best_month = max(months, key=lambda m: m.get("prem", 0))
        worst_month = min(months, key=lambda m: m.get("prem", 0))
        import calendar as _cal2
        best_label = f"{_cal2.month_abbr[best_month.get('m', 1)]} {best_month.get('y', '')}"
        worst_label = f"{_cal2.month_abbr[worst_month.get('m', 1)]} {worst_month.get('y', '')}"
        recent_3 = sum(m.get("prem", 0) for m in months[-3:]) / 3
        trend_dir = "upward" if recent_3 > avg_12m else "stable"
        narr["TREND_NARRATIVE"] = (
            f"Over the past 12 months, your trailing average premium sits at <b>${avg_12m:,.0f}/month</b>. "
            f"Your strongest month was <b>{best_label}</b> at <b>${best_month.get('prem', 0):,}</b>, "
            f"while your lightest was <b>{worst_label}</b> at <b>${worst_month.get('prem', 0):,}</b>. "
            f"Your recent 3-month average of <b>${recent_3:,.0f}</b> suggests {trend_dir} momentum heading into the next quarter."
        )
    else:
        narr["TREND_NARRATIVE"] = f"Your <b>${total_12m:,}</b> in total premium reflects your production across the available period."

    # COMPLETION_NARRATIVE
    avg_days = config.get("AVG_DAYS", 0)
    in_progress = config.get("FEB_IN_PROGRESS", 0)
    expected_completions = config.get("EXPECTED_COMPLETIONS", 0)
    expected_prem = config.get("EXPECTED_PREM", 0)
    if completion_rate > 0 and in_progress > 0:
        narr["COMPLETION_NARRATIVE"] = (
            f"Your historical inforce rate of <b>{completion_rate}%</b> — with an average of "
            f"<b>{avg_days} days</b> from submission to inforce — provides a solid foundation for forecasting. "
            f"You currently have <b>{in_progress} in-progress applications</b>, and based on your track record, "
            f"approximately <b>{expected_completions}</b> of these are expected to complete, delivering an estimated "
            f"<b>${expected_prem:,}</b> in paid premium. This pipeline represents tangible revenue on the horizon."
        )
    else:
        narr["COMPLETION_NARRATIVE"] = (
            f"As your inforce history builds, this section will provide increasingly accurate forecasts "
            f"for your in-progress applications."
        )

    # CONCLUSION_NARRATIVE
    parts_conc = [f"In summary, {month_name} saw you deliver <b>${prem:,}</b> in premium across <b>{apps} applications</b>."]
    if prev_prem > 0 and prem > prev_prem:
        parts_conc.append(f"This marks an increase from last month's <b>${prev_prem:,}</b>, showing positive momentum.")
    if quoted_conv > 0:
        parts_conc.append(f"Your <b>{quoted_conv}% quoted conversion rate</b> remains a key strength in your practice.")
    if completion_rate > 0:
        parts_conc.append(f"With a <b>{completion_rate}% inforce rate</b>, your pipeline is well-positioned for follow-through.")
    parts_conc.append(f"Continue leveraging what works — your data shows clear patterns that drive results.")
    narr["CONCLUSION_NARRATIVE"] = " ".join(parts_conc)

    # HIGHLIGHTS
    highlights = []
    if prev_apps > 0 and apps > prev_apps:
        highlights.append(f"Application volume increased from {prev_apps} to {apps} month-over-month")
    elif apps > 0:
        highlights.append(f"{apps} applications submitted in {month_name}")
    if avg_prem > 0:
        highlights.append(f"${avg_prem:,} average case value maintained")
    if quoted_conv > 0:
        highlights.append(f"{quoted_conv}% conversion rate on quoted leads")
    if stale_appts == 0:
        highlights.append("Excellent CRM hygiene with zero stale appointments")
    elif completion_rate > 0:
        highlights.append(f"{completion_rate}% completion rate on submitted applications")
    narr["HIGHLIGHTS"] = highlights[:4]

    # MILESTONE / CALLOUT
    narr["SHOW_MILESTONE"] = prem >= 100000
    if narr["SHOW_MILESTONE"]:
        narr["MILESTONE_TEXT"] = f"${prem // 1000}K MONTH — ACHIEVED"
        narr["MILESTONE_SUB"] = f"Congratulations on a standout {month_name} with <b>${prem:,}</b> in submitted premium."
    else:
        narr["MILESTONE_TEXT"] = ""
        narr["MILESTONE_SUB"] = ""

    # CALLOUT for sub-$100k months
    if not narr["SHOW_MILESTONE"]:
        if prev_prem > 0 and apps > prev_apps:
            narr["CALLOUT_TEXT"] = "VOLUME RECOVERY MONTH"
            next_m = _cal.month_name[(config.get("REPORT_MONTH", 1) % 12) + 1]
            narr["CALLOUT_SUB"] = f"Your <b>{apps} applications</b> this month signal strong momentum heading into {next_m}."
        elif avg_prem >= 3000:
            narr["CALLOUT_TEXT"] = "STRONG CASE VALUE"
            narr["CALLOUT_SUB"] = f"Your <b>${avg_prem:,} average case value</b> reflects quality consultations."
        elif apps > 0:
            narr["CALLOUT_TEXT"] = f"{apps} APPLICATIONS SUBMITTED"
            narr["CALLOUT_SUB"] = f"Your <b>${prem:,}</b> in premium this {month_name} continues your production."
        else:
            narr["CALLOUT_TEXT"] = ""
            narr["CALLOUT_SUB"] = ""
    else:
        narr["CALLOUT_TEXT"] = ""
        narr["CALLOUT_SUB"] = ""

    return narr


def _apply_narrative_fallbacks(config):
    """Ensure all narrative keys exist; use data-driven fallback if AI narratives are empty."""
    narrative_keys = [
        "EXEC_NARRATIVE", "EXEC_DRIVING", "TREND_NARRATIVE",
        "STC_NARRATIVE",
        "WHAT_WORKS_INTRO", "WHAT_WORKS_NARRATIVE",
        "PREDICTOR_NARRATIVE_1", "PREDICTOR_NARRATIVE_2_TEMPLATE",
        "PREDICTOR_CLOSING", "CRM_NOTE", "FORMULA_TEXT",
        "COMPLETION_NARRATIVE", "CONCLUSION_NARRATIVE",
        "HIGHLIGHTS", "SHOW_MILESTONE", "MILESTONE_TEXT", "MILESTONE_SUB",
        "CALLOUT_TEXT", "CALLOUT_SUB",
    ]

    # Check if any key narrative is missing
    needs_fallback = any(
        k not in config or not config[k]
        for k in ["EXEC_NARRATIVE", "HIGHLIGHTS", "PREDICTOR_NARRATIVE_1"]
    )

    if needs_fallback:
        fallbacks = _generate_fallback_narratives(config)
        for k in narrative_keys:
            if k not in config or not config[k]:
                config[k] = fallbacks.get(k, "" if k != "HIGHLIGHTS" else [])

    # Ensure all keys exist
    for k in narrative_keys:
        if k not in config:
            config[k] = "" if k not in ("HIGHLIGHTS",) else []

    # Enforce milestone rule
    config["SHOW_MILESTONE"] = config.get("KPI_TOTAL_SUBMITTED_RAW", 0) >= 100000
    if not config["SHOW_MILESTONE"]:
        config["MILESTONE_TEXT"] = ""
        config["MILESTONE_SUB"] = ""

    return config


# ═══════════════════════════════════════════════════════════════════
#  MAIN: BUILD + WRITE CONFIG
# ═══════════════════════════════════════════════════════════════════

def build_all(user_id, month, year, conn=None, api_key=None):
    """Run all section queries and return combined config dict.

    Args:
        user_id: adviser user ID
        month: report month (1-12)
        year: report year
        conn: optional MySQL connection (will create one if None)
        api_key: optional Anthropic API key for narrative generation
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        config = {}
        config.update(build_identity(conn, user_id, month, year))
        config.update(build_12month_performance(conn, user_id, month, year))
        config.update(build_benchmarking(conn, user_id, month, year))
        config.update(build_referral_partners(conn, user_id, month, year))

        insurer_data = build_insurers_and_submissions(conn, user_id, month, year)
        config["INSURERS"] = insurer_data["INSURERS"]
        config["APPS"] = insurer_data["APPS"]
        config["SUBMISSIONS_FOOTNOTE"] = insurer_data["SUBMISSIONS_FOOTNOTE"]

        config.update(build_speed_to_contact(conn, user_id, month, year))
        config.update(build_completion_forecast(conn, user_id, month, year))
        config.update(build_quoted_pipeline(conn, user_id))
        config.update(build_conversion_drivers(conn, user_id, month, year))
        config.update(build_summary(conn, user_id))

        has_page6 = len(config.get("APPS", [])) > 10
        config["HAS_PAGE6"] = has_page6
        config["TOTAL_PAGES"] = 13 if has_page6 else 12

        # Generate AI narratives (or leave as placeholders)
        from generate_narratives import enrich_config_with_narratives
        config = enrich_config_with_narratives(config, api_key=api_key)

        # Fallback: generate data-driven narratives if AI narratives are empty
        config = _apply_narrative_fallbacks(config)

        return config

    finally:
        if own_conn:
            conn.close()


def write_config(config, output_path="report_config.py"):
    """Write config dict as a valid Python module."""
    lines = [
        '"""',
        'report_config.py',
        
        
        'Auto-generated by build_config.py — DO NOT EDIT MANUALLY.',
        f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '"""',
        '',
        'from decimal import Decimal',
        '',
    ]

    # Group by section for readability
    sections = {
        "IDENTITY": ["ADVISER_NAME", "PRACTICE_NAME", "REPORT_DATE", "REPORT_MONTH",
                      "REPORT_YEAR", "REPORT_MONTH_NAME", "TOTAL_PAGES", "HAS_PAGE6"],
        "SECTION 1 & 2: 12-MONTH PERFORMANCE": [
            "MONTHS_DATA", "KPI_TOTAL_SUBMITTED", "KPI_TOTAL_SUBMITTED_RAW",
            "KPI_APPLICATIONS", "KPI_AVG_PREMIUM", "KPI_AVG_PREMIUM_RAW",
            "KPI_TOTAL_SUB_LABEL", "KPI_APPS_LABEL", "KPI_AVG_LABEL",
            "EXEC_NARRATIVE", "EXEC_DRIVING", "TREND_NARRATIVE"],
        "SECTION 3: BENCHMARKING": [
            "ADVISER_PREMIUM_12M", "ADVISER_RANK", "TOTAL_PRACTICES",
            "PERCENTILE", "NETWORK_AVG_PREM", "MEDIAN_PREM", "TOP_QUARTILE_PREM",
            "PREM_Q1", "PREM_Q2", "PREM_Q3", "BENCH_PERIOD", "HIST_DATA"],
        "SECTION 4: REFERRAL PARTNERS": ["PARTNERS", "PARTNER_GROUPS"],
        "SECTION 5/6: INSURERS + SUBMISSIONS": [
            "INSURERS", "APPS", "SUBMISSIONS_FOOTNOTE"],
        "SECTION 7: SPEED-TO-CONTACT": [
            "CALL_BUCKETS", "CONV_RATES", "AVG_CASE_VALUES", "TOTAL_LEADS_STC",
            "STC_PERIOD", "QUOTED_CONV_RATE_STC", "UNQUOTED_CONV_RATE_STC",
            "STC_NARRATIVE", "IS_FACE_TO_FACE"],
        "SECTION 8: COMPLETION FORECAST": [
            "COMPLETION_BUCKETS", "PER_PERIOD_PCT", "CUMULATIVE_PCT",
            "TOTAL_COMPLETED", "TOTAL_SUBMITTED_HIST", "COMPLETION_RATE",
            "AVG_DAYS", "FEB_IN_PROGRESS", "FEB_IP_PREMIUM",
            "FEB_INFORCED_PREM", "EXPECTED_COMPLETIONS", "EXPECTED_PREM",
            "TOTAL_FORECAST", "COMPLETION_NARRATIVE"],
        "SECTION 9: QUOTED PIPELINE": ["PIPELINE"],
        "SECTION 10: WHAT WORKS": [
            "CONV_BY_CALLS_12M", "QUOTED_VS_UNQUOTED", "CALL_MULTIPLIER",
            "QUOTE_MULTIPLIER", "TOTAL_LEADS_12M", "AVG_CASE_0_CALLS",
            "AVG_CASE_3_PLUS", "TABLE_DATA_10",
            "WHAT_WORKS_INTRO", "WHAT_WORKS_NARRATIVE"],
        "SECTION 11: STRONGEST PREDICTOR": [
            "PIPELINE_SEGMENTS", "STALE_QUOTED_COUNT", "STALE_EST_PREMIUM",
            "QUOTED_CONV", "UNQUOTED_CONV",
            "PREDICTOR_NARRATIVE_1", "PREDICTOR_NARRATIVE_2_TEMPLATE",
            "PREDICTOR_CLOSING"],
	"SECTION 12: SUMMARY + MILESTONE": [
            "STALE_APPOINTMENTS", "CRM_NOTE", "FORMULA_TEXT",
            "HIGHLIGHTS", "SHOW_MILESTONE", "MILESTONE_TEXT", "MILESTONE_SUB",
            "CALLOUT_TEXT", "CALLOUT_SUB", "CONCLUSION_NARRATIVE",
            "UNTOUCHED_LEADS", "UNTOUCHED_CONV", "STALE_QUOTES",
            "STALE_QUOTES_CONV", "EST_PIPELINE_VALUE"],
    }

    # Also add derived values that section files expect
    # (UNTOUCHED_LEADS, UNTOUCHED_CONV, etc. are derived from other fields)

    for section_name, keys in sections.items():
        lines.append(f"# {'═' * 47}")
        lines.append(f"#  {section_name}")
        lines.append(f"# {'═' * 47}")
        for k in keys:
            if k in config:
                lines.append(f"{k} = {repr(config[k])}")
        lines.append("")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    print(f"\u2705 Config written to {output_path}")


# ═══════════════════════════════════════════════════════════════════
#  PRACTICE-LEVEL (AGGREGATE) CONFIG
# ═══════════════════════════════════════════════════════════════════

def _uid_in(user_ids):
    """Return (placeholders_string, params_tuple) for SQL IN clause."""
    ph = ",".join(["%s"] * len(user_ids))
    return ph, tuple(user_ids)


def _real_adviser_ids(conn):
    """Return set of user_ids who are genuine advisers (in ≤3 real practice groups).
    Admin/licensee accounts typically appear in 10+ groups; real advisers appear in 1–2.
    """
    rows = query(conn, """
        SELECT ugu.user_id
        FROM account_usergroup_users ugu
        JOIN account_usergroup ug ON ug.id = ugu.usergroup_id AND ug.real = 1 AND ug.is_active = 1
        JOIN auth_user au ON au.id = ugu.user_id AND au.is_active = 1
        WHERE ugu.user_id NOT IN (88, 118, 172)
        GROUP BY ugu.user_id
        HAVING COUNT(DISTINCT ug.id) <= 3
    """)
    return {r["user_id"] for r in rows}


def get_practice_user_ids(conn, practice_name):
    """Return active adviser user_ids for a practice (excludes admin/multi-group accounts)."""
    adviser_ids = _real_adviser_ids(conn)
    rows = query(conn, """
        SELECT DISTINCT ugu.user_id
        FROM account_usergroup_users ugu
        JOIN account_usergroup ug ON ug.id = ugu.usergroup_id
            AND ug.real = 1 AND ug.is_active = 1 AND ug.name = %s
        JOIN auth_user au ON au.id = ugu.user_id AND au.is_active = 1
        WHERE ugu.user_id NOT IN (88, 118, 172)
        ORDER BY ugu.user_id
    """, (practice_name,))
    return [r["user_id"] for r in rows if r["user_id"] in adviser_ids]


def get_all_practices(conn):
    """Return list of (name, [user_ids]) for all active practices with 2+ real advisers."""
    adviser_ids = _real_adviser_ids(conn)
    rows = query(conn, """
        SELECT ug.name, ugu.user_id
        FROM account_usergroup_users ugu
        JOIN account_usergroup ug ON ug.id = ugu.usergroup_id
            AND ug.real = 1 AND ug.is_active = 1
        JOIN auth_user au ON au.id = ugu.user_id AND au.is_active = 1
        WHERE ugu.user_id NOT IN (88, 118, 172)
        ORDER BY ug.name, ugu.user_id
    """)
    practices = {}
    for r in rows:
        if r["user_id"] in adviser_ids:
            practices.setdefault(r["name"], []).append(r["user_id"])
    return [(name, ids) for name, ids in practices.items() if len(ids) >= 2]


def build_all_practice(practice_name, month, year, conn=None, api_key=None):
    """Build aggregated report config for all advisers under a practice."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        user_ids = get_practice_user_ids(conn, practice_name)
        if not user_ids:
            raise ValueError(f"No active advisers found for practice: {practice_name!r}")

        ph, uid_params = _uid_in(user_ids)

        # ── Date windows ──
        import calendar as cal
        month_name = calendar.month_name[month]
        last_day = cal.monthrange(year, month)[1]
        report_date = f"{last_day} {month_name} {year}"

        end_dt = datetime(year, month, 1) + timedelta(days=32)
        end_dt = end_dt.replace(day=1)
        start_12m = datetime(year - 1, month, 1) + timedelta(days=32)
        start_12m = start_12m.replace(day=1)
        start_str_12m = start_12m.strftime("%Y-%m-%d")
        end_str_12m   = end_dt.strftime("%Y-%m-%d")
        start_str_cur = f"{year}-{month:02d}-01"
        end_str_cur   = end_dt.strftime("%Y-%m-%d")

        config = {
            "ADVISER_NAME":      practice_name,
            "PRACTICE_NAME":     practice_name,
            "REPORT_DATE":       report_date,
            "REPORT_MONTH":      month,
            "REPORT_YEAR":       year,
            "REPORT_MONTH_NAME": month_name,
            "TOTAL_PAGES":       13,
            "HAS_PAGE6":         True,
        }

        # ── Sections 1 & 2: 12-month performance ──
        perf_rows = query(conn, f"""
            SELECT YEAR(submitted) AS y, MONTH(submitted) AS m,
                   COUNT(*) AS apps, ROUND(SUM(premium)) AS prem
            FROM applications_application
            WHERE adviser_id IN ({ph})
              AND submitted >= %s AND submitted < %s
            GROUP BY YEAR(submitted), MONTH(submitted)
            ORDER BY y, m
        """, uid_params + (start_str_12m, end_str_12m))

        months_data = [{"y": r["y"], "m": r["m"], "apps": r["apps"], "prem": int(r["prem"] or 0)} for r in perf_rows]
        current = next((m for m in months_data if m["y"] == year and m["m"] == month),
                       {"y": year, "m": month, "apps": 0, "prem": 0})
        total_prem = current["prem"]
        total_apps = current["apps"]
        avg_prem   = round(total_prem / total_apps) if total_apps > 0 else 0
        prior_months     = [m for m in months_data if not (m["y"] == year and m["m"] == month)]
        best_prior_prem  = max((m["prem"] for m in prior_months), default=0)
        best_prior_apps  = max((m["apps"] for m in prior_months), default=0)
        prem_label = ""
        if total_prem > best_prior_prem and total_prem > 0:
            prem_label = "Practice best"
            if total_prem >= 100000:
                prem_label += " \u2013 $100K+ milestone"
        apps_label = "Highest volume month" if total_apps > best_prior_apps else ""
        avg_label  = ""
        if total_apps > 0:
            prior_avgs = [m["prem"] / m["apps"] for m in prior_months if m["apps"] > 0]
            if prior_avgs and avg_prem > max(prior_avgs):
                avg_label = "Highest ever average"
        config.update({
            "MONTHS_DATA":             months_data,
            "KPI_TOTAL_SUBMITTED":     f"${total_prem:,}",
            "KPI_TOTAL_SUBMITTED_RAW": total_prem,
            "KPI_APPLICATIONS":        total_apps,
            "KPI_AVG_PREMIUM":         f"${avg_prem:,}",
            "KPI_AVG_PREMIUM_RAW":     avg_prem,
            "KPI_TOTAL_SUB_LABEL":     prem_label,
            "KPI_APPS_LABEL":          apps_label,
            "KPI_AVG_LABEL":           avg_label,
        })

        # ── Section 3: Benchmarking ──
        # Get network data via first adviser, then substitute practice combined total
        bench = build_benchmarking(conn, user_ids[0], month, year)
        practice_12m = sum(m["prem"] for m in months_data)
        all_prems = list(bench.get("HIST_DATA", {}).values())  # not ideal but sufficient
        # Re-run all_practices to get proper rank
        all_p = query(conn, """
            SELECT a.adviser_id, ROUND(SUM(a.premium)) AS total_prem
            FROM applications_application a
            WHERE a.submitted >= %s AND a.submitted < %s
              AND a.adviser_id IN (
                SELECT DISTINCT ugu.user_id FROM account_usergroup_users ugu
                JOIN account_usergroup ug ON ug.id=ugu.usergroup_id AND ug.real=1 AND ug.is_active=1
                JOIN auth_user au ON au.id=ugu.user_id AND au.is_active=1
                WHERE ugu.user_id NOT IN (88, 118, 172)
              )
            GROUP BY a.adviser_id
        """, (start_str_12m, end_str_12m))
        all_prems_list = [int(r["total_prem"] or 0) for r in all_p]
        total_n = bench["TOTAL_PRACTICES"]
        zero_count = total_n - len(all_prems_list)
        all_prems_list.extend([0] * max(zero_count, 0))
        all_prems_list.sort(reverse=True)
        practice_rank = sum(1 for p in all_prems_list if p > practice_12m) + 1
        practice_pctile = round((1 - practice_rank / total_n) * 100) if total_n > 0 else 0
        bench["ADVISER_PREMIUM_12M"] = practice_12m
        bench["ADVISER_RANK"]        = practice_rank
        bench["PERCENTILE"]          = practice_pctile
        config.update(bench)

        # ── Section 4: Referral partners — merge per-adviser results ──
        all_partners = {}
        all_groups   = {}
        for uid in user_ids:
            p_result = build_referral_partners(conn, uid, month, year)
            for p in p_result["PARTNERS"]:
                key = (p["group"], p["name"])
                if key in all_partners:
                    all_partners[key]["leads"] += p["leads"]
                    all_partners[key]["apps"]  += p["apps"]
                    all_partners[key]["prem"]  += p["prem"]
                else:
                    all_partners[key] = dict(p)
            for g in p_result["PARTNER_GROUPS"]:
                gk = g["name"]
                if gk in all_groups:
                    all_groups[gk]["leads"] += g["leads"]
                    all_groups[gk]["apps"]  += g["apps"]
                    all_groups[gk]["prem"]  += g["prem"]
                else:
                    all_groups[gk] = dict(g)
        partners_list = sorted(all_partners.values(), key=lambda x: -x["leads"])
        for p in partners_list:
            p["conv"] = round(p["apps"] / p["leads"] * 100) if p["leads"] > 0 else 0
        groups_list = sorted(all_groups.values(), key=lambda x: -x["leads"])
        for g in groups_list:
            g["conv"] = round(g["apps"] / g["leads"] * 100) if g["leads"] > 0 else 0
        config.update({"PARTNERS": partners_list, "PARTNER_GROUPS": groups_list})

        # ── Sections 5 & 6: Insurers + submissions ──
        apps_raw = query(conn, f"""
            SELECT a.customer_name, ROUND(a.premium) as premium, a.company_name as insurer,
                   a.status, a.submitted, a.commenced, a.life, a.tpd, a.trauma, a.ip
            FROM applications_application a
            WHERE a.adviser_id IN ({ph})
              AND a.submitted >= %s AND a.submitted < %s
            ORDER BY a.premium DESC
        """, uid_params + (start_str_cur, end_str_cur))
        status_map = {0: "In Progress", 4: "Inforced", 5: "Completed", 6: "Cancelled"}
        insurer_counts = {}
        apps_list = []
        for a in apps_raw:
            insurer = a["insurer"] or "Unknown"
            insurer_counts[insurer] = insurer_counts.get(insurer, 0) + 1
            products = [x for x in ["Life", "TPD", "Trauma", "IP"] if a[x.lower()]]
            name = (a["customer_name"] or "").strip()
            for prefix in ["Mr ", "Mrs ", "Ms ", "Miss ", "Dr "]:
                if name.startswith(prefix):
                    name = name[len(prefix):]
            sub_date = a["submitted"]
            apps_list.append({
                "client":   " ".join(name.split()),
                "prem":     int(a["premium"]) if a["premium"] else "TBC",
                "insurer":  insurer,
                "status":   status_map.get(int(a["status"] or 0), "Unknown"),
                "products": ", ".join(products),
                "date":     sub_date.strftime("%-d %b") if sub_date else "",
                "green":    False,
            })
        config.update({
            "INSURERS":             sorted(insurer_counts.items(), key=lambda x: -x[1]),
            "APPS":                 apps_list,
            "SUBMISSIONS_FOOTNOTE": "",
        })

        # ── Section 7: Speed-to-contact ──
        stc_rows = query(conn, f"""
            SELECT
              CASE
                WHEN COALESCE(cc.consultant_calls, 0) = 0 THEN '0 calls'
                WHEN COALESCE(cc.consultant_calls, 0) = 1 THEN '1 call'
                WHEN COALESCE(cc.consultant_calls, 0) = 2 THEN '2 calls'
                ELSE '3+ calls'
              END as bucket,
              COUNT(DISTINCT l.id) as leads,
              COUNT(DISTINCT CASE WHEN l.status = 5 THEN l.id END) as converted,
              ROUND(AVG(CASE WHEN l.status = 5 THEN a.app_value END)) as avg_case
            FROM leads_lead l
            LEFT JOIN (
              SELECT la.object_id, COUNT(*) as consultant_calls
              FROM leads_leadaction la
              WHERE la.object_type = 'lead' AND la.action_type = 'call'
                AND la.deleted = 0
                AND la.user_id IN (SELECT user_id FROM account_userrole_users WHERE userrole_id = 2)
              GROUP BY la.object_id
            ) cc ON cc.object_id = l.id
            LEFT JOIN applications_application a ON a.lead_id = l.id
            WHERE l.user_id IN ({ph}) AND l.created >= %s AND l.created < %s
            GROUP BY bucket
            ORDER BY FIELD(bucket, '0 calls', '1 call', '2 calls', '3+ calls')
        """, uid_params + (start_str_12m, end_str_12m))
        buckets = ["0 calls", "1 call", "2 calls", "3+ calls"]
        stc_map = {r["bucket"]: r for r in stc_rows}
        conv_rates, avg_values, bucket_counts = [], [], []
        total_leads_stc = 0
        for b in buckets:
            r = stc_map.get(b)
            if r:
                leads = r["leads"]
                rate  = round(r["converted"] / leads * 100, 1) if leads > 0 else 0
                conv_rates.append(rate)
                avg_values.append(int(r["avg_case"] or 0))
                total_leads_stc += leads
                bucket_counts.append(leads)
            else:
                conv_rates.append(0); avg_values.append(0); bucket_counts.append(0)
        leads_with_calls = sum(bucket_counts[1:])
        is_f2f = total_leads_stc > 10 and leads_with_calls / total_leads_stc < 0.05
        quoted_stc = query(conn, f"""
            SELECT
              ROUND(COUNT(DISTINCT CASE WHEN l.status = 5 AND lq.id IS NOT NULL THEN l.id END) /
                    NULLIF(COUNT(DISTINCT CASE WHEN lq.id IS NOT NULL THEN l.id END), 0) * 100, 1) as quoted_conv,
              ROUND(COUNT(DISTINCT CASE WHEN l.status = 5 AND lq.id IS NULL THEN l.id END) /
                    NULLIF(COUNT(DISTINCT CASE WHEN lq.id IS NULL THEN l.id END), 0) * 100, 1) as unquoted_conv
            FROM leads_lead l
            LEFT JOIN leads_leadquote lq ON lq.lead_id = l.id
            WHERE l.user_id IN ({ph}) AND l.created >= %s AND l.created < %s
        """, uid_params + (start_str_12m, end_str_12m))[0]
        config.update({
            "CALL_BUCKETS":          buckets,
            "CONV_RATES":            conv_rates,
            "AVG_CASE_VALUES":       avg_values,
            "TOTAL_LEADS_STC":       total_leads_stc,
            "STC_PERIOD":            "12 months",
            "QUOTED_CONV_RATE_STC":  float(quoted_stc["quoted_conv"] or 0),
            "UNQUOTED_CONV_RATE_STC":float(quoted_stc["unquoted_conv"] or 0),
            "IS_FACE_TO_FACE":       is_f2f,
        })

        # ── Section 8: Completion forecast ──
        all_apps_hist = query(conn, f"""
            SELECT a.submitted, a.commenced, a.status
            FROM applications_application a
            WHERE a.adviser_id IN ({ph}) AND a.submitted IS NOT NULL
        """, uid_params)

        def _as_dt(val):
            from datetime import date
            if isinstance(val, datetime): return val
            if isinstance(val, date): return datetime(val.year, val.month, val.day)
            return None

        report_end      = end_dt
        eligible_cutoff = report_end - timedelta(days=120)
        eligible_submitted = sum(1 for a in all_apps_hist if _as_dt(a["submitted"]) and _as_dt(a["submitted"]) < eligible_cutoff)
        eligible_completed = sum(1 for a in all_apps_hist if int(a["status"] or 0) == 4 and _as_dt(a["submitted"]) and _as_dt(a["submitted"]) < eligible_cutoff)
        completion_rate = round(eligible_completed / eligible_submitted * 100) if eligible_submitted > 0 else 0

        week_buckets = {"Week 1": 0, "Week 2": 0, "Week 3": 0, "Week 4": 0, "Month 2": 0, "60+ days": 0}
        days_list = []
        for a in all_apps_hist:
            c_dt = _as_dt(a["commenced"]); s_dt = _as_dt(a["submitted"])
            if c_dt and s_dt:
                days = (c_dt - s_dt).days
                if days >= 0: days_list.append(days)
                if days <= 7: week_buckets["Week 1"] += 1
                elif days <= 14: week_buckets["Week 2"] += 1
                elif days <= 21: week_buckets["Week 3"] += 1
                elif days <= 28: week_buckets["Week 4"] += 1
                elif days <= 60: week_buckets["Month 2"] += 1
                else: week_buckets["60+ days"] += 1
        total_dated = sum(week_buckets.values())
        per_period, cumulative, running = [], [], 0
        for lbl in ["Week 1", "Week 2", "Week 3", "Week 4", "Month 2", "60+ days"]:
            pct = round(week_buckets[lbl] / total_dated * 100) if total_dated > 0 else 0
            per_period.append(pct); running += pct; cumulative.append(min(running, 100))

        ip_row = query(conn, f"""
            SELECT COUNT(*) as cnt, ROUND(SUM(premium)) as prem
            FROM applications_application
            WHERE adviser_id IN ({ph}) AND submitted >= %s AND submitted < %s
              AND commenced IS NULL AND status = 0
        """, uid_params + (start_str_cur, end_str_cur))[0]
        comm_row = query(conn, f"""
            SELECT ROUND(SUM(premium)) as prem
            FROM applications_application
            WHERE adviser_id IN ({ph}) AND submitted >= %s AND submitted < %s AND status = 4
        """, uid_params + (start_str_cur, end_str_cur))[0]
        feb_ip       = int(ip_row["cnt"] or 0)
        feb_ip_prem  = int(ip_row["prem"] or 0)
        feb_comm_prem = int(comm_row["prem"] or 0)
        expected_completions = round(feb_ip * completion_rate / 100)
        expected_prem = round(feb_ip_prem * completion_rate / 100 / 1000) * 1000
        config.update({
            "COMPLETION_BUCKETS":  ["Week 1", "Week 2", "Week 3", "Week 4", "Month 2", "60+ days"],
            "PER_PERIOD_PCT":      per_period,
            "CUMULATIVE_PCT":      cumulative,
            "TOTAL_COMPLETED":     eligible_completed,
            "TOTAL_SUBMITTED_HIST":eligible_submitted,
            "TOTAL_DATED":         total_dated,
            "COMPLETION_RATE":     completion_rate,
            "AVG_DAYS":            round(statistics.mean(days_list)) if days_list else 0,
            "FEB_IN_PROGRESS":     feb_ip,
            "FEB_IP_PREMIUM":      feb_ip_prem,
            "FEB_INFORCED_PREM":   feb_comm_prem,
            "EXPECTED_COMPLETIONS":expected_completions,
            "EXPECTED_PREM":       expected_prem,
            "TOTAL_FORECAST":      feb_comm_prem + expected_prem,
        })

        # ── Section 9: Quoted pipeline ──
        pipeline_rows = query(conn, f"""
            SELECT CONCAT(l.first_name, ' ', l.last_name) as client,
                   ROUND(COALESCE(lq.last_premium, 0)) as last_quoted,
                   ls.name as source, l.calls_made
            FROM leads_lead l
            LEFT JOIN (
              SELECT lqq.lead_id, ROUND(lqq.value) as last_premium
              FROM leads_leadquote lqq
              INNER JOIN (
                SELECT lead_id, MAX(created) as latest FROM leads_leadquote WHERE deleted = 0 GROUP BY lead_id
              ) mx ON mx.lead_id = lqq.lead_id AND mx.latest = lqq.created
              WHERE lqq.deleted = 0
            ) lq ON lq.lead_id = l.id
            LEFT JOIN leads_leadsource ls ON ls.id = l.source_id
            WHERE l.user_id IN ({ph}) AND l.status = 3 AND l.close_reason_id IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM applications_application a
                WHERE a.lead_id = l.id AND a.submitted IS NOT NULL
              )
            ORDER BY ISNULL(lq.last_premium), lq.last_premium DESC
            LIMIT 30
        """, uid_params)
        config["PIPELINE"] = [
            {"client": " ".join(r["client"].split()), "last_quoted": int(r["last_quoted"] or 0),
             "source": r["source"] or "Other", "calls_made": int(r["calls_made"] or 0),
             "f2f": int(r["calls_made"] or 0) == 0}
            for r in pipeline_rows
        ]

        # ── Sections 10 & 11: Conversion drivers ──
        call_rows = query(conn, f"""
            SELECT
              CASE
                WHEN COALESCE(cc.consultant_calls, 0) = 0 THEN '0 calls'
                WHEN COALESCE(cc.consultant_calls, 0) = 1 THEN '1 call'
                WHEN COALESCE(cc.consultant_calls, 0) = 2 THEN '2 calls'
                ELSE '3+ calls'
              END as bucket,
              COUNT(DISTINCT l.id) as leads,
              COUNT(DISTINCT CASE WHEN l.status = 5 THEN l.id END) as converted,
              ROUND(AVG(CASE WHEN l.status = 5 THEN a.app_value END)) as avg_case
            FROM leads_lead l
            LEFT JOIN (
              SELECT la.object_id, COUNT(*) as consultant_calls
              FROM leads_leadaction la
              WHERE la.object_type = 'lead' AND la.action_type = 'call'
                AND la.deleted = 0
                AND la.user_id IN (SELECT user_id FROM account_userrole_users WHERE userrole_id = 2)
              GROUP BY la.object_id
            ) cc ON cc.object_id = l.id
            LEFT JOIN applications_application a ON a.lead_id = l.id
            WHERE l.user_id IN ({ph}) AND l.created >= %s AND l.created < %s
            GROUP BY bucket
            ORDER BY FIELD(bucket, '0 calls', '1 call', '2 calls', '3+ calls')
        """, uid_params + (start_str_12m, end_str_12m))
        buckets_4    = ["0 calls", "1 call", "2 calls", "3+ calls"]
        call_row_map = {r["bucket"]: r for r in call_rows}
        total_leads_12m = sum(r["leads"] for r in call_rows)
        conv_by_calls, avg_case_0, avg_case_3 = [], 0, 0
        curr_pipeline_rows = query(conn, f"""
            SELECT
              CASE
                WHEN COALESCE(cc.calls, 0) = 0 THEN '0 calls'
                WHEN COALESCE(cc.calls, 0) = 1 THEN '1 call'
                WHEN COALESCE(cc.calls, 0) = 2 THEN '2 calls'
                ELSE '3+ calls'
              END as bucket, COUNT(*) as cnt
            FROM leads_lead l
            LEFT JOIN (
              SELECT la.object_id, COUNT(*) as calls
              FROM leads_leadaction la
              WHERE la.object_type = 'lead' AND la.action_type = 'call'
                AND la.deleted = 0
                AND la.user_id IN (SELECT user_id FROM account_userrole_users WHERE userrole_id = 2)
              GROUP BY la.object_id
            ) cc ON cc.object_id = l.id
            WHERE l.user_id IN ({ph}) AND l.status NOT IN (5, 6, 7)
              AND l.close_reason_id IS NULL
              AND NOT EXISTS (SELECT 1 FROM applications_application a WHERE a.lead_id = l.id AND a.submitted IS NOT NULL)
            GROUP BY bucket
        """, uid_params)
        current_pipeline = {r["bucket"]: int(r["cnt"] or 0) for r in curr_pipeline_rows}
        f2f_row = query(conn, f"""
            SELECT COUNT(*) as cnt FROM leads_lead l
            WHERE l.user_id IN ({ph}) AND l.status NOT IN (5, 6, 7) AND l.close_reason_id IS NULL
              AND NOT EXISTS (SELECT 1 FROM applications_application a WHERE a.lead_id = l.id AND a.submitted IS NOT NULL)
              AND NOT EXISTS (SELECT 1 FROM leads_leadaction la WHERE la.object_id = l.id AND la.object_type = 'lead' AND la.action_type = 'call' AND la.deleted = 0 AND la.user_id IN (SELECT user_id FROM account_userrole_users WHERE userrole_id = 2))
              AND EXISTS (SELECT 1 FROM leads_leadquote lq WHERE lq.lead_id = l.id AND lq.deleted = 0)
        """, uid_params)[0]
        current_pipeline["0 calls"] = f2f_row["cnt"]

        table_data = []
        for b in buckets_4:
            r = call_row_map.get(b)
            if r:
                rate  = round(r["converted"] / r["leads"] * 100, 1) if r["leads"] > 0 else 0
                avg_c = int(r["avg_case"] or 0)
            else:
                rate, avg_c = 0, 0
            conv_by_calls.append(rate)
            if b == "0 calls":   avg_case_0 = avg_c
            elif b == "3+ calls": avg_case_3 = avg_c
            curr = current_pipeline.get(b, 0)
            table_data.append([b, f"{rate}%", f"${avg_c:,}", str(curr) if curr > 0 else "—"])

        qv_row = query(conn, f"""
            SELECT
              ROUND(COUNT(DISTINCT CASE WHEN l.status = 5 AND lq.id IS NOT NULL THEN l.id END) /
                    NULLIF(COUNT(DISTINCT CASE WHEN lq.id IS NOT NULL THEN l.id END), 0) * 100, 1) as q_conv,
              ROUND(COUNT(DISTINCT CASE WHEN l.status = 5 AND lq.id IS NULL THEN l.id END) /
                    NULLIF(COUNT(DISTINCT CASE WHEN lq.id IS NULL THEN l.id END), 0) * 100, 1) as uq_conv
            FROM leads_lead l
            LEFT JOIN leads_leadquote lq ON lq.lead_id = l.id
            WHERE l.user_id IN ({ph}) AND l.created >= %s AND l.created < %s
        """, uid_params + (start_str_12m, end_str_12m))[0]
        quoted_conv   = float(qv_row["q_conv"] or 0)
        unquoted_conv = float(qv_row["uq_conv"] or 0)
        call_mult  = (f"{conv_by_calls[-1] / conv_by_calls[0]:.1f}x" if conv_by_calls[0] > 0 and conv_by_calls[-1] > 0 else "N/A")
        quote_mult = f"{quoted_conv / unquoted_conv:.1f}x" if unquoted_conv > 0 else "N/A"

        seg_0     = query(conn, f"""SELECT COUNT(*) as cnt FROM leads_lead l WHERE l.user_id IN ({ph}) AND l.status NOT IN (5,6,7) AND l.close_reason_id IS NULL AND NOT EXISTS (SELECT 1 FROM applications_application a WHERE a.lead_id=l.id AND a.submitted IS NOT NULL) AND NOT EXISTS (SELECT 1 FROM leads_leadaction la WHERE la.object_id=l.id AND la.object_type='lead' AND la.action_type='call' AND la.deleted=0 AND la.user_id IN (SELECT user_id FROM account_userrole_users WHERE userrole_id=2)) AND EXISTS (SELECT 1 FROM leads_leadquote lq WHERE lq.lead_id=l.id AND lq.deleted=0)""", uid_params)[0]["cnt"]
        seg_3plus = query(conn, f"""SELECT COUNT(*) as cnt FROM leads_lead l WHERE l.user_id IN ({ph}) AND l.status NOT IN (5,6,7) AND l.close_reason_id IS NULL AND NOT EXISTS (SELECT 1 FROM applications_application a WHERE a.lead_id=l.id AND a.submitted IS NOT NULL) AND (SELECT COUNT(*) FROM leads_leadaction la WHERE la.object_id=l.id AND la.object_type='lead' AND la.action_type='call' AND la.deleted=0 AND la.user_id IN (SELECT user_id FROM account_userrole_users WHERE userrole_id=2))>=3""", uid_params)[0]["cnt"]
        q_followed = query(conn, f"""SELECT COUNT(*) as cnt FROM leads_lead l WHERE l.user_id IN ({ph}) AND l.status=3 AND l.close_reason_id IS NULL AND NOT EXISTS (SELECT 1 FROM applications_application a WHERE a.lead_id=l.id AND a.submitted IS NOT NULL) AND EXISTS (SELECT 1 FROM leads_leadaction la WHERE la.object_id=l.id AND la.object_type='lead' AND la.action_type='call' AND la.deleted=0 AND la.user_id IN (SELECT user_id FROM account_userrole_users WHERE userrole_id=2))""", uid_params)[0]["cnt"]
        stale_q   = query(conn, f"""SELECT COUNT(*) as cnt FROM leads_lead l WHERE l.user_id IN ({ph}) AND l.status=3 AND l.close_reason_id IS NULL AND l.last_action_time < DATE_SUB(NOW(), INTERVAL 5 DAY) AND NOT EXISTS (SELECT 1 FROM applications_application a WHERE a.lead_id=l.id AND a.submitted IS NOT NULL)""", uid_params)[0]["cnt"]
        stale_p   = query(conn, f"""SELECT ROUND(SUM(lq.total_premium)) as est_prem FROM leads_lead l LEFT JOIN (SELECT lead_id, SUM(value) as total_premium FROM leads_leadquote WHERE deleted=0 GROUP BY lead_id) lq ON lq.lead_id=l.id WHERE l.user_id IN ({ph}) AND l.status=3 AND l.close_reason_id IS NULL AND l.last_action_time < DATE_SUB(NOW(), INTERVAL 5 DAY) AND NOT EXISTS (SELECT 1 FROM applications_application a WHERE a.lead_id=l.id AND a.submitted IS NOT NULL)""", uid_params)[0]
        stale_est_prem = int(stale_p["est_prem"] or 0)

        total_conv = sum(int(r["converted"] or 0) for r in call_rows)
        total_val  = sum(int(r["avg_case"] or 0) * int(r["converted"] or 0) for r in call_rows)
        overall_avg_case = round(total_val / total_conv) if total_conv > 0 else 2000
        pipeline_segments = [
            ("Leads with\n3+ calls", seg_3plus, f"{conv_by_calls[-1]}%", round(seg_3plus * conv_by_calls[-1] / 100 * overall_avg_case / 1000) if conv_by_calls else 0),
            ("Quoted leads\n(follow-up done)", q_followed, f"{quoted_conv}%", round(q_followed * quoted_conv / 100 * overall_avg_case / 1000)),
            ("Quoted leads\n(awaiting follow-up)", stale_q, f"{quoted_conv}%", round(stale_q * quoted_conv / 100 * overall_avg_case / 1000)),
            ("Face-to-face\nleads", seg_0, f"{conv_by_calls[0]}%" if conv_by_calls else "0%", round(seg_0 * conv_by_calls[0] / 100 * overall_avg_case / 1000) if conv_by_calls else 0),
        ]
        config.update({
            "CONV_BY_CALLS_12M":  conv_by_calls,
            "QUOTED_VS_UNQUOTED": [unquoted_conv, quoted_conv],
            "CALL_MULTIPLIER":    call_mult,
            "QUOTE_MULTIPLIER":   quote_mult,
            "TOTAL_LEADS_12M":    total_leads_12m,
            "AVG_CASE_0_CALLS":   avg_case_0,
            "AVG_CASE_3_PLUS":    avg_case_3,
            "TABLE_DATA_10":      table_data,
            "PIPELINE_SEGMENTS":  pipeline_segments,
            "STALE_QUOTED_COUNT": stale_q,
            "STALE_EST_PREMIUM":  stale_est_prem,
            "QUOTED_CONV":        quoted_conv,
            "UNTOUCHED_LEADS":    seg_0,
            "UNTOUCHED_CONV":     f"{conv_by_calls[0]}%" if conv_by_calls else "0%",
            "STALE_QUOTES":       stale_q,
            "STALE_QUOTES_CONV":  f"{quoted_conv}%",
            "EST_PIPELINE_VALUE": f"${stale_est_prem:,}",
            "UNQUOTED_CONV":      unquoted_conv,
        })

        # ── Section 12: Summary ──
        stale_appts = query(conn, f"""
            SELECT COUNT(*) as cnt FROM leads_leadschedule ls
            JOIN leads_lead l ON l.id = ls.object_id AND ls.object_type = 'lead'
            WHERE l.user_id IN ({ph})
              AND ls.date < DATE_SUB(NOW(), INTERVAL 7 DAY)
              AND l.status NOT IN (5, 6, 7) AND l.close_reason_id IS NULL
        """, uid_params)[0]["cnt"]
        config["STALE_APPOINTMENTS"] = stale_appts

        has_page6 = len(config.get("APPS", [])) > 10
        config["HAS_PAGE6"]    = has_page6
        config["TOTAL_PAGES"]  = 13 if has_page6 else 12

        # AI narratives
        from generate_narratives import enrich_config_with_narratives
        config = enrich_config_with_narratives(config, api_key=api_key)
        config = _apply_narrative_fallbacks(config)

        return config

    finally:
        if own_conn:
            conn.close()


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build adviser report config from live DB")
    parser.add_argument("--user_id", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--output", default="report_config.py")
    parser.add_argument("--api_key", default=None,
                        help="Anthropic API key (or set ANTHROPIC_API_KEY env var)")
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("ANTHROPIC_API_KEY")
    config = build_all(args.user_id, args.month, args.year, api_key=api_key)
    write_config(config, args.output)
    print(f"  Adviser: {config['ADVISER_NAME']}")
    print(f"  Practice: {config['PRACTICE_NAME']}")
    print(f"  Period: {config['REPORT_MONTH_NAME']} {config['REPORT_YEAR']}")
    print(f"  Apps: {config['KPI_APPLICATIONS']}, Premium: {config['KPI_TOTAL_SUBMITTED']}")
