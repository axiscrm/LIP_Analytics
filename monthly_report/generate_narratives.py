"""
generate_narratives.py
Uses the Claude API to generate personalised narrative commentary
for each adviser report from their data config.

Called after build_config.py produces the raw data, and before
generate_report.py renders the PDF.

Usage:
    from generate_narratives import enrich_config_with_narratives
    config = enrich_config_with_narratives(config)
"""

import json
import os
import anthropic


SYSTEM_PROMPT = """\
You are a senior performance analyst writing personalised monthly reports for life insurance advisers.
Your role is to deeply analyse each adviser's data and deliver insightful, specific, and actionable
commentary — like a coach who has studied every metric. You connect data points across sections to
tell a cohesive story about what happened, why it matters, and what to do next.

STYLE RULES:
- ALWAYS write in second person ("you", "your"). NEVER say "this adviser", "the adviser", or refer to the adviser in third person.
- Bold key numbers and standout insights using HTML: <b>bold</b>
- Be analytical — don't just state numbers, interpret them and draw connections
- Compare current vs prior periods, identify trends, explain what's driving results
- If performance is strong, highlight WHY and how to sustain it
- If performance is declining, frame constructively — identify the lever to pull
- Never use negative or accusatory language — every observation should feel like coaching
- Celebrate wins; frame gaps as untapped upside
- Use HTML tags for formatting: <b> for bold
- Do NOT use markdown — only HTML inline formatting
- Vary sentence length for readability — mix short punchy lines with deeper analysis
"""


def build_narrative_prompt(config):
    """Build the prompt with all data the narrator needs."""
    # Extract the key data points for the prompt
    months = config.get("MONTHS_DATA", [])
    current_month = months[-1] if months else {}
    prev_months = months[:-1] if len(months) > 1 else []

    # Find best previous month
    best_prev = max(prev_months, key=lambda m: m.get("prem", 0)) if prev_months else {}

    # 12-month totals
    total_12m_premium = sum(m.get("prem", 0) for m in months)
    total_12m_apps = sum(m.get("apps", 0) for m in months)

    # Month labels
    import calendar as cal
    def month_label(m):
        return f"{cal.month_abbr[m.get('m', 1)]} {m.get('y', '')}"

    data_summary = {
        "adviser_name": config.get("ADVISER_NAME", ""),
        "report_month": config.get("REPORT_MONTH_NAME", ""),
        "report_year": config.get("REPORT_YEAR", ""),

        # Current month KPIs
        "current_month_premium": config.get("KPI_TOTAL_SUBMITTED_RAW", 0),
        "current_month_premium_formatted": config.get("KPI_TOTAL_SUBMITTED", ""),
        "current_month_apps": config.get("KPI_APPLICATIONS", 0),
        "current_month_avg_premium": config.get("KPI_AVG_PREMIUM_RAW", 0),

        # Historical context
        "best_previous_month": month_label(best_prev) if best_prev else "",
        "best_previous_premium": best_prev.get("prem", 0),
        "total_12m_premium": total_12m_premium,
        "total_12m_apps": total_12m_apps,
        "months_trend": [{"month": month_label(m), "premium": m.get("prem", 0),
                          "apps": m.get("apps", 0)} for m in months],

        # Benchmarking
        "conversion_rate": config.get("ADVISER_CONV", 0),
        "network_avg": config.get("NETWORK_AVG", 0),
        "median_conv": config.get("MEDIAN", 0),
        "rank": config.get("ADVISER_RANK", 0),
        "total_practices": config.get("TOTAL_PRACTICES", 0),
        "percentile": config.get("PERCENTILE", 0),
        "adviser_leads": config.get("ADVISER_LEADS", 0),

        # Speed to contact / conversion drivers
        "is_face_to_face": config.get("IS_FACE_TO_FACE", False),
        "conv_by_calls": config.get("CONV_BY_CALLS_12M", {}),
        "call_multiplier": config.get("CALL_MULTIPLIER", 0),
        "total_leads_12m": config.get("TOTAL_LEADS_12M", 0),
        "avg_case_0_calls": config.get("AVG_CASE_0_CALLS", 0),
        "avg_case_3_plus": config.get("AVG_CASE_3_PLUS", 0),

        # Pipeline / strongest predictor
        "quoted_conv": config.get("QUOTED_CONV", 0),
        "stale_quoted_count": config.get("STALE_QUOTED_COUNT", 0),
        "stale_est_premium": config.get("STALE_EST_PREMIUM", 0),
        "pipeline_segments": config.get("PIPELINE_SEGMENTS", []),

        # CRM hygiene
        "stale_appointments": config.get("STALE_APPOINTMENTS", 0),

        # Completion forecast
        "completion_rate": config.get("COMPLETION_RATE", 0),
        "avg_days_to_complete": config.get("AVG_DAYS", 0),
        "in_progress_count": config.get("FEB_IN_PROGRESS", 0),
        "expected_completions": config.get("EXPECTED_COMPLETIONS", 0),
        "expected_prem": config.get("EXPECTED_PREM", 0),

        # Referral partners (top sources — from PARTNER_GROUPS)
        "referral_partners": [
            {"name": rp.get("name", ""), "leads": rp.get("leads", 0),
             "apps": rp.get("apps", 0), "conv": rp.get("conv", 0)}
            for rp in (config.get("PARTNER_GROUPS", []) or [])[:5]
        ],

        # Insurer mix (list of tuples: (name, app_count))
        "insurers": [
            {"name": ins[0], "apps": ins[1]} if isinstance(ins, (list, tuple))
            else {"name": ins.get("name", ""), "apps": ins.get("apps", 0)}
            for ins in (config.get("INSURERS", []) or [])[:5]
        ],
    }

    prompt = f"""Here is the complete data for this adviser's monthly performance report. Analyse ALL
the data holistically — look for connections between sections, identify the key story of this
month, and generate rich, personalised commentary that feels like it was written by a coach
who has studied every metric.

DATA:
{json.dumps(data_summary, indent=2, default=str)}

Generate a JSON object with these exact keys. Each value should be an HTML-formatted string
(using <b> for bold, no markdown). Follow the length and content guidance carefully:

1. "EXEC_NARRATIVE" — THIS IS THE MOST IMPORTANT SECTION. Write a comprehensive 5-8 sentence
   executive summary that analyses the full month. Structure it as:
   - Open with the headline result (premium and volume)
   - Compare to prior month AND 12-month trend (is this month above/below average? accelerating or decelerating?)
   - Call out the 1-2 most significant insights from the data (e.g. case size change, conversion shift,
     referral partner concentration, completion rate)
   - Connect the dots — explain WHAT drove the result (was it more leads, better conversion, bigger cases?)
   - Close with a forward-looking statement (pipeline opportunity, momentum, or area to focus)
   This should read like a personalised analyst briefing, not a template.

2. "EXEC_DRIVING" — 2-3 sentences decomposing performance drivers. Break down whether growth came
   from volume (more apps), value (higher case size), or both. Reference specific numbers.

3. "TREND_NARRATIVE" — 3-4 sentences analysing the 12-month performance trajectory. Identify
   the trend direction (growing, declining, volatile, stable). Call out the best and worst months.
   Note whether recent months show acceleration or deceleration. Reference the trailing average.

4. "STC_NARRATIVE" — 3-5 sentences about engagement patterns with an analytical insight.
   CRITICAL: Write in second person — say "you", "your approach", "your leads". NEVER say "this adviser" or "the adviser".
   If is_face_to_face is TRUE: Focus on your face-to-face engagement quality, quoting process, and
   quoted conversion rate. Do NOT reference phone call counts or call multipliers.
   If is_face_to_face is FALSE: Analyse the call engagement data — reference the multiplier,
   conversion across call buckets, and what the pattern reveals about optimal contact strategy.
   In BOTH cases: do NOT reference an unquoted conversion rate. End with a specific insight
   or recommendation.

5. "WHAT_WORKS_INTRO" — 2-3 sentences introducing the conversion analysis with context. Compare
   your rate vs network average and percentile position. Frame what makes your approach distinctive.
   CRITICAL: Write in second person — "you", "your", never "this adviser".

6. "WHAT_WORKS_NARRATIVE" — 4-6 sentences providing deep conversion analysis.
   If is_face_to_face is TRUE: Focus on your overall conversion rate, quoting rate, average case value,
   and what the data reveals about your face-to-face model effectiveness.
   If is_face_to_face is FALSE: Analyse total leads, conversion at 0 vs 3+ calls, case value
   differences, and what this means for time allocation. Bold the multiplier.
   End with a specific, actionable insight.

7. "PREDICTOR_NARRATIVE_1" — 3-4 sentences analysing why getting to a quote is the strongest
   predictor. State the quoted conversion rate, translate it to practical terms (X out of 10),
   and explain what this means for pipeline strategy. Do NOT reference unquoted rates.

8. "PREDICTOR_NARRATIVE_2_TEMPLATE" — 2-3 sentences about the stale quoted pipeline opportunity.
   MUST include the literal placeholders {{{{stale_count}}}} and ${{{{est_premium:,.0f}}}} (these get
   formatted later). Reference the quoted conversion rate and frame the commercial opportunity.

9. "PREDICTOR_CLOSING" — 2-3 sentences with a specific, actionable closing recommendation.
   Summarise the pattern and suggest a concrete next step (not generic advice).

10. "CRM_NOTE" — 2-3 sentences about CRM hygiene. Professional and encouraging — never critical.
    If stale appointments = 0: Celebrate the discipline and tie it to report accuracy.
    If > 0: Frame as a small opportunity for sharper insights. Never use "missing", "problem",
    "incomplete", or "overdue".

11. "FORMULA_TEXT" — 2-3 sentences summarising the winning formula with specificity. Bold the key
    drivers. Reference actual conversion rates or metrics, not just generic advice.
    If is_face_to_face is TRUE: Focus on face-to-face engagement and quoting.
    If is_face_to_face is FALSE: Reference speed-to-contact and follow-up patterns.

12. "COMPLETION_NARRATIVE" — 3-4 sentences analysing the application completion pipeline. Reference
    the completion rate, average days to complete, in-progress applications, and expected premium
    from completions. Frame the forward outlook.

13. "CONCLUSION_NARRATIVE" — THIS IS THE SECOND MOST IMPORTANT SECTION. Write a comprehensive
    5-8 sentence wrap-up that ties the entire report together. Structure it as:
    - Summarise the month's story in one opening sentence
    - Highlight the 2-3 strongest positives from across ALL sections (production, conversion,
      engagement, pipeline, completion)
    - Identify the single biggest opportunity or area for improvement
    - Close with an encouraging, specific, forward-looking statement about next month
    This should feel like a coach's closing remarks — personal, specific, and motivating.

14. "HIGHLIGHTS" — A JSON array of 4-5 short strings (max 15 words each). Each should be a
    specific, data-backed takeaway — not generic statements. Include the actual numbers.

15. "SHOW_MILESTONE" — boolean: true ONLY if current month premium >= $100,000. Otherwise false.

16. "MILESTONE_TEXT" — If SHOW_MILESTONE is true, a short celebration line. If false, empty string.

17. "MILESTONE_SUB" — If SHOW_MILESTONE is true, a 1-sentence celebration. If false, empty string.

18. "CALLOUT_TEXT" — If SHOW_MILESTONE is false, highlight one standout stat as a short punchy
    headline (max 8 words). If SHOW_MILESTONE is true, empty string.

19. "CALLOUT_SUB" — If SHOW_MILESTONE is false, 1 encouraging sentence with a specific number
    or trend. If SHOW_MILESTONE is true, empty string.

RESPOND WITH ONLY THE JSON OBJECT. No preamble, no markdown fences, no explanation."""

    return prompt


def enrich_config_with_narratives(config, api_key=None):
    """Call Claude API to generate narratives and merge them into config.

    Args:
        config: dict from build_config.build_all()
        api_key: Anthropic API key (defaults to ANTHROPIC_API_KEY env var)

    Returns:
        config dict with narrative fields populated
    """
    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("⚠️  No ANTHROPIC_API_KEY — narratives will be empty placeholders")
        return config

    client = anthropic.Anthropic(api_key=api_key)

    prompt = build_narrative_prompt(config)

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        # Extract text response
        text = ""
        for block in response.content:
            if hasattr(block, "text"):
                text += block.text

        # Clean and parse JSON
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            text = text.rsplit("```", 1)[0]

        narratives = json.loads(text)

        # Merge into config
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
        for key in narrative_keys:
            if key in narratives:
                config[key] = narratives[key]

        print(f"✅ Narratives generated ({len([k for k in narrative_keys if k in narratives])} fields)")
        return config

    except json.JSONDecodeError as e:
        print(f"⚠️  Failed to parse narrative JSON: {e}")
        print(f"   Raw response: {text[:200]}...")
        return config
    except Exception as e:
        print(f"⚠️  Claude API error: {e}")
        return config


# ═══════════════════════════════════════════════════════════════════
#  CLI — standalone test
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import importlib
    import sys

    # Load existing static config for testing
    sys.path.insert(0, os.path.dirname(__file__))
    rc = importlib.import_module("report_config")

    # Build config dict from module attributes
    config = {k: getattr(rc, k) for k in dir(rc) if k.isupper() and not k.startswith("_")}

    enriched = enrich_config_with_narratives(config)

    # Print results
    for key in ["EXEC_NARRATIVE", "HIGHLIGHTS", "SHOW_MILESTONE"]:
        print(f"\n{key}:")
        print(f"  {enriched.get(key, '(empty)')}")
