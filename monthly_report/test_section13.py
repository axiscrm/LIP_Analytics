"""
Section 13: Glossary & Appendix — methodology, formulas, and data logic.
Two-column layout, all text at 60% opacity.
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph, Frame, KeepInFrame
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT
import report_config as cfg
from report_footer import draw_footer

W, H = A4
ML = 28 * mm
MR = 28 * mm
MT = 28 * mm
MB = 22 * mm
UW = W - ML - MR

NAVY   = colors.Color(24/255,  29/255,  39/255,  0.6)
BODY   = colors.Color(83/255,  88/255,  98/255,  0.6)
GREY   = colors.Color(113/255, 118/255, 128/255, 0.6)
RULE   = colors.Color(233/255, 234/255, 235/255, 0.6)

def S(name, **kw):
    base = dict(fontName="Helvetica", fontSize=7.5, leading=11,
                textColor=BODY, spaceAfter=2, alignment=TA_LEFT)
    base.update(kw)
    return ParagraphStyle(name, **base)

HEAD    = S("head",   fontName="Helvetica-Bold", fontSize=9,   textColor=NAVY, spaceAfter=3, spaceBefore=7)
SUBHEAD = S("sub",    fontName="Helvetica-Bold", fontSize=7.5, textColor=NAVY, spaceAfter=2, spaceBefore=4)
BODY_S  = S("body")
LABEL   = S("label",  fontName="Helvetica-Bold", fontSize=7,   textColor=NAVY, spaceAfter=0)
DESC    = S("desc",   fontSize=7, textColor=BODY, spaceAfter=3, leftIndent=6)

def row(label, desc):
    return [Paragraph(label, LABEL), Paragraph(desc, DESC)]


CONTENT = [
    Paragraph("Appendix: Methodology &amp; Formula Reference", S("title",
        fontName="Helvetica-Bold", fontSize=11, textColor=NAVY, spaceAfter=5)),

    # ── SECTION 1 ──
    Paragraph("Section 1 — Executive Summary", HEAD),
    Paragraph("1.1  Monthly Overview", SUBHEAD),
    *row("Total Submitted Premium",
         "Sum of <i>premium</i> on all applications where <i>submitted</i> falls within the report month."),
    *row("Applications",
         "Count of submitted applications in the report month."),
    *row("Avg Premium",
         "Total Submitted ÷ Applications."),
    *row("12-Month Trend Chart",
         "Bar series = monthly submitted premium ($K) over the rolling 12-month window ending in the report month. "
         "Line series = application count per month. Report month bar is highlighted dark. "
         "Dashed line = 12-month average premium."),

    # ── SECTION 2 ──
    Paragraph("Section 2 — 12-Month Performance Table", HEAD),
    *row("Monthly Rows",
         "Each row covers one calendar month. Premium and application counts are derived from "
         "<i>applications_application</i> filtered to <i>submitted</i> within the month."),

    # ── SECTION 3 ──
    Paragraph("Section 3 — Licensee Benchmarking", HEAD),
    *row("Conversion Rate",
         "Adviser's (applications submitted ÷ leads created) × 100 over the 12-month window."),
    *row("Network Average / Median",
         "Mean and median conversion rates across all active advisers in the licensee with ≥ 5 leads "
         "in the same 12-month window."),
    *row("Percentile",
         "Adviser's position in the ranked network distribution (higher = better)."),
    *row("Benchmarking Chart",
         "Scatter of all adviser conversion rates; adviser dot highlighted. "
         "Horizontal lines mark the network average and median."),

    # ── SECTION 4 ──
    Paragraph("Section 4 — Referral Partners", HEAD),
    Paragraph("4.1  Top Referral Sources", SUBHEAD),
    *row("Groups",
         "Leads are tagged in the CRM with a referral source. Tags are parsed into an organisation group "
         "and an individual name. Leads, applications, and premium are aggregated per group."),
    *row("Conversion",
         "Applications ÷ leads for that group × 100."),
    Paragraph("4.2  Partner Breakdown", SUBHEAD),
    *row("Individual rows",
         "One row per unique referrer within each group. Group = organisation name; "
         "individual = person name only (no org suffix). "
         "Tags with zero calls and a quote are classified as face-to-face."),

    # ── SECTION 5 / 6 ──
    Paragraph("Section 5/6 — Insurer Submissions", HEAD),
    *row("Insurer Rows",
         "Applications submitted in the report month grouped by insurer. "
         "Shows count, total premium, and individual application detail."),

    # ── SECTION 7 ──
    Paragraph("Section 7 — Speed-to-Contact", HEAD),
    Paragraph("7.1  Call Activity vs Conversion", SUBHEAD),
    *row("Call Buckets",
         "Leads created in the 12-month window are bucketed by the number of consultant-role "
         "calls logged in the CRM (0 calls, 1 call, 2 calls, 3+ calls). "
         "Only calls made by users with the <i>Consultant</i> role are counted."),
    *row("Conversion Rate per Bucket",
         "(Leads with status = 5 ÷ total leads in bucket) × 100."),
    *row("Avg Case Value",
         "Average submitted premium across converted leads (status = 5) in the bucket."),
    *row("Call Multiplier",
         "Conversion rate at 3+ calls ÷ conversion rate at 0 calls. "
         "Measures the uplift from persistent follow-up."),
    *row("Face-to-face Detection",
         "Adviser is flagged as face-to-face if more than 95% of their leads in the 12-month "
         "window have zero consultant calls. Chart and commentary adapt accordingly."),
    Paragraph("7.2  Quote Conversion Rate", SUBHEAD),
    *row("Quoted Conversion Rate",
         "(Leads with status = 5 AND at least one quote in <i>leads_leadquote</i>) ÷ "
         "(all leads with at least one quote) × 100, over the 12-month window."),

    # ── SECTION 8 ──
    Paragraph("Section 8 — In-Progress Inforce Forecast", HEAD),
    Paragraph("8.1  Historical Inforce Pattern", SUBHEAD),
    *row("Eligible Applications",
         "All submitted applications (adviser-wide, all time) whose <i>submitted</i> date is "
         "more than 120 days before the first day of the month following the report month. "
         "This ensures every eligible application has had a full opportunity to be inforced."),
    *row("Inforce Rate",
         "(Eligible apps with status = 4) ÷ eligible submitted apps × 100."),
    *row("Timing Distribution Chart",
         "For apps where both <i>submitted</i> and <i>commenced</i> dates are populated, "
         "days from submission to inforce are bucketed: Week 1 (≤7d), Week 2 (8–14d), "
         "Week 3 (15–21d), Week 4 (22–28d), Month 2 (29–60d), 60+ days. "
         "Bars = % of completions per period; line = cumulative %."),
    Paragraph("8.2  Inforce Forecast", SUBHEAD),
    *row("In-Progress Apps",
         "Applications submitted in the report month with status = 0 (pending) and no "
         "<i>commenced</i> date — i.e., not yet inforced."),
    *row("Expected to Inforce",
         "In-progress count × historical inforce rate."),
    *row("Expected Premium",
         "In-progress premium × historical inforce rate (rounded to nearest $1,000)."),

    # ── SECTION 9 ──
    Paragraph("Section 9 — Remaining Quoted Pipeline", HEAD),
    *row("Quoted Pipeline",
         "Leads with status = 3 (quoted), no <i>close_reason_id</i>, and no submitted application. "
         "Ordered by most recent quote value descending. Client names are clipped to column width. "
         "Last Quote = most recent non-deleted quote value from <i>leads_leadquote</i>."),

    # ── SECTION 10 ──
    Paragraph("Section 10 — What Works", HEAD),
    Paragraph("10.1  What Drives Your Results", SUBHEAD),
    *row("Conversion by Call Activity",
         "Same call-bucket data as Section 7.1. Bar chart shows conversion rate per bucket; "
         "colours distinguish face-to-face (grey) from phone-contact buckets (dark)."),
    Paragraph("10.2  Conversion Driver Table", SUBHEAD),
    *row("Leads Currently Here",
         "Open leads (status not in 5/6/7, no close reason, no submitted application) "
         "grouped by call bucket. Face-to-face row (0 calls) requires the lead to also have "
         "at least one quote — this distinguishes genuinely engaged face-to-face leads from "
         "completely untouched leads."),
    *row("Face-to-face (0 calls)",
         "A lead is counted as face-to-face in the current pipeline only if it has 0 consultant "
         "calls AND at least one quote in <i>leads_leadquote</i>. Untouched leads with no quote "
         "are excluded from this count."),

    # ── SECTION 11 ──
    Paragraph("Section 11 — Strongest Predictor", HEAD),
    Paragraph("11.1  Quoted Conversion Rate", SUBHEAD),
    *row("Narrative",
         "Quoted conversion rate (see Section 7.2). Stale quoted pipeline = quoted leads with "
         "no submitted application, no close reason, and no consultant call in the last 30 days."),
    Paragraph("11.2  Pipeline Segments Chart", SUBHEAD),
    *row("Estimated Value ($K)",
         "Leads × conversion rate % × overall avg case value ÷ 1,000. "
         "Overall avg case = weighted average across all converted leads in the 12-month window."),
    *row("Segments",
         "Quoted (follow-up done): quoted leads with ≥ 1 consultant call after quoting. "
         "Quoted (awaiting follow-up): quoted leads with 0 consultant calls after quoting. "
         "Face-to-face: open leads with a quote and 0 consultant calls ever. "
         "Segments with 0 leads are hidden."),
    *row("Horizontal Bar Chart",
         "Each bar = estimated pipeline value ($K). Label shows value, lead count, and rate applied."),

    # ── SECTION 12 ──
    Paragraph("Section 12 — What Stands Out This Month", HEAD),
    Paragraph("12.1  CRM Logging", SUBHEAD),
    *row("Face-to-face Leads",
         "Open quoted leads with 0 consultant calls (same definition as Section 10.2/11.2)."),
    *row("Stale Quotes",
         "Leads at quoted status with no submitted application, no close reason, "
         "and no consultant call logged in the last 30 days."),
    *row("Est. Pipeline Value",
         "Stale quoted leads × quoted conversion rate × overall avg case value ÷ 1,000. "
         "Represents the estimated premium from re-engaging these dormant leads."),
    Paragraph("12.2  Highlights", SUBHEAD),
    *row("Highlights",
         "AI-generated bullet points summarising the key takeaways from the month. "
         "Generated from the adviser's data by the Claude API (Anthropic) using only "
         "numbers present in the report — no invented figures."),
    *row("Milestone / Callout",
         "Milestone banner shown if total submitted premium ≥ $100,000. "
         "Otherwise a callout box highlights a standout stat with forward-looking commentary."),
]


def draw_section13(output_path):
    c = canvas.Canvas(output_path, pagesize=A4)

    page_num = cfg.TOTAL_PAGES
    draw_footer(c, page_num, page_num)

    # Two-column layout
    col_gap = 6 * mm
    col_w = (UW - col_gap) / 2
    col_h = H - MT - MB - 10 * mm
    col_x = [ML, ML + col_w + col_gap]
    col_y = H - MT

    # Page heading
    c.setFont("Helvetica-Bold", 13)
    c.setFillColor(NAVY)
    c.drawString(ML, col_y, "Appendix: Methodology & Formula Reference")
    c.setStrokeColor(RULE)
    c.setLineWidth(0.3)
    c.line(ML, col_y - 4 * mm, W - MR, col_y - 4 * mm)
    col_y -= 10 * mm

    # Split content roughly in half and flow into two columns
    mid = len(CONTENT) // 2
    # Find a clean split point near the middle at a HEAD paragraph
    for i in range(mid, len(CONTENT)):
        if CONTENT[i].style.name == "head":
            mid = i
            break

    left_story  = CONTENT[:mid]
    right_story = CONTENT[mid:]

    for story, cx in [(left_story, col_x[0]), (right_story, col_x[1])]:
        frame = Frame(cx, MB, col_w, col_h, leftPadding=0, rightPadding=0,
                      topPadding=0, bottomPadding=0, showBoundary=0)
        kif = KeepInFrame(col_w, col_h, story, mode="shrink")
        frame.addFromList([kif], c)

    c.save()
    return output_path


if __name__ == "__main__":
    import os
    os.makedirs("/Users/jamesnicholls/monthly_performance_report/output", exist_ok=True)
    path = draw_section13("/Users/jamesnicholls/monthly_performance_report/output/section13_sample.pdf")
    print(f"Done: {path}")
