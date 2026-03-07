"""
Section 7: Speed-to-Contact Conversion Analysis
Dual bar chart (Conversion by Call Activity | Case Value by Call Activity) + narrative
Data from Sonny's reference - to be validated against DB when reconnected
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph
from reportlab.lib.styles import ParagraphStyle
import report_config as cfg
from report_footer import draw_footer

W, H = A4
ML = 28 * mm
MR = 28 * mm
UW = W - ML - MR

NAVY = "#181D27"
GREY_TEXT = "#717680"
BODY_TEXT = "#535862"

# Data from config
CALL_BUCKETS = cfg.CALL_BUCKETS
CONV_RATES = cfg.CONV_RATES
AVG_CASE_VALUES = cfg.AVG_CASE_VALUES
TOTAL_LEADS = cfg.TOTAL_LEADS_STC
PERIOD = cfg.STC_PERIOD

# Quoted vs unquoted insight (from DB)
QUOTED_CONV = cfg.QUOTED_CONV_RATE_STC
UNQUOTED_CONV = cfg.UNQUOTED_CONV_RATE_STC

# Bar colors
CONV_COLORS = ["#D5D7DA", "#717680", "#414651", "#252B37"]
CASE_COLORS = ["#A4A7AE", "#717680", "#414651", "#252B37"]


def build_speed_chart(output_path):
    """Side-by-side bar charts: Conversion Rate | Avg Case Value.
    Leads with 0 calls are classified as face-to-face and shown with a distinct colour."""
    # Relabel "0 calls" → "Face-to-face" for display
    display_labels = [
        "Face-to-face" if b == "0 calls" else b for b in CALL_BUCKETS
    ]
    # Distinct colour for F2F bar; standard palette for phone bars
    f2f_color = "#A4A7AE"
    conv_colors = [f2f_color if b == "0 calls" else c
                   for b, c in zip(CALL_BUCKETS, CONV_COLORS)]
    case_colors = [f2f_color if b == "0 calls" else c
                   for b, c in zip(CALL_BUCKETS, CASE_COLORS)]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8.5, 3.8))
    fig.subplots_adjust(wspace=0.35)

    x = np.arange(len(CALL_BUCKETS))
    bar_w = 0.55

    # ── Left: Conversion by Call Activity ──
    bars1 = ax1.bar(x, CONV_RATES, width=bar_w, color=conv_colors, zorder=3, edgecolor="white", linewidth=0.5)
    ax1.set_ylim(0, 105)
    ax1.set_xticks(x)
    ax1.set_xticklabels(display_labels, fontsize=9)
    ax1.set_ylabel("Conversion Rate (%)", fontsize=9)
    ax1.set_title("Conversion by Call Activity", fontsize=11, fontweight="bold", color=NAVY, pad=10)
    ax1.grid(axis="y", alpha=0.15)
    ax1.set_axisbelow(True)
    for spine in ["top", "right"]:
        ax1.spines[spine].set_visible(False)

    for bar, val in zip(bars1, CONV_RATES):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1.5,
                 f"{val}%", ha="center", va="bottom", fontsize=9, fontweight="bold", color=NAVY)

    # ── Right: Avg Case Value by Call Activity ──
    bars2 = ax2.bar(x, AVG_CASE_VALUES, width=bar_w, color=case_colors, zorder=3, edgecolor="white", linewidth=0.5)
    ax2.set_ylim(0, max(max(AVG_CASE_VALUES), 500) * 1.25)
    ax2.set_xticks(x)
    ax2.set_xticklabels(display_labels, fontsize=9)
    ax2.set_ylabel("Avg Case Value ($)", fontsize=9)
    ax2.set_title("Case Value by Call Activity", fontsize=11, fontweight="bold", color=NAVY, pad=10)
    ax2.grid(axis="y", alpha=0.15)
    ax2.set_axisbelow(True)
    for spine in ["top", "right"]:
        ax2.spines[spine].set_visible(False)

    for bar, val in zip(bars2, AVG_CASE_VALUES):
        label = f"${val:,}" if val > 0 else "—"
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 15,
                 label, ha="center", va="bottom", fontsize=9, fontweight="bold", color=NAVY)
    
    plt.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path


def build_f2f_chart(output_path):
    """Quoted vs Unquoted conversion bar chart for face-to-face advisers."""
    labels = ["Unquoted", "Quoted"]
    rates = [UNQUOTED_CONV, QUOTED_CONV]
    bar_colors = ["#A4A7AE", "#252B37"]

    fig, ax = plt.subplots(figsize=(5, 3.2))
    bars = ax.bar(labels, rates, color=bar_colors, width=0.5, zorder=3,
                  edgecolor="white", linewidth=0.5)
    ax.set_ylim(0, min(max(rates) * 1.4, 100) if max(rates) > 0 else 10)
    ax.set_ylabel("Conversion Rate (%)", fontsize=9)
    ax.grid(axis="y", alpha=0.15)
    ax.set_axisbelow(True)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    for bar, val in zip(bars, rates):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{val}%", ha="center", va="bottom", fontsize=10,
                fontweight="bold", color=NAVY)
    plt.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path


def draw_section7(output_path):
    c = canvas.Canvas(output_path, pagesize=A4)

    page_num = 7 - (0 if getattr(cfg, "HAS_PAGE6", True) else 1)

    # Section heading
    y = H - 28 * mm
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(ML, y, "7. Speed-to-Contact Conversion Analysis")

    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(ML, y, W - MR, y)

    narr_style = ParagraphStyle("narr", fontName="Helvetica", fontSize=10,
                                 leading=14, textColor=colors.HexColor(BODY_TEXT))
    subsec_style = ParagraphStyle("subsec", fontName="Helvetica-Bold", fontSize=12,
                                   leading=16, textColor=colors.HexColor(NAVY))
    note_style = ParagraphStyle("note", fontName="Helvetica", fontSize=8.5,
                                 leading=12, textColor=colors.HexColor(GREY_TEXT))

    if getattr(cfg, "IS_FACE_TO_FACE", False):
        # ── 7.1 Face-to-Face Conversion Analysis ──
        y -= 8 * mm
        s = Paragraph("7.1  Face-to-Face Conversion Analysis", subsec_style)
        _, sh = s.wrap(UW, 20)
        s.drawOn(c, ML, y - sh)
        y -= sh + 4 * mm

        # ── Commentary ──
        f2f_conv = CONV_RATES[0] if CONV_RATES else 0
        f2f_avg = AVG_CASE_VALUES[0] if AVG_CASE_VALUES else 0
        stc_narr = getattr(cfg, "STC_NARRATIVE", "")
        if stc_narr:
            narr = Paragraph(stc_narr, narr_style)
        else:
            narr = Paragraph(
                f"Your practice operates exclusively through <b>face-to-face meetings</b> — inclusive of video calls "
                f"and in-person appointments. Phone-based speed-to-contact metrics are not applicable to your approach; "
                f"instead, your performance is measured by conversion rate and case value across {TOTAL_LEADS} leads "
                f"over the last {PERIOD}. "
                f"Your overall conversion rate sits at <b>{f2f_conv}%</b> with an average case value of <b>${f2f_avg:,}</b>.",
                narr_style)
        pw, ph = narr.wrap(UW, 80)
        narr.drawOn(c, ML, y - ph)
        y -= ph + 8 * mm

        # ── KPI tiles: Total Leads | Conversion Rate | Avg Case Value ──
        tile_gap = 6
        tile_w = (UW - tile_gap * 2) / 3
        tile_h = 58
        tile_y = y - tile_h
        tiles = [
            {"label": "TOTAL LEADS", "value": str(TOTAL_LEADS), "sub": f"Last {PERIOD}"},
            {"label": "CONVERSION RATE", "value": f"{f2f_conv}%", "sub": "Face-to-face"},
            {"label": "AVG CASE VALUE", "value": f"${f2f_avg:,}", "sub": "Converted leads"},
        ]
        for i, t in enumerate(tiles):
            tx = ML + i * (tile_w + tile_gap)
            c.setFillColor(colors.HexColor("#FAFAFA"))
            c.setStrokeColor(colors.HexColor("#D5D7DA"))
            c.setLineWidth(0.5)
            c.roundRect(tx, tile_y, tile_w, tile_h, 4, fill=1, stroke=1)
            c.setFont("Helvetica-Bold", 7)
            c.setFillColor(colors.HexColor(GREY_TEXT))
            c.drawCentredString(tx + tile_w / 2, tile_y + tile_h - 13, t["label"])
            c.setFont("Helvetica-Bold", 20)
            c.setFillColor(colors.HexColor(NAVY))
            c.drawCentredString(tx + tile_w / 2, tile_y + tile_h - 36, t["value"])
            c.setFont("Helvetica-Oblique", 7)
            c.setFillColor(colors.HexColor(GREY_TEXT))
            c.drawCentredString(tx + tile_w / 2, tile_y + 6, t["sub"])
        y = tile_y - 10 * mm

        # ── 7.2 Quote Conversion Rate ──
        s2 = Paragraph("7.2  Quote Conversion Rate", subsec_style)
        _, sh2 = s2.wrap(UW, 20)
        s2.drawOn(c, ML, y - sh2)
        y -= sh2 + 4 * mm

        narr2 = Paragraph(
            f"Getting to a quote is the single strongest predictor of conversion. "
            f"<b>{QUOTED_CONV}%</b> of quoted leads go on to convert — every conversation "
            f"that reaches a quote is a high-probability opportunity.",
            narr_style)
        pw, ph = narr2.wrap(UW, 60)
        narr2.drawOn(c, ML, y - ph)
        y -= ph + 6 * mm

        # ── Stats table ──
        col_w = [UW * 0.55, UW * 0.45]
        row_h = 7 * mm
        hdr_h = 7.5 * mm
        rows_data = [
            ("Total leads analysed", str(TOTAL_LEADS)),
            ("Overall conversion rate", f"{f2f_conv}%"),
            ("Average case value", f"${f2f_avg:,}"),
            ("Quoted conversion rate", f"{QUOTED_CONV}%"),
        ]
        # Header
        c.setFillColor(colors.HexColor(NAVY))
        c.rect(ML, y - hdr_h, UW, hdr_h, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 8.5)
        c.drawString(ML + 3 * mm, y - hdr_h + 2.2 * mm, "Metric")
        c.drawString(ML + col_w[0] + 3 * mm, y - hdr_h + 2.2 * mm, f"Last {PERIOD}")
        y -= hdr_h
        for idx, (label, value) in enumerate(rows_data):
            row_y = y - row_h
            if idx % 2 == 1:
                c.setFillColor(colors.HexColor("#F5F5F5"))
                c.rect(ML, row_y, UW, row_h, fill=1, stroke=0)
            c.setStrokeColor(colors.HexColor("#D5D7DA"))
            c.setLineWidth(0.3)
            c.line(ML, row_y, ML + UW, row_y)
            c.setFont("Helvetica", 8.5)
            c.setFillColor(colors.HexColor(BODY_TEXT))
            c.drawString(ML + 3 * mm, row_y + 2.2 * mm, label)
            c.setFont("Helvetica-Bold", 8.5)
            c.drawString(ML + col_w[0] + 3 * mm, row_y + 2.2 * mm, value)
            y -= row_h

        draw_footer(c, page_num, cfg.TOTAL_PAGES)
        c.save()
        return output_path

    # ── Standard phone-based section ──
    # ── 7.1 Subsection heading ──
    y -= 8 * mm
    s = Paragraph("7.1  Speed to Contact Analysis", subsec_style)
    _, sh = s.wrap(UW, 20)
    s.drawOn(c, ML, y - sh)
    y -= sh + 4 * mm

    # ── Commentary ──
    narr_text = cfg.STC_NARRATIVE if cfg.STC_NARRATIVE else (
        f"<b>Each additional call attempt increases your conversion rate.</b> "
        f"The data below covers your last {PERIOD} ({TOTAL_LEADS} leads), broken down by call activity. "
        f"<b>{QUOTED_CONV}%</b> of your quoted leads go on to convert — "
        "getting to a quote is the single strongest predictor of your success."
    )
    narr = Paragraph(narr_text, narr_style)
    pw, ph = narr.wrap(UW, 80)
    narr.drawOn(c, ML, y - ph)
    y -= ph + 6 * mm

    # ── Chart ──
    chart_path = output_path.replace(".pdf", "_chart.png")
    build_speed_chart(chart_path)
    chart_h = 68 * mm
    c.drawImage(chart_path, ML - 5 * mm, y - chart_h, width=UW + 10 * mm, height=chart_h,
                preserveAspectRatio=True, anchor="nw")
    y -= chart_h + 6 * mm

    # ── Footnote ──
    f2f_note = Paragraph(
        "* Face-to-face is inclusive of video calls and in-person meetings. Leads with zero "
        "calls logged in 3CX are classified as face-to-face, tracked separately in Section 9, "
        "and excluded from phone-based conversion benchmarks.",
        note_style)
    _, nh = f2f_note.wrap(UW, 60)
    f2f_note.drawOn(c, ML, y - nh)

    # Footer
    draw_footer(c, page_num, cfg.TOTAL_PAGES)

    c.save()
    if os.path.exists(chart_path):
        os.remove(chart_path)
    return output_path


if __name__ == "__main__":
    os.makedirs("/home/claude/adviser-monthly-reports/output", exist_ok=True)
    path = draw_section7("/home/claude/adviser-monthly-reports/output/section7_sample.pdf")
    print(f"✅ {path} ({os.path.getsize(path) / 1024:.0f} KB)")
