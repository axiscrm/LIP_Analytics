"""
Section 10: What Your Data Says Works
Two side-by-side charts: Call Activity conversion & Quoted vs Unquoted
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
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
BAR_BLUE = "#717680"
LINE_NAVY = "#181D27"


def make_charts(out_path):
    fig, ax1 = plt.subplots(figsize=(7.2, 3.0))
    fig.subplots_adjust(left=0.10, right=0.96, top=0.88, bottom=0.18)

    # ── Conversion by Call Activity ──
    labels = cfg.CALL_BUCKETS
    rates = cfg.CONV_BY_CALLS_12M
    # Relabel "0 calls" → "Face-to-face" (leads with no phone calls logged)
    display_labels = ["Face-to-face" if l == "0 calls" else l for l in labels]
    f2f_color = "#A4A7AE"
    bar_colors1 = [f2f_color if l == "0 calls" else BAR_BLUE for l in labels]

    bars1 = ax1.bar(display_labels, rates, color=bar_colors1, width=0.6, zorder=3)

    x_pos = np.arange(len(labels))
    ax1.plot(x_pos, rates, color=LINE_NAVY, linewidth=2, marker='o',
             markersize=5, zorder=4)

    for i, (bar, rate) in enumerate(zip(bars1, rates)):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                f"{rate}%", ha='center', va='bottom', fontsize=8.5,
                fontweight='bold', color=NAVY)

    top_rate = max(rates) if rates else 0
    ax1.annotate(cfg.CALL_MULTIPLIER, xy=(len(rates) - 1, top_rate),
                xytext=(len(rates) - 1.8, min(top_rate + 20, 100)),
                fontsize=14, fontweight='bold', color=NAVY, ha='center')

    ax1.set_ylabel("Your Conversion Rate (%)", fontsize=8, color=GREY_TEXT)
    ax1.set_ylim(0, max(max(rates) * 1.4, 10) if rates else 10)
    ax1.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax1.tick_params(axis='both', labelsize=7.5)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['left'].set_color('#D5D7DA')
    ax1.spines['bottom'].set_color('#D5D7DA')
    ax1.grid(axis='y', alpha=0.3, linewidth=0.5)

    fig.savefig(out_path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    return out_path


def make_f2f_chart(out_path):
    """For face-to-face advisers: bar chart comparing overall vs quoted conversion rate."""
    conv_by_calls = getattr(cfg, "CONV_BY_CALLS_12M", [])
    overall_conv = conv_by_calls[0] if conv_by_calls else 0  # 0-calls bucket = F2F overall rate
    quoted_conv  = getattr(cfg, "QUOTED_CONV", 0)

    labels = ["All face-to-face\nleads", "Quoted leads\nonly"]
    values = [overall_conv, quoted_conv]
    bar_colors = ["#A4A7AE", "#252B37"]

    fig, ax = plt.subplots(figsize=(5.5, 3.2))
    bars = ax.bar(labels, values, color=bar_colors, width=0.45, zorder=3)

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{val}%", ha="center", va="bottom", fontsize=11,
                fontweight="bold", color="#181D27")

    # Multiplier annotation between the two bars
    if overall_conv > 0 and quoted_conv > 0:
        mult = round(quoted_conv / overall_conv, 1)
        ax.annotate(
            f"{mult}× more likely\nonce quoted",
            xy=(0.5, (overall_conv + quoted_conv) / 2),
            xytext=(0.5, max(values) * 0.55),
            fontsize=9, color="#717680", ha="center", style="italic",
        )

    ax.set_ylim(0, max(values) * 1.5 if values else 10)
    ax.set_ylabel("Conversion Rate (%)", fontsize=9, color="#535862")
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax.tick_params(axis="both", labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#D5D7DA")
    ax.spines["bottom"].set_color("#D5D7DA")
    ax.grid(axis="y", alpha=0.25, linewidth=0.5)

    plt.tight_layout()
    fig.savefig(out_path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close()
    return out_path


def draw_section10(output_path):
    c = canvas.Canvas(output_path, pagesize=A4)

    # Section heading
    y = H - 28 * mm
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(ML, y, "10. What Your Data Says Works")

    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(ML, y, W - MR, y)

    subsec_style = ParagraphStyle("subsec", fontName="Helvetica-Bold", fontSize=12,
                                   leading=16, textColor=colors.HexColor(NAVY))
    narr_style = ParagraphStyle("narr", fontName="Helvetica", fontSize=10,
                                 leading=14, textColor=colors.HexColor(BODY_TEXT))
    close_style = ParagraphStyle("close", fontName="Helvetica", fontSize=10,
                                  leading=14, textColor=colors.HexColor(BODY_TEXT))

    is_f2f = getattr(cfg, "IS_FACE_TO_FACE", False)
    quoted_conv = getattr(cfg, "QUOTED_CONV", 0)
    chart_path = output_path.replace(".pdf", "_chart.png")

    if is_f2f:
        # ── 10.1 Your Current Pipeline ──
        y -= 6 * mm
        s1 = Paragraph("10.1  Your Current Pipeline", subsec_style)
        sw, sh = s1.wrap(UW, 20)
        s1.drawOn(c, ML, y - sh)
        y -= sh + 4 * mm

        # Commentary above table
        what_works_narr = getattr(cfg, "WHAT_WORKS_NARRATIVE", "")
        if what_works_narr:
            narr = Paragraph(what_works_narr, narr_style)
            nw, nh = narr.wrap(UW, 80)
            narr.drawOn(c, ML, y - nh)
            y -= nh + 6 * mm

        col1_hdr = "Engagement Level"
        close_text = (
            f"Your <b>{cfg.UNTOUCHED_LEADS} quoted face-to-face leads</b> represent the most "
            f"immediate conversion opportunity in your pipeline — at a quoted conversion rate of "
            f"<b>{quoted_conv}%</b>, each of these leads is a high-probability prospect. "
            f"Prioritise moving them to application."
        )

    else:
        # ── 10.1 Your Conversion Driver: Repeated Contact ──
        y -= 6 * mm
        s1 = Paragraph("10.1  Your Conversion Driver: Repeated Contact", subsec_style)
        sw, sh = s1.wrap(UW, 20)
        s1.drawOn(c, ML, y - sh)
        y -= sh + 4 * mm

        # Commentary above table
        narr = Paragraph(cfg.WHAT_WORKS_NARRATIVE, narr_style)
        nw, nh = narr.wrap(UW, 80)
        narr.drawOn(c, ML, y - nh)
        y -= nh + 6 * mm

        col1_hdr = "Call Activity"
        untouched = cfg.UNTOUCHED_LEADS
        untouched_conv = cfg.UNTOUCHED_CONV
        close_text = (
            f"The <b>{untouched} face-to-face leads</b> currently with no phone contact represent the biggest "
            f"untapped opportunity in your pipeline (current conversion: {untouched_conv}). "
            "Making initial contact — even a brief call — on these leads would significantly lift their expected conversion rate."
        )

    # ── Table (shared) ──
    TABLE_DATA = cfg.TABLE_DATA_10
    col_w = [UW * 0.22, UW * 0.22, UW * 0.28, UW * 0.28]
    hdr_h = 7.5 * mm
    row_h = 6.5 * mm

    c.setFillColor(colors.HexColor(NAVY))
    c.rect(ML, y - hdr_h, UW, hdr_h, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 8.5)
    headers = [col1_hdr, "Your Conv. Rate", "Avg Case Value", "Leads Currently Here"]
    x = ML
    for i, hdr in enumerate(headers):
        c.drawString(x + 2.5 * mm, y - hdr_h + 2.2 * mm, hdr)
        x += col_w[i]
    y -= hdr_h

    green = "#252B37"
    for idx, row in enumerate(TABLE_DATA):
        bg = colors.HexColor("#F5F5F5") if idx % 2 == 1 else colors.white
        c.setFillColor(bg)
        c.rect(ML, y - row_h, UW, row_h, fill=1, stroke=0)
        c.setStrokeColor(colors.HexColor("#D5D7DA"))
        c.setLineWidth(0.3)
        c.line(ML, y - row_h, ML + UW, y - row_h)

        is_last = (idx == len(TABLE_DATA) - 1)
        c.setFont("Helvetica-Bold" if is_last else "Helvetica", 8.5)
        c.setFillColor(colors.HexColor(green) if is_last else colors.HexColor(BODY_TEXT))

        x = ML
        for i, val in enumerate(row):
            display_val = "Face-to-face" if (i == 0 and val == "0 calls") else val
            c.drawString(x + 2.5 * mm, y - row_h + 2 * mm, display_val)
            x += col_w[i]
        y -= row_h

    y -= 6 * mm

    close = Paragraph(close_text, close_style)
    cw, ch = close.wrap(UW, 50)
    close.drawOn(c, ML, y - ch)

    # Footer
    draw_footer(c, 10 - (0 if getattr(cfg, "HAS_PAGE6", True) else 1), cfg.TOTAL_PAGES)

    c.save()
    if os.path.exists(chart_path):
        os.remove(chart_path)
    return output_path


if __name__ == "__main__":
    os.makedirs("/home/claude/adviser-monthly-reports/output", exist_ok=True)
    path = draw_section10("/home/claude/adviser-monthly-reports/output/section10_sample.pdf")
    print(f"\u2705 {path} ({os.path.getsize(path) / 1024:.0f} KB)")
