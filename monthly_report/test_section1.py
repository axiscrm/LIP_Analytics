"""
Section 1: Executive Summary - Sample PDF
Pixel-perfect match to Sonny's report layout.
"""
import calendar
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import Paragraph
from reportlab.lib.enums import TA_LEFT, TA_CENTER
import report_config as cfg
from report_footer import draw_footer

W, H = A4  # 595.27 x 841.89 pts
MARGIN_L = 28 * mm
MARGIN_R = 28 * mm
USABLE_W = W - MARGIN_L - MARGIN_R

# Colours from Sonny's report
NAVY = "#181D27"
BLUE_LINE = "#535862"
GREEN_BG = "#F5F5F5"   # light blue
GREEN_TEXT = "#181D27"  # navy
BLUE_BG = "#F5F5F5"
BLUE_TEXT = "#181D27"
GOLD_BG = "#FAFAFA"    # lightest blue
GOLD_TEXT = "#252B37"   # dark blue
GREY_TEXT = "#717680"
BODY_TEXT = "#535862"
LABEL_GREY = "#535862"


# ── 12-Month trend data (from config) ──
MONTHS_DATA = cfg.MONTHS_DATA

CHART_BLUE = "#414651"
CHART_DARK = "#252B37"
CHART_LIGHT = "#A4A7AE"
CHART_MED = "#717680"

def build_trend_chart_mini(output_path):
    """Compact trend chart for page 1."""
    labels = []
    prems = []
    apps = []
    bar_colors = []
    for d in MONTHS_DATA:
        labels.append(f"{calendar.month_abbr[d['m']]}\n'{str(d['y'])[2:]}")
        prems.append(d["prem"])
        apps.append(d["apps"])
        bar_colors.append(CHART_DARK if (d["m"] == cfg.REPORT_MONTH and d["y"] == cfg.REPORT_YEAR) else CHART_BLUE)

    avg_prem = sum(prems) / len(prems)
    fig, ax1 = plt.subplots(figsize=(10, 3.8))
    x = np.arange(len(labels))

    bars = ax1.bar(x, [p / 1000 for p in prems], 0.6, color=bar_colors, zorder=3)
    ax1.axhline(y=avg_prem / 1000, color=CHART_MED, linestyle="--", linewidth=1.2, alpha=0.7, zorder=2)
    ax1.text(len(labels) - 0.5, avg_prem / 1000 + 1, f"12m avg: ${avg_prem/1000:.0f}K",
             fontsize=7.5, color=CHART_MED, fontstyle="italic", ha="right", alpha=0.8)

    ax2 = ax1.twinx()
    ax2.plot(x, apps, color=CHART_LIGHT, marker="o", markersize=6, linewidth=2, zorder=5)
    current_idx = len(labels) - 1
    ax2.plot(current_idx, apps[current_idx], marker="*", markersize=12, color="white", zorder=6)
    ax2.plot(current_idx, apps[current_idx], marker="*", markersize=9, color=CHART_LIGHT, zorder=7)

    label_threshold = max(prems) * 0.65 if prems else 0
    for i, p in enumerate(prems):
        if p >= label_threshold:
            ax1.text(i, p / 1000 + 2, f"${p/1000:.0f}K", ha="center", va="bottom",
                     fontsize=7.5, fontweight="bold", color="#181D27")

    ax1.set_ylabel("Submitted Premium ($K)", fontsize=8, color="#181D27")
    ax2.set_ylabel("Applications", fontsize=8, color=CHART_LIGHT, rotation=270, labelpad=12)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=7)
    ax1.set_ylim(0, max(prems) / 1000 * 1.45 if prems else 10)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:.0f}K"))
    ax1.tick_params(axis="y", labelsize=7)
    ax2.set_ylim(0, max(apps) * 2.8 if apps else 10)
    ax2.tick_params(axis="y", labelsize=7, colors=CHART_LIGHT)
    # Chart title removed — section heading in PDF serves this purpose
    ax1.grid(axis="y", alpha=0.15, zorder=0)
    ax1.set_axisbelow(True)
    for spine in ["top", "right"]:
        ax1.spines[spine].set_visible(False)
    ax2.spines["top"].set_visible(False)
    plt.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path


def draw_section1(output_path):
    c = canvas.Canvas(output_path, pagesize=A4)

    # ── Top left meta
    y = H - 36 * mm
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.HexColor(GREY_TEXT))
    c.drawString(MARGIN_L, y, f"Prepared for {cfg.ADVISER_NAME}, {cfg.PRACTICE_NAME}  |  {cfg.REPORT_DATE}")

    # ── Title
    y -= 18 * mm
    c.setFont("Helvetica-Bold", 26)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(MARGIN_L, y, f"{cfg.REPORT_MONTH_NAME} {cfg.REPORT_YEAR} Performance Report")

    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#181D27"))
    c.setLineWidth(1.2)
    c.line(MARGIN_L, y, W - MARGIN_R, y)

    # ── Section heading
    y -= 12 * mm
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(MARGIN_L, y, "1. Executive Summary")
    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(MARGIN_L, y, W - MARGIN_R, y)

    subsec_style = ParagraphStyle("subsec", fontName="Helvetica-Bold", fontSize=12,
                                   leading=16, textColor=colors.HexColor(NAVY))
    narrative_style = ParagraphStyle("narrative", fontName="Helvetica", fontSize=10,
                                      leading=14, textColor=colors.HexColor(BODY_TEXT))

    # ── 1.1 Monthly Overview ──
    y -= 6 * mm
    s1 = Paragraph("1.1  Monthly Overview", subsec_style)
    sw, sh = s1.wrap(USABLE_W, 20)
    s1.drawOn(c, MARGIN_L, y - sh)
    y -= sh + 4 * mm

    # ── Commentary ──
    p = Paragraph(cfg.EXEC_NARRATIVE, narrative_style)
    pw, ph = p.wrap(USABLE_W, 200)
    p.drawOn(c, MARGIN_L, y - ph)
    y -= ph + 4 * mm

    p2 = Paragraph(cfg.EXEC_DRIVING, narrative_style)
    pw2, ph2 = p2.wrap(USABLE_W, 200)
    p2.drawOn(c, MARGIN_L, y - ph2)
    y -= ph2 + 10 * mm

    # ── KPI Tiles ──
    tile_gap = 6
    tile_w = (USABLE_W - tile_gap * 2) / 3
    tile_h = 62
    box_y = y - tile_h

    tiles = [
        {"label": "TOTAL SUBMITTED", "value": cfg.KPI_TOTAL_SUBMITTED,
         "sub": cfg.KPI_TOTAL_SUB_LABEL, "value_color": "#252B37"},
        {"label": "APPLICATIONS", "value": str(cfg.KPI_APPLICATIONS),
         "sub": cfg.KPI_APPS_LABEL, "value_color": "#252B37"},
        {"label": "AVG PREMIUM", "value": cfg.KPI_AVG_PREMIUM,
         "sub": cfg.KPI_AVG_LABEL, "value_color": "#252B37"},
    ]

    for i, t in enumerate(tiles):
        tx = MARGIN_L + i * (tile_w + tile_gap)
        c.setFillColor(colors.HexColor("#FAFAFA"))
        c.setStrokeColor(colors.HexColor("#D5D7DA"))
        c.setLineWidth(0.5)
        c.roundRect(tx, box_y, tile_w, tile_h, 4, fill=1, stroke=1)
        c.setFont("Helvetica-Bold", 7)
        c.setFillColor(colors.HexColor(LABEL_GREY))
        c.drawCentredString(tx + tile_w/2, box_y + tile_h - 14, t["label"])
        c.setFont("Helvetica-Bold", 24)
        c.setFillColor(colors.HexColor(t["value_color"]))
        c.drawCentredString(tx + tile_w/2, box_y + tile_h - 40, t["value"])
        c.setFont("Helvetica-Oblique", 7)
        c.setFillColor(colors.HexColor("#717680"))
        c.drawCentredString(tx + tile_w/2, box_y + 6, t["sub"])

    draw_footer(c, 1, cfg.TOTAL_PAGES)

    c.save()
    return output_path



if __name__ == "__main__":
    import os
    os.makedirs("/home/claude/adviser-monthly-reports/output", exist_ok=True)
    path = draw_section1("/home/claude/adviser-monthly-reports/output/section1_sample.pdf")
    print(f"✅ {path} ({os.path.getsize(path) / 1024:.0f} KB)")
