"""
Section 3: How You Compare – Licensee Benchmarking
Matches Sonny's layout: percentile bar, comparison table, histogram
"""
import os
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
import report_config as cfg
from report_footer import draw_footer
from reportlab.platypus import Paragraph

W, H = A4
MARGIN_L = 28 * mm
MARGIN_R = 28 * mm
USABLE_W = W - MARGIN_L - MARGIN_R

NAVY = "#181D27"
BLUE_BAR = "#414651"
GREEN_BAR = "#252B37"   # dark blue (adviser highlight)
GOLD_LINE = "#717680"
GREY_TEXT = "#717680"
BODY_TEXT = "#535862"
TABLE_HEADER_BG = "#181D27"
TABLE_ALT_ROW = "#F5F5F5"
GREEN_TEXT = "#181D27"  # navy for adviser values
BLUE_TEXT = "#414651"

# Data from config
ADVISER_PREMIUM_12M = cfg.ADVISER_PREMIUM_12M
ADVISER_RANK = cfg.ADVISER_RANK
TOTAL_PRACTICES = cfg.TOTAL_PRACTICES
PERCENTILE = cfg.PERCENTILE
NETWORK_AVG_PREM = cfg.NETWORK_AVG_PREM
MEDIAN_PREM = cfg.MEDIAN_PREM
TOP_QUARTILE_PREM = cfg.TOP_QUARTILE_PREM
PREM_Q1 = cfg.PREM_Q1
PREM_Q2 = cfg.PREM_Q2
PREM_Q3 = cfg.PREM_Q3

# Histogram data from config
HIST_DATA = cfg.HIST_DATA


def build_histogram(output_path, adviser_prem, median_prem, network_avg_prem):
    """Histogram showing where all practices sit by 12-month premium submitted."""
    bins = list(HIST_DATA.keys())
    counts = list(HIST_DATA.values())
    x = np.arange(len(bins))

    # Bin edges in $k for lookup (matching build_config bin_edges_k)
    bin_edges_k = [(0, 50), (50, 100), (100, 150), (150, 200), (200, 250), (250, 300)]

    def find_adviser_bin(prem_dollars):
        prem_k = prem_dollars / 1000
        for i, (lo, hi) in enumerate(bin_edges_k):
            if lo <= prem_k < hi:
                return i
        return len(bins) - 1  # $300k+

    def val_to_x(prem_dollars):
        """Convert dollar amount to fractional x position for vertical lines."""
        prem_k = prem_dollars / 1000
        for i, (lo, hi) in enumerate(bin_edges_k):
            if lo <= prem_k < hi:
                return i + (prem_k - lo) / (hi - lo)
        return len(bins) - 1  # $300k+

    adviser_bin = find_adviser_bin(adviser_prem)
    bar_colors = [BLUE_BAR] * len(bins)
    bar_colors[adviser_bin] = GREEN_BAR

    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.bar(x, counts, color=bar_colors, width=0.85, zorder=3)

    med_x = val_to_x(median_prem)
    avg_x = val_to_x(network_avg_prem)
    adv_x = val_to_x(adviser_prem)

    ax.axvline(x=med_x, color="#A4A7AE", linestyle="--", linewidth=2.0, alpha=0.9, zorder=4)
    ax.text(med_x, max(counts) + 0.8, f"Median\n${median_prem/1000:.0f}k",
            ha="center", fontsize=8, color="#A4A7AE", fontweight="bold")

    ax.axvline(x=avg_x, color="#A4A7AE", linestyle="-.", linewidth=2.0, alpha=0.9, zorder=4)
    ax.text(avg_x + 0.3, max(counts) + 0.8, f"Network Avg\n${network_avg_prem/1000:.0f}k",
            ha="left", fontsize=8, color="#A4A7AE", fontweight="bold")

    # Adviser marker with arrow
    ax.annotate(f"You: #{cfg.ADVISER_RANK} of {cfg.TOTAL_PRACTICES}",
                xy=(adv_x, counts[adviser_bin]),
                xytext=(adv_x + 1.2, counts[adviser_bin] + 4),
                fontsize=8, fontweight="bold", color="#181D27",
                arrowprops=dict(arrowstyle="->", color="#181D27", lw=1.5))

    ax.set_xticks(x)
    ax.set_xticklabels(bins, fontsize=7.5, rotation=15, ha="right")
    ax.set_xlabel("Total Submitted Premium Over the Past 12 Months", fontsize=9)
    ax.set_ylabel("Number of Practices", fontsize=9)
    ax.tick_params(axis="both", labelsize=8)
    ax.grid(axis="y", alpha=0.15)
    ax.set_axisbelow(True)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    plt.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path


def draw_percentile_bar(c, x, y, w, h, adviser_pct, breakpoints, labels):
    """Draw the horizontal percentile bar with 4 segments."""
    # Segment colours (light to dark)
    seg_colors = ["#F5F5F5", "#E9EAEB", "#D5D7DA", "#A4A7AE"]
    n = len(labels)
    seg_w = w / n

    for i in range(n):
        sx = x + i * seg_w
        c.setFillColor(colors.HexColor(seg_colors[i]))
        c.setStrokeColor(colors.HexColor("#D5D7DA"))
        c.setLineWidth(0.5)
        c.rect(sx, y, seg_w, h, fill=1, stroke=1)

        # Label inside segment
        c.setFont("Helvetica", 8)
        c.setFillColor(colors.HexColor("#535862"))
        c.drawCentredString(sx + seg_w / 2, y + h / 2 - 3, labels[i])

    # Breakpoint labels below — quartile premium thresholds
    c.setFont("Helvetica", 7)
    c.setFillColor(colors.HexColor(GREY_TEXT))
    def fmt_k(v):
        return f"${v // 1000}k" if v >= 1000 else ""
    bp_labels = ["$0", fmt_k(PREM_Q1), fmt_k(PREM_Q2), fmt_k(PREM_Q3), "Top"]
    positions = [x, x + seg_w, x + 2 * seg_w, x + 3 * seg_w, x + w]
    for i, (pos, lbl) in enumerate(zip(positions, bp_labels)):
        if i == 0:
            c.drawString(pos, y - 4 * mm, lbl)
        elif i == len(positions) - 1:
            c.drawRightString(pos, y - 4 * mm, lbl)
        else:
            c.drawCentredString(pos, y - 4 * mm, lbl)

    # Adviser marker (triangle) at their PERCENTILE position (not conversion %)
    # 99th percentile = 99% across the bar
    marker_x = x + (PERCENTILE / 100) * w
    marker_x = min(marker_x, x + w - 3)
    # Draw green triangle
    c.setFillColor(colors.HexColor("#252B37"))
    path = c.beginPath()
    path.moveTo(marker_x - 3 * mm, y - 1 * mm)
    path.lineTo(marker_x + 3 * mm, y - 1 * mm)
    path.lineTo(marker_x, y + 2 * mm)
    path.close()
    c.drawPath(path, fill=1, stroke=0)

    # "YOU: Top X%" label
    c.setFont("Helvetica-Bold", 9)
    c.setFillColor(colors.HexColor("#252B37"))
    pctl_text = "Top 1%" if PERCENTILE >= 99 else f"Top {100 - PERCENTILE}%"
    c.drawString(x + w + 4 * mm, y + h / 2 - 3, f"YOU: {pctl_text}")


def draw_section3(output_path):
    c = canvas.Canvas(output_path, pagesize=A4)


    # Section heading
    y = H - 28 * mm
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(MARGIN_L, y, "3. How You Compare – Licensee Benchmarking")

    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(MARGIN_L, y, W - MARGIN_R, y)

    subsection_style = ParagraphStyle("subsec", fontName="Helvetica-Bold", fontSize=12,
                                       leading=16, textColor=colors.HexColor(NAVY))
    body_style = ParagraphStyle("body", fontName="Helvetica", fontSize=10,
                                 leading=14, textColor=colors.HexColor(BODY_TEXT))

    # ── 3.1 Your Percentile Ranking ──
    y -= 6 * mm
    s1 = Paragraph(f"3.1  Your Percentile Ranking Across {cfg.TOTAL_PRACTICES} Practices", subsection_style)
    sw, sh = s1.wrap(USABLE_W, 30)
    s1.drawOn(c, MARGIN_L, y - sh)
    y -= sh + 4 * mm

    prem_vs_median = (ADVISER_PREMIUM_12M / MEDIAN_PREM) if MEDIAN_PREM > 0 else 0
    prem_vs_avg = (ADVISER_PREMIUM_12M / NETWORK_AVG_PREM) if NETWORK_AVG_PREM > 0 else 0
    pctl_text = "top 1%" if PERCENTILE >= 99 else f"top {100 - PERCENTILE}%"
    intro = Paragraph(
        f"Here's how your total submitted premium over the past 12 months stacks up against the full adviser network "
        f"({cfg.BENCH_PERIOD}). "
        f"<b>You rank #{ADVISER_RANK} out of {cfg.TOTAL_PRACTICES} active practices</b>, placing you in the {pctl_text}. "
        f"Your ${ADVISER_PREMIUM_12M:,} in submitted premium is "
        f"{prem_vs_median:.1f}x the network median of ${MEDIAN_PREM:,} "
        f"and {prem_vs_avg:.1f}x the network average of ${NETWORK_AVG_PREM:,}. "
        f"The top quartile threshold sits at ${TOP_QUARTILE_PREM:,}. "
        "All other practices are shown anonymously below.", body_style)
    pw, ph = intro.wrap(USABLE_W, 150)
    intro.drawOn(c, MARGIN_L, y - ph)
    y -= ph + 14 * mm

    # Percentile bar — centred horizontally
    bar_w = USABLE_W * 0.75
    bar_x = MARGIN_L + (USABLE_W - bar_w) / 2
    bar_h = 10 * mm
    draw_percentile_bar(c, bar_x, y, bar_w, bar_h, ADVISER_PREMIUM_12M,
                        [PREM_Q1, PREM_Q2, PREM_Q3, None],
                        ["Bottom 25%", "25th–50th", "50th–75th", "Top 25%"])
    y -= 18 * mm

    # ── 3.2 Where You Sit histogram ──
    s2 = Paragraph(
        f"3.2  Where You Sit – Total Submitted Premium Over the Past 12 Months "
        f"Across {cfg.TOTAL_PRACTICES} Active Practices", subsection_style)
    sw, sh = s2.wrap(USABLE_W, 40)
    s2.drawOn(c, MARGIN_L, y - sh)
    y -= sh + 4 * mm

    if ADVISER_RANK == 1:
        position_text = "No other practice submitted more premium over this period — you sit at the very top of the network."
    elif ADVISER_RANK <= 5:
        position_text = (f"Only {ADVISER_RANK - 1} other practice{'s' if ADVISER_RANK > 2 else ''} "
                         f"submitted more premium over this period.")
    else:
        position_text = f"Your highlighted bar shows where your ${ADVISER_PREMIUM_12M:,} sits relative to the field."
    bottom = Paragraph(
        f"The distribution below shows where all {TOTAL_PRACTICES} practices sit by total submitted premium "
        f"over the past 12 months. {position_text}",
        body_style)
    pw, ph = bottom.wrap(USABLE_W, 100)
    bottom.drawOn(c, MARGIN_L, y - ph)
    y -= ph + 4 * mm

    # ── Histogram chart
    chart_path = output_path.replace(".pdf", "_hist.png")
    build_histogram(chart_path, ADVISER_PREMIUM_12M, MEDIAN_PREM, NETWORK_AVG_PREM)

    chart_h = 60 * mm
    c.drawImage(chart_path, MARGIN_L, y - chart_h, width=USABLE_W, height=chart_h,
                preserveAspectRatio=True, anchor="c")

    # Footer
    draw_footer(c, 3, cfg.TOTAL_PAGES)

    c.save()
    if os.path.exists(chart_path):
        os.remove(chart_path)
    return output_path


if __name__ == "__main__":
    os.makedirs("/home/claude/adviser-monthly-reports/output", exist_ok=True)
    path = draw_section3("/home/claude/adviser-monthly-reports/output/section3_sample.pdf")
    print(f"✅ {path} ({os.path.getsize(path) / 1024:.0f} KB)")
