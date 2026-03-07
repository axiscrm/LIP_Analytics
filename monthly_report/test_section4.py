"""
Section 4: Referral Partner Performance
Primary chart: grouped by organisation (PARTNER_GROUPS)
Secondary table: individual breakdown with Group column
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
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import Paragraph
from reportlab.pdfbase import pdfmetrics
import report_config as cfg
from report_footer import draw_footer

W, H = A4
MARGIN_L = 28 * mm
MARGIN_R = 28 * mm
USABLE_W = W - MARGIN_L - MARGIN_R

NAVY = "#181D27"
BLUE_BAR = "#414651"
GREEN_BAR = "#252B37"
GOLD_BAR = "#717680"
LIGHT_BAR = "#D5D7DA"
GREY_TEXT = "#717680"
BODY_TEXT = "#535862"
TABLE_HEADER_BG = "#181D27"
TABLE_ALT_ROW = "#F5F5F5"

PARTNER_GROUPS = cfg.PARTNER_GROUPS
PARTNERS = cfg.PARTNERS


def clip_str(text, max_pts, font_name="Helvetica", font_size=8.5):
    if pdfmetrics.stringWidth(text, font_name, font_size) <= max_pts:
        return text
    ellipsis = "…"
    while text and pdfmetrics.stringWidth(text + ellipsis, font_name, font_size) > max_pts:
        text = text[:-1]
    return text + ellipsis


def build_group_chart(output_path, groups):
    """Dual horizontal bar chart: Volume (leads) | Conversion (%) by org group."""
    if not groups:
        return None

    names = [g["name"] for g in groups]
    leads = [g["leads"] for g in groups]
    convs = [g["conv"] for g in groups]
    n = len(names)

    # Wrap long names
    wrapped = []
    for name in names:
        if len(name) > 18:
            words = name.split()
            mid = len(words) // 2
            wrapped.append(" ".join(words[:mid]) + "\n" + " ".join(words[mid:]))
        else:
            wrapped.append(name)

    fig_h = max(3.2, n * 0.55 + 1.4)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8.5, fig_h), sharey=True)
    fig.subplots_adjust(wspace=0.08)

    y = np.arange(n)

    # Left: lead volume
    bars1 = ax1.barh(y, leads, color=BLUE_BAR, height=0.6, zorder=3)
    ax1.set_xlim(0, max(leads) * 1.3 if leads else 10)
    ax1.set_yticks(y)
    ax1.set_yticklabels(wrapped, fontsize=8)
    ax1.yaxis.set_ticks_position("none")
    for bar, v in zip(bars1, leads):
        ax1.text(v + 0.3, bar.get_y() + bar.get_height() / 2,
                 str(v), va="center", ha="left", fontsize=8, fontweight="bold", color=NAVY)
    ax1.set_xlabel("Leads (12 months)", fontsize=9)
    ax1.grid(axis="x", alpha=0.15)
    ax1.set_axisbelow(True)
    for spine in ["top", "right"]:
        ax1.spines[spine].set_visible(False)

    # Right: conversion
    bar_colors = [GREEN_BAR if c >= 60 else GOLD_BAR if c >= 40 else LIGHT_BAR for c in convs]
    bars2 = ax2.barh(y, convs, color=bar_colors, height=0.6, zorder=3)
    ax2.set_xlim(0, 110)
    for bar, v in zip(bars2, convs):
        ax2.text(v + 1.5, bar.get_y() + bar.get_height() / 2,
                 f"{v}%", va="center", ha="left", fontsize=8, fontweight="bold", color=NAVY)
    ax2.set_xlabel("Conversion Rate (%)", fontsize=9)
    ax2.grid(axis="x", alpha=0.15)
    ax2.set_axisbelow(True)
    for spine in ["top", "right"]:
        ax2.spines[spine].set_visible(False)

    ax1.invert_yaxis()

    plt.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path


def draw_section4(output_path):
    from collections import OrderedDict

    c = canvas.Canvas(output_path, pagesize=A4)
    chart_path = output_path.replace(".pdf", "_chart.png")

    subsec_style = ParagraphStyle("subsec", fontName="Helvetica-Bold", fontSize=12,
                                   leading=16, textColor=colors.HexColor(NAVY))
    body_style   = ParagraphStyle("body", fontName="Helvetica", fontSize=10,
                                   leading=14, textColor=colors.HexColor(BODY_TEXT))

    col_widths = [USABLE_W * 0.48, USABLE_W * 0.13, USABLE_W * 0.13,
                  USABLE_W * 0.16, USABLE_W * 0.10]
    row_h  = 7 * mm
    table_x = MARGIN_L
    headers = ["Name", "Leads", "Apps", "Premium", "Conv."]

    def footer(canvas_obj):
        draw_footer(canvas_obj, 4, cfg.TOTAL_PAGES)

    def new_page():
        nonlocal y
        footer(c)
        c.showPage()
        y = H - 28 * mm

    def draw_table_header():
        nonlocal y
        c.setFillColor(colors.HexColor(TABLE_HEADER_BG))
        c.rect(table_x, y - row_h, USABLE_W, row_h, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 8.5)
        cx = table_x
        for i, h in enumerate(headers):
            if i == 0:
                c.drawString(cx + 2.5 * mm, y - row_h + 2.2 * mm, h)
            else:
                c.drawCentredString(cx + col_widths[i] / 2, y - row_h + 2.2 * mm, h)
            cx += col_widths[i]
        y -= row_h

    def draw_group_header(group_name, g_data):
        nonlocal y
        if y - row_h < 28 * mm:
            new_page()
            draw_table_header()
        row_y = y - row_h
        c.setFillColor(colors.HexColor("#E9EAEB"))
        c.rect(table_x, row_y, USABLE_W, row_h, fill=1, stroke=0)
        c.setFont("Helvetica-Bold", 8.5)
        c.setFillColor(colors.HexColor(NAVY))
        c.drawString(table_x + 2.5 * mm, row_y + 2.2 * mm,
                     clip_str(group_name, col_widths[0] - 3 * mm))
        if g_data:
            col_offset = col_widths[0]
            prem_g = f"${g_data['prem']:,}" if g_data["prem"] else "—"
            for val, cw in zip(
                [str(g_data["leads"]), str(g_data["apps"]),
                 clip_str(prem_g, col_widths[3] - 3 * mm), f"{g_data['conv']}%"],
                col_widths[1:]
            ):
                c.drawCentredString(table_x + col_offset + cw / 2, row_y + 2.2 * mm, val)
                col_offset += cw
        y -= row_h

    def draw_data_row(name, leads, apps, prem, conv, alt, bold_conv=False, italic=False):
        nonlocal y
        if y - row_h < 28 * mm:
            new_page()
            draw_table_header()
        row_y = y - row_h
        if alt:
            c.setFillColor(colors.HexColor(TABLE_ALT_ROW))
            c.rect(table_x, row_y, USABLE_W, row_h, fill=1, stroke=0)
        prem_str = f"${prem:,}" if prem else "—"
        vals = [clip_str(name, col_widths[0] - 3 * mm),
                str(leads), str(apps),
                clip_str(prem_str, col_widths[3] - 3 * mm),
                f"{conv}%"]
        cx = table_x
        for j, val in enumerate(vals):
            font = "Helvetica-Bold" if (j == 4 and bold_conv) or italic else "Helvetica"
            if italic:
                font = "Helvetica-Oblique"
            c.setFont(font, 8.5)
            c.setFillColor(colors.HexColor(NAVY if (j == 4 and bold_conv) else BODY_TEXT))
            if j == 0:
                c.drawString(cx + 2.5 * mm, row_y + 2.2 * mm, val)
            else:
                c.drawCentredString(cx + col_widths[j] / 2, row_y + 2.2 * mm, val)
            cx += col_widths[j]
        y -= row_h

    # ════════════════════════════════
    y = H - 28 * mm

    # Section heading
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(MARGIN_L, y, "4. Referral Partner Performance")
    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(MARGIN_L, y, W - MARGIN_R, y)

    if not PARTNER_GROUPS:
        y -= 10 * mm
        c.setFont("Helvetica-Oblique", 10)
        c.setFillColor(colors.HexColor(BODY_TEXT))
        c.drawString(MARGIN_L, y, "No referral partner data found for the last 12 months.")
        footer(c)
        c.save()
        return output_path

    # ── 4.1 Performance by Organisation ──
    y -= 6 * mm
    s1 = Paragraph("4.1  Referral Partner Performance by Organisation", subsec_style)
    sw, sh = s1.wrap(USABLE_W, 30)
    s1.drawOn(c, MARGIN_L, y - sh)
    y -= sh + 4 * mm

    top = PARTNER_GROUPS[0]
    high_conv = [g for g in PARTNER_GROUPS if g["conv"] >= 60]
    if high_conv:
        conv_note = f"{high_conv[0]['name']} achieves the highest conversion at {high_conv[0]['conv']}%."
    else:
        best = max(PARTNER_GROUPS, key=lambda g: g["conv"])
        conv_note = f"{best['name']} leads on conversion at {best['conv']}%."
    narr1 = Paragraph(
        f"<b>{top['name']}</b> is your dominant referral source — "
        f"{top['leads']} leads, {top['conv']}% conversion and "
        f"${top['prem']:,} in submitted premium over the last 12 months. {conv_note}",
        body_style)
    pw, ph = narr1.wrap(USABLE_W, 80)
    narr1.drawOn(c, MARGIN_L, y - ph)
    y -= ph + 6 * mm

    if len(PARTNER_GROUPS) >= 2:
        build_group_chart(chart_path, PARTNER_GROUPS)
        n_groups = len(PARTNER_GROUPS)
        chart_h = max(48, n_groups * 9 + 20) * mm
        c.drawImage(chart_path, MARGIN_L, y - chart_h,
                    width=USABLE_W, height=chart_h,
                    preserveAspectRatio=True, anchor="c")
        y -= chart_h + 10 * mm
    else:
        y -= 4 * mm

    # ── 4.2 Individual Breakdown ──
    if y - 40 * mm < 28 * mm:
        new_page()

    s2 = Paragraph("4.2  Individual Breakdown", subsec_style)
    sw, sh = s2.wrap(USABLE_W, 30)
    s2.drawOn(c, MARGIN_L, y - sh)
    y -= sh + 4 * mm

    # Build group lookup from PARTNER_GROUPS for sorting and totals
    partner_group_data = {g["name"]: g for g in PARTNER_GROUPS}
    total_leads = sum(g["leads"] for g in PARTNER_GROUPS)

    # Brief narrative
    top_pct = round(top["leads"] / total_leads * 100) if total_leads else 0
    narr2 = Paragraph(
        f"The table below breaks down performance by individual referral partner. "
        f"{top['name']} accounts for {top_pct}% of all referral leads over the period.",
        body_style)
    pw, ph = narr2.wrap(USABLE_W, 60)
    narr2.drawOn(c, MARGIN_L, y - ph)
    y -= ph + 5 * mm

    draw_table_header()
    alt_row = 0

    # Group ALL non-Other partners by their group field — every partner gets a group header
    all_partners = [p for p in PARTNERS if p["group"] != "Other"]

    def group_sort_key(p):
        g = partner_group_data.get(p["group"])
        group_leads = g["leads"] if g else sum(q["leads"] for q in PARTNERS if q["group"] == p["group"])
        return (-group_leads, p["group"], -p["leads"])

    groups_ordered = OrderedDict()
    for p in sorted(all_partners, key=group_sort_key):
        groups_ordered.setdefault(p["group"], []).append(p)

    for group_name, members in groups_ordered.items():
        # Use PARTNER_GROUPS totals if available, else aggregate from members
        g_data = partner_group_data.get(group_name)
        if g_data is None:
            g_leads = sum(m["leads"] for m in members)
            g_apps  = sum(m["apps"]  for m in members)
            g_prem  = sum(m["prem"]  for m in members)
            g_conv  = round(g_apps / g_leads * 100) if g_leads else 0
            g_data  = {"leads": g_leads, "apps": g_apps, "prem": g_prem, "conv": g_conv}
        draw_group_header(group_name, g_data)
        for p in members:
            draw_data_row("  " + p["name"], p["leads"], p["apps"], p["prem"], p["conv"],
                          alt_row % 2 == 1, bold_conv=(p["conv"] >= 60))
            alt_row += 1

    # Other — single aggregate row
    other_groups = [g for g in PARTNER_GROUPS if g["name"] == "Other"]
    if other_groups:
        og = other_groups[0]
        if y - row_h < 28 * mm:
            new_page()
            draw_table_header()
        row_y = y - row_h
        c.setFillColor(colors.HexColor(TABLE_ALT_ROW))
        c.rect(table_x, row_y, USABLE_W, row_h, fill=1, stroke=0)
        c.setFont("Helvetica-Bold", 8.5)
        c.setFillColor(colors.HexColor(BODY_TEXT))
        c.drawString(table_x + 2.5 * mm, row_y + 2.2 * mm, "Other")
        col_offset = col_widths[0]
        for val, cw in zip(
            [str(og["leads"]), str(og["apps"]),
             f"${og['prem']:,}" if og["prem"] else "—", f"{og['conv']}%"],
            col_widths[1:]
        ):
            c.drawCentredString(table_x + col_offset + cw / 2, row_y + 2.2 * mm, val)
            col_offset += cw
        y -= row_h

    # Grand total row
    if y - row_h < 28 * mm:
        new_page()
        draw_table_header()
    total_leads_all = sum(p["leads"] for p in PARTNERS)
    total_apps_all  = sum(p["apps"]  for p in PARTNERS)
    total_prem_all  = sum(p["prem"]  for p in PARTNERS)
    total_conv_all  = round(total_apps_all / total_leads_all * 100) if total_leads_all else 0
    row_y = y - row_h
    c.setFillColor(colors.HexColor(TABLE_HEADER_BG))
    c.rect(table_x, row_y, USABLE_W, row_h, fill=1, stroke=0)
    c.setFont("Helvetica-Bold", 8.5)
    c.setFillColor(colors.white)
    c.drawString(table_x + 2.5 * mm, row_y + 2.2 * mm, "Total")
    col_offset = col_widths[0]
    for val, cw in zip(
        [str(total_leads_all), str(total_apps_all),
         f"${total_prem_all:,}" if total_prem_all else "—", f"{total_conv_all}%"],
        col_widths[1:]
    ):
        c.drawCentredString(table_x + col_offset + cw / 2, row_y + 2.2 * mm, val)
        col_offset += cw
    y -= row_h

    footer(c)
    c.save()
    if chart_path and os.path.exists(chart_path):
        os.remove(chart_path)
    return output_path


if __name__ == "__main__":
    os.makedirs("/home/claude/adviser-monthly-reports/output", exist_ok=True)
    path = draw_section4("/home/claude/adviser-monthly-reports/output/section4_sample.pdf")
    print(f"✅ {path} ({os.path.getsize(path) / 1024:.0f} KB)")
