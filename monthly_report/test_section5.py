"""
Section 5: Insurer Diversification + February Submissions Full Detail
Page 1: Donut chart + narrative + summary cards + table (first ~20 rows)
Page 2: Table continuation + footnote
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
from reportlab.pdfbase import pdfmetrics

W, H = A4
ML = 28 * mm
MR = 28 * mm
UW = W - ML - MR

NAVY = "#181D27"
GREY_TEXT = "#717680"
BODY_TEXT = "#535862"
TABLE_HEADER_BG = "#181D27"
TABLE_ALT_ROW = "#F5F5F5"
GREEN_TEXT = "#181D27"  # navy
GREEN_BG = "#F5F5F5"   # light blue bg

# ── Insurer data (from config) ──
# Add grey shade colors based on count ranking
_GREY_SHADES = ["#181D27", "#414651", "#535862", "#717680", "#A4A7AE",
                "#E9EAEB", "#E9EAEB", "#F5F5F5"]
INSURERS = [(name, cnt, _GREY_SHADES[i] if i < len(_GREY_SHADES) else "#F5F5F5")
            for i, (name, cnt) in enumerate(cfg.INSURERS)]
DONUT_COLORS = _GREY_SHADES[:len(cfg.INSURERS)]

# ── Application data (from config) ──
APPS = cfg.APPS

INFORCED_COUNT = sum(1 for a in APPS if a["status"] == "Inforced")
INFORCED_PREM = sum(a["prem"] for a in APPS if a["status"] == "Inforced" and isinstance(a["prem"], (int, float)))
INPROGRESS_COUNT = sum(1 for a in APPS if a["status"] in ("In Progress", "Submitted today"))
INPROGRESS_PREM = sum(a["prem"] for a in APPS if a["status"] in ("In Progress", "Submitted today") and isinstance(a["prem"], (int, float)))

PAGE1_ROWS = 10  # First page rows


def clip_str(text, max_pts, font_name="Helvetica", font_size=8.5):
    """Truncate text with ellipsis if it exceeds max_pts width."""
    if pdfmetrics.stringWidth(text, font_name, font_size) <= max_pts:
        return text
    ellipsis = "…"
    while text and pdfmetrics.stringWidth(text + ellipsis, font_name, font_size) > max_pts:
        text = text[:-1]
    return text + ellipsis


def build_donut_chart(output_path):
    """Insurer diversification donut chart."""
    labels = [name for name, cnt, clr in INSURERS]
    sizes = [cnt for name, cnt, clr in INSURERS]
    clrs = [clr for name, cnt, clr in INSURERS]
    total = sum(sizes)
    
    fig, ax = plt.subplots(figsize=(8, 7))
    wedges, _ = ax.pie(sizes, colors=clrs, startangle=130,
                       wedgeprops=dict(width=0.35, edgecolor='white', linewidth=1.5))
    
    # Center text
    ax.text(0, 0.06, f"{total}", fontsize=28, fontweight="bold", ha="center", va="center", color=NAVY)
    ax.text(0, -0.14, "apps", fontsize=16, fontweight="bold", ha="center", va="center", color=NAVY)
    
    for i, (wedge, label, size) in enumerate(zip(wedges, labels, sizes)):
        pct = size / total * 100
        ang = (wedge.theta2 + wedge.theta1) / 2
        
        if size >= 3:
            rx = 0.825 * np.cos(np.radians(ang))
            ry = 0.825 * np.sin(np.radians(ang))
            # Use dark text on light wedges, white on dark
            light_colors = {"#A4A7AE", "#E9EAEB", "#F5F5F5", "#D5D7DA", "#717680"}
            txt_color = "#181D27" if clrs[i] in light_colors else "white"
            ax.text(rx, ry, f"{pct:.0f}%", fontsize=9, fontweight="bold",
                    ha="center", va="center", color=txt_color)
        
        lx = 1.22 * np.cos(np.radians(ang))
        ly = 1.22 * np.sin(np.radians(ang))
        ha_align = "left" if lx >= 0 else "right"
        ax.text(lx, ly, label, fontsize=11, ha=ha_align, va="center", color=NAVY,
                fontstyle="italic")
    
    # Chart title removed — subsection heading in PDF serves this purpose
    
    plt.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return output_path


def draw_table_rows(c, apps_slice, start_y, table_x, col_widths, row_h):
    """Draw table rows, return y position after last row."""
    y = start_y
    uw = sum(col_widths)
    
    for i, app in enumerate(apps_slice):
        row_y = y - row_h
        
        # Background
        if app["green"]:
            c.setFillColor(colors.HexColor(GREEN_BG))
            c.rect(table_x, row_y, uw, row_h, fill=1, stroke=0)
        elif i % 2 == 1:
            c.setFillColor(colors.HexColor(TABLE_ALT_ROW))
            c.rect(table_x, row_y, uw, row_h, fill=1, stroke=0)
        
        # Format premium
        prem_str = app["prem"] if isinstance(app["prem"], str) else f"${app['prem']:,}"
        
        # Determine text style
        if app["green"]:
            font = "Helvetica-Bold"
            txt_color = colors.HexColor(GREEN_TEXT)
        else:
            font = "Helvetica"
            txt_color = colors.HexColor(BODY_TEXT)
        
        raw_vals = [app["client"], prem_str, app["insurer"], app["status"], app["products"], app["date"]]
        padding = 4 * mm  # 2mm left + 2mm right buffer
        fn = font if app["green"] else "Helvetica"
        vals = [clip_str(v, col_widths[j] - padding, fn, 8.5) for j, v in enumerate(raw_vals)]
        cx = table_x
        for j, val in enumerate(vals):
            c.setFont(fn, 8.5)
            c.setFillColor(txt_color)
            if j == 0:
                c.drawString(cx + 2 * mm, row_y + 2.2 * mm, val)
            elif j == 1:
                c.drawRightString(cx + col_widths[j] - 2 * mm, row_y + 2.2 * mm, val)
            else:
                c.drawString(cx + 2 * mm, row_y + 2.2 * mm, val)
            cx += col_widths[j]
        
        y -= row_h
    return y


def draw_table_header(c, y, table_x, col_widths, row_h):
    """Draw table header row."""
    uw = sum(col_widths)
    c.setFillColor(colors.HexColor(TABLE_HEADER_BG))
    c.rect(table_x, y - row_h, uw, row_h, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 8.5)
    headers = ["Client", "Premium", "Insurer", "Status", "Products", "Date"]
    cx = table_x
    for j, h in enumerate(headers):
        if j == 1:
            c.drawRightString(cx + col_widths[j] - 2 * mm, y - row_h + 2.5 * mm, h)
        else:
            c.drawString(cx + 2 * mm, y - row_h + 2.5 * mm, h)
        cx += col_widths[j]
    return y - row_h


def draw_section5(output_path):
    c = canvas.Canvas(output_path, pagesize=A4)
    
    col_widths = [UW * 0.28, UW * 0.11, UW * 0.14, UW * 0.14, UW * 0.21, UW * 0.12]
    row_h = 6.5 * mm
    table_x = ML
    
    # ═══════════════════════ PAGE 1 ═══════════════════════
    
    subsec_style = ParagraphStyle("subsec", fontName="Helvetica-Bold", fontSize=12,
                                   leading=16, textColor=colors.HexColor(NAVY))
    narr_style = ParagraphStyle("narr", fontName="Helvetica", fontSize=10,
                                 leading=13, textColor=colors.HexColor(BODY_TEXT))

    # Section heading
    y = H - 28 * mm
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(ML, y, "5. Insurer Diversification")

    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(ML, y, W - MR, y)

    # ── 5.1 Insurer Mix ──
    y -= 6 * mm
    s1 = Paragraph("5.1  Insurer Mix", subsec_style)
    sw, sh = s1.wrap(UW, 20)
    s1.drawOn(c, ML, y - sh)
    y -= sh + 4 * mm

    # ── Commentary ──
    insurer_summary = ", ".join([f"{name} ({cnt})" for name, cnt, _ in INSURERS if cnt >= 3])
    small_insurers = ", ".join([name for name, cnt, _ in INSURERS if cnt == 1])
    narr_text = (
        f"Strong diversification across {len(INSURERS)} insurers: {insurer_summary}"
        + (f", {small_insurers} (1 each)" if small_insurers else "")
        + ". No single-insurer concentration risk."
    )
    narr = Paragraph(narr_text, narr_style)
    pw, ph = narr.wrap(UW, 50)
    narr.drawOn(c, ML, y - ph)
    y -= ph + 6 * mm

    # ── Donut chart ──
    chart_path = output_path.replace(".pdf", "_donut.png")
    build_donut_chart(chart_path)
    chart_h = 95 * mm
    chart_w = 115 * mm
    chart_x = ML + (UW - chart_w) / 2
    c.drawImage(chart_path, chart_x, y - chart_h, width=chart_w, height=chart_h,
                preserveAspectRatio=True, anchor="nw")
    y -= chart_h + 8 * mm

    # ── February Submissions heading ──
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(ML, y, f"6. {cfg.REPORT_MONTH_NAME} Submissions \u2013 Full Detail")

    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(ML, y, W - MR, y)

    # ── 6.1 Submissions Detail ──
    y -= 6 * mm
    s2 = Paragraph("6.1  Submissions Detail", subsec_style)
    sw, sh = s2.wrap(UW, 20)
    s2.drawOn(c, ML, y - sh)
    y -= sh + 4 * mm

    # ── Commentary ──
    narr2 = Paragraph(
        f"The following table details all applications submitted in {cfg.REPORT_MONTH_NAME}, "
        f"including inforced and in-progress cases.",
        narr_style)
    pw, ph = narr2.wrap(UW, 40)
    narr2.drawOn(c, ML, y - ph)
    y -= ph + 6 * mm

    # ── Summary tiles (2-across, matching section 12 style) ──
    tile_gap = 6
    tile_w = (UW - tile_gap) / 2
    tile_h = 62
    tile_y = y - tile_h

    tiles = [
        {"label": "INFORCED", "value": f"${INFORCED_PREM:,}", "sub": f"{INFORCED_COUNT} applications",
         "value_color": "#252B37"},
        {"label": "IN PROGRESS", "value": f"${INPROGRESS_PREM:,}", "sub": f"{INPROGRESS_COUNT} in underwriting",
         "value_color": "#252B37"},
    ]

    for i, t in enumerate(tiles):
        tx = ML + i * (tile_w + tile_gap)
        c.setFillColor(colors.HexColor("#FAFAFA"))
        c.setStrokeColor(colors.HexColor("#D5D7DA"))
        c.setLineWidth(0.5)
        c.roundRect(tx, tile_y, tile_w, tile_h, 4, fill=1, stroke=1)

        c.setFont("Helvetica-Bold", 7)
        c.setFillColor(colors.HexColor(GREY_TEXT))
        c.drawCentredString(tx + tile_w/2, tile_y + tile_h - 14, t["label"])

        c.setFont("Helvetica-Bold", 22)
        c.setFillColor(colors.HexColor(t["value_color"]))
        c.drawCentredString(tx + tile_w/2, tile_y + tile_h - 38, t["value"])

        c.setFont("Helvetica-Oblique", 7)
        c.setFillColor(colors.HexColor(GREY_TEXT))
        c.drawCentredString(tx + tile_w/2, tile_y + 6, t["sub"])

    y = tile_y - 14
    
    # ── Table header ──
    y = draw_table_header(c, y, table_x, col_widths, row_h)

    # ── Table rows - page 1 (dynamic: fill available space above footer) ──
    page1_rows = max(0, int((y - 28 * mm) / row_h))
    y = draw_table_rows(c, APPS[:page1_rows], y, table_x, col_widths, row_h)
    
    # Footer
    draw_footer(c, 5, cfg.TOTAL_PAGES)
    
    # ═══════════════════════ PAGE 2 ═══════════════════════
    if not APPS[page1_rows:]:
        c.save()
        if os.path.exists(chart_path):
            os.remove(chart_path)
        return output_path
    c.showPage()

    y = H - 28 * mm

    # Table header repeat
    y = draw_table_header(c, y, table_x, col_widths, row_h)

    # Remaining rows
    y = draw_table_rows(c, APPS[page1_rows:], y, table_x, col_widths, row_h)
    
    # Footnote
    y -= 8 * mm
    fn_style = ParagraphStyle("fn", fontName="Helvetica-Oblique", fontSize=9,
                               leading=12, textColor=colors.HexColor(BODY_TEXT))
    fn_text = cfg.SUBMISSIONS_FOOTNOTE
    fn = Paragraph(fn_text, fn_style)
    fw, fh = fn.wrap(UW, 60)
    fn.drawOn(c, ML, y - fh)
    
    # Footer
    draw_footer(c, 6, cfg.TOTAL_PAGES)
    
    c.save()
    if os.path.exists(chart_path):
        os.remove(chart_path)
    return output_path


if __name__ == "__main__":
    os.makedirs("/home/claude/adviser-monthly-reports/output", exist_ok=True)
    path = draw_section5("/home/claude/adviser-monthly-reports/output/section5_sample.pdf")
    print(f"✅ {path} ({os.path.getsize(path) / 1024:.0f} KB)")
