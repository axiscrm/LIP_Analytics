"""
Section 9: Remaining Quoted Pipeline
Quoted leads (status=3) with no application yet, ordered by quote_value DESC
Table: Client | Quoted | Referral Partner | Status
"""
import os
import matplotlib
matplotlib.use("Agg")
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
import report_config as cfg
from report_footer import draw_footer


def clip_str(text, max_pts, font_name="Helvetica", font_size=8.5):
    if pdfmetrics.stringWidth(text, font_name, font_size) <= max_pts:
        return text
    ellipsis = "…"
    while text and pdfmetrics.stringWidth(text + ellipsis, font_name, font_size) > max_pts:
        text = text[:-1]
    return text + ellipsis

W, H = A4
ML = 28 * mm
MR = 28 * mm
UW = W - ML - MR

NAVY = "#181D27"
GREY_TEXT = "#717680"
BODY_TEXT = "#535862"
ALT_ROW = "#F5F5F5"

PIPELINE = cfg.PIPELINE

# Columns: Client (35%), Referral Partner (40%), Last Quote (25%)
COL_W = [UW * 0.35, UW * 0.40, UW * 0.25]
ROW_H = 7 * mm
HDR_H = 8 * mm


def draw_table_header(c, y):
    """Draw navy header row."""
    c.setFillColor(colors.HexColor(NAVY))
    c.rect(ML, y - HDR_H, UW, HDR_H, fill=1, stroke=0)
    
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 9)
    headers = ["Client", "Referral Partner", "Last Quote"]
    x = ML
    for i, hdr in enumerate(headers):
        c.drawString(x + 3 * mm, y - HDR_H + 2.5 * mm, hdr)
        x += COL_W[i]
    return y - HDR_H


def draw_table_row(c, y, row, idx):
    """Draw a single data row."""
    bg = colors.HexColor(ALT_ROW) if idx % 2 == 1 else colors.white
    c.setFillColor(bg)
    c.rect(ML, y - ROW_H, UW, ROW_H, fill=1, stroke=0)
    
    # Light border
    c.setStrokeColor(colors.HexColor("#D5D7DA"))
    c.setLineWidth(0.3)
    c.line(ML, y - ROW_H, ML + UW, y - ROW_H)
    
    c.setFont("Helvetica", 8.5)
    c.setFillColor(colors.HexColor(BODY_TEXT))
    x = ML
    
    last_q = row.get("last_quoted", 0)
    vals = [
        clip_str(row["client"], COL_W[0] - 6 * mm),
        clip_str(row["source"], COL_W[1] - 6 * mm),
        f"${last_q:,}" if last_q else "—",
    ]
    for i, val in enumerate(vals):
        c.drawString(x + 3 * mm, y - ROW_H + 2.2 * mm, val)
        x += COL_W[i]
    
    return y - ROW_H


def draw_section9(output_path):
    c = canvas.Canvas(output_path, pagesize=A4)
    
    # ── PAGE 1 ──
    
    # Section heading
    y = H - 28 * mm
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(ML, y, "9. Remaining Quoted Pipeline")

    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(ML, y, W - MR, y)
    
    y -= 8 * mm

    import calendar as _cal
    next_month_name = _cal.month_name[(cfg.REPORT_MONTH % 12) + 1]
    subsec_style = ParagraphStyle("subsec", fontName="Helvetica-Bold", fontSize=12,
                                   leading=16, textColor=colors.HexColor(NAVY))
    narr_style = ParagraphStyle("narr", fontName="Helvetica", fontSize=10,
                                 leading=14, textColor=colors.HexColor(BODY_TEXT))
    total_style = ParagraphStyle("total", fontName="Helvetica", fontSize=10,
                                  leading=14, textColor=colors.HexColor(BODY_TEXT))

    # ── 9.1 Subsection heading ──
    s = Paragraph("9.1  Pipeline", subsec_style)
    _, sh = s.wrap(UW, 20)
    s.drawOn(c, ML, y - sh)
    y -= sh + 4 * mm

    # ── Commentary ──
    narr = Paragraph(
        f"The following clients have been quoted but do not yet have an application submitted. "
        f"These represent additional upside beyond the ${cfg.KPI_TOTAL_SUBMITTED_RAW:,} "
        f"already submitted in {cfg.REPORT_MONTH_NAME}.",
        narr_style)
    _, nh = narr.wrap(UW, 60)
    narr.drawOn(c, ML, y - nh)
    y -= nh + 6 * mm

    if not PIPELINE:
        nd = Paragraph("No quoted leads without an application found in the CRM.", narr_style)
        _, ndh = nd.wrap(UW, 30)
        nd.drawOn(c, ML, y - ndh)
    else:
        # ── Table ──
        y = draw_table_header(c, y)
        for idx, row in enumerate(PIPELINE):
            y = draw_table_row(c, y, row, idx)
    
    # Footer
    draw_footer(c, 9 - (0 if getattr(cfg, "HAS_PAGE6", True) else 1), cfg.TOTAL_PAGES)
    
    c.save()
    return output_path


if __name__ == "__main__":
    os.makedirs("/home/claude/adviser-monthly-reports/output", exist_ok=True)
    path = draw_section9("/home/claude/adviser-monthly-reports/output/section9_sample.pdf")
    print(f"✅ {path} ({os.path.getsize(path) / 1024:.0f} KB)")
