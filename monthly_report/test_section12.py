"""
Section 12: A Note on CRM Logging + What Stands Out This Month + Milestone Banner
Final page of the adviser report.
"""
import os
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
GREEN = "#252B37"
GOLD = "#414651"
GREY_TEXT = "#717680"
BODY_TEXT = "#535862"


def draw_section12(output_path):
    c = canvas.Canvas(output_path, pagesize=A4)

    page_num = 12 - (0 if getattr(cfg, "HAS_PAGE6", True) else 1)
    draw_footer(c, page_num, cfg.TOTAL_PAGES)

    y = H - 28 * mm

    style_subsec = ParagraphStyle("subsec", fontName="Helvetica-Bold", fontSize=12,
                                   textColor=colors.HexColor(NAVY), leading=16)
    style_body = ParagraphStyle("body", fontName="Helvetica", fontSize=10,
                                 textColor=colors.HexColor(BODY_TEXT), leading=14)

    # ── Section heading ──
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(ML, y, f"12. What Stands Out This {cfg.REPORT_MONTH_NAME}")
    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(ML, y, W - MR, y)
    y -= 6 * mm

    # ── 12.1 Conclusion ──
    s2 = Paragraph("12.1  Conclusion", style_subsec)
    pw, ph = s2.wrap(UW, 20)
    s2.drawOn(c, ML, y - ph)
    y -= ph + 4 * mm

    # ── Conclusion narrative (AI-generated wrap-up) ──
    conclusion_narr = getattr(cfg, "CONCLUSION_NARRATIVE", "")
    if conclusion_narr:
        conc_p = Paragraph(conclusion_narr, style_body)
        cw, ch = conc_p.wrap(UW, 200)
        conc_p.drawOn(c, ML, y - ch)
        y -= ch + 6 * mm

    # ── Key Highlights ──
    highlight_heading = Paragraph("Key Highlights", style_subsec)
    hw, hh = highlight_heading.wrap(UW, 20)
    highlight_heading.drawOn(c, ML, y - hh)
    y -= hh + 4 * mm

    bullet_style = ParagraphStyle("bullet", fontName="Helvetica", fontSize=10,
                                   textColor=colors.HexColor(BODY_TEXT), leading=14,
                                   leftIndent=18, firstLineIndent=-18)

    for highlight in cfg.HIGHLIGHTS:
        bp = Paragraph(f"★  {highlight}", bullet_style)
        bw, bh = bp.wrap(UW, 200)
        bp.drawOn(c, ML, y - bh)
        y -= bh + 6

    y -= 8

    # ── Milestone banner OR Callout box ──
    if getattr(cfg, "SHOW_MILESTONE", False) and cfg.MILESTONE_TEXT:
        # $100k+ month — full celebration banner
        c.setStrokeColor(colors.HexColor(NAVY))
        c.setLineWidth(1.5)
        c.line(ML, y, W - MR, y)
        y -= 8 * mm

        milestone_title_style = ParagraphStyle(
            "ms_title", fontName="Helvetica-Bold", fontSize=22,
            leading=26, textColor=colors.HexColor(GREEN), alignment=1)
        mt = Paragraph(cfg.MILESTONE_TEXT, milestone_title_style)
        mw, mh = mt.wrap(UW, 40)
        mt.drawOn(c, ML, y - mh)
        y -= mh + 4 * mm

        if cfg.MILESTONE_SUB:
            milestone_sub_style = ParagraphStyle(
                "ms_sub", fontName="Helvetica", fontSize=10,
                leading=14, textColor=colors.HexColor(BODY_TEXT), alignment=1)
            ms = Paragraph(cfg.MILESTONE_SUB, milestone_sub_style)
            sw, sh = ms.wrap(UW, 60)
            ms.drawOn(c, ML, y - sh)
            y -= sh + 8 * mm

        c.setStrokeColor(colors.HexColor(NAVY))
        c.setLineWidth(1.5)
        c.line(ML, y, W - MR, y)

    elif getattr(cfg, "CALLOUT_TEXT", ""):
        # Sub-$100k month — key stat callout + encouragement
        callout_text = cfg.CALLOUT_TEXT
        callout_sub = getattr(cfg, "CALLOUT_SUB", "")
        box_h = 48 * mm
        box_y = y - box_h
        c.setFillColor(colors.HexColor("#F5F5F5"))
        c.setStrokeColor(colors.HexColor("#D5D7DA"))
        c.setLineWidth(0.5)
        c.roundRect(ML, box_y, UW, box_h, 4, fill=1, stroke=1)

        c.setFont("Helvetica-Bold", 16)
        c.setFillColor(colors.HexColor(NAVY))
        c.drawCentredString(W / 2, box_y + box_h - 18 * mm, callout_text)

        if callout_sub:
            callout_style = ParagraphStyle("callout", fontName="Helvetica", fontSize=10,
                                            leading=14, textColor=colors.HexColor(BODY_TEXT),
                                            alignment=1)
            cp = Paragraph(callout_sub, callout_style)
            cw, ch = cp.wrap(UW - 16 * mm, 30)
            cp.drawOn(c, ML + 8 * mm, box_y + 8 * mm)

    c.save()
    return output_path


# Keep alias for any legacy callers
def build_section12():
    output = os.path.join(os.path.dirname(__file__), "output", "section12_sample.pdf")
    os.makedirs(os.path.dirname(output), exist_ok=True)
    return draw_section12(output)


if __name__ == "__main__":
    path = build_section12()
    print(f"✅ {path} ({os.path.getsize(path) / 1024:.0f} KB)")
