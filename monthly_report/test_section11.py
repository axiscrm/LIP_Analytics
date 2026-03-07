"""
Section 11: Your Strongest Predictor + Pipeline by Engagement Level
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
GREEN = "#252B37"  # dark blue for highlights
GREY_TEXT = "#717680"
BODY_TEXT = "#535862"


def make_pipeline_chart(out_path):
    # Data: segment, leads, conv_rate, est_premium_$K
    # Est premium = leads × conv_rate × avg_quote_value_of_converted ($2,755)
    segments = [s for s in cfg.PIPELINE_SEGMENTS if s[1] > 0]

    # If no segments have leads, create a placeholder chart
    if not segments:
        fig, ax = plt.subplots(figsize=(6.5, 2.8))
        ax.text(0.5, 0.5, "No pipeline data available for this period",
                ha='center', va='center', fontsize=10, color=GREY_TEXT,
                transform=ax.transAxes)
        ax.set_axis_off()
        fig.savefig(out_path, dpi=200, bbox_inches='tight', facecolor='white')
        plt.close()
        return out_path

    bar_colors = ["#252B37", "#414651", "#717680", "#D5D7DA"]

    fig, ax = plt.subplots(figsize=(6.5, 2.8))

    labels = [s[0] for s in segments]
    values = [s[3] for s in segments]
    y_pos = np.arange(len(segments))

    bars = ax.barh(y_pos, values, height=0.55, color=bar_colors[:len(segments)], edgecolor='none')

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=8, color=BODY_TEXT)
    ax.invert_yaxis()
    ax.set_xlabel("Estimated Premium Value ($K)", fontsize=8, color=GREY_TEXT)
    # Chart title removed — subsection heading in PDF serves this purpose

    # Value labels
    for i, (seg_name, leads, rate, val) in enumerate(segments):
        ax.text(val + 1.5, i, f"${val}K  ({leads} leads × {rate})",
                va='center', fontsize=7.5, color=BODY_TEXT)

    max_val = max(values) if values else 10
    ax.set_xlim(0, max_val * 1.8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#D5D7DA')
    ax.spines['bottom'].set_color('#D5D7DA')
    ax.tick_params(axis='x', colors=GREY_TEXT, labelsize=7.5)
    ax.grid(axis='x', alpha=0.3, linewidth=0.5)
    
    fig.savefig(out_path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()
    return out_path


def draw_section11(output_path):
    c = canvas.Canvas(output_path, pagesize=A4)
    
    
    y = H - 28 * mm

    # ── Section heading ──
    c.setFont("Helvetica-Bold", 16)
    c.setFillColor(colors.HexColor(NAVY))
    c.drawString(ML, y, "11. Your Strongest Predictor")
    y -= 4 * mm
    c.setStrokeColor(colors.HexColor("#E9EAEB"))
    c.setLineWidth(0.3)
    c.line(ML, y, W - MR, y)
    y -= 6 * mm

    subsec_style = ParagraphStyle("subsec", fontName="Helvetica-Bold", fontSize=12,
                                   leading=16, textColor=colors.HexColor(NAVY))
    narr_style = ParagraphStyle("narr", fontName="Helvetica", fontSize=10,
                                 leading=14, textColor=colors.HexColor(BODY_TEXT))

    # ── 11.1 Getting to a Quote ──
    h = Paragraph("11.1  Getting to a Quote", subsec_style)
    hw, hh = h.wrap(UW, 30)
    h.drawOn(c, ML, y - hh)
    y -= hh + 4 * mm

    # ── Commentary ──
    p1 = Paragraph(cfg.PREDICTOR_NARRATIVE_1, narr_style)
    pw, ph = p1.wrap(UW, 60)
    p1.drawOn(c, ML, y - ph)
    y -= ph + 5 * mm

    stale_count = cfg.STALE_QUOTED_COUNT
    est_premium = cfg.STALE_EST_PREMIUM
    narr2_text = cfg.PREDICTOR_NARRATIVE_2_TEMPLATE.format(
        stale_count=stale_count, est_premium=est_premium
    )
    p2 = Paragraph(narr2_text, narr_style)
    pw2, ph2 = p2.wrap(UW, 60)
    p2.drawOn(c, ML, y - ph2)
    y -= ph2 + 10 * mm

    # ── 11.2 Pipeline by Engagement Level ──
    s2 = Paragraph("11.2  Pipeline by Engagement Level", subsec_style)
    sw, sh = s2.wrap(UW, 20)
    s2.drawOn(c, ML, y - sh)
    y -= sh + 4 * mm

    narr3 = Paragraph(
        "The chart below shows your current open pipeline segmented by engagement level, "
        "with estimated premium value based on your own historical conversion rates.",
        narr_style)
    pw, ph = narr3.wrap(UW, 50)
    narr3.drawOn(c, ML, y - ph)
    y -= ph + 6 * mm

    # ── Chart ──
    chart_path = output_path.replace('.pdf', '_chart.png')
    make_pipeline_chart(chart_path)

    chart_w = UW
    chart_h = chart_w * 0.43
    c.drawImage(chart_path, ML, y - chart_h, width=chart_w, height=chart_h)
    y -= chart_h + 8 * mm
    
    # Closing italic paragraph
    italic_style = ParagraphStyle("italic_close", fontName="Helvetica-Oblique", fontSize=10,
                                   leading=14, textColor=colors.HexColor(BODY_TEXT))
    closing = Paragraph(cfg.PREDICTOR_CLOSING, italic_style)
    cw, ch = closing.wrap(UW, 60)
    closing.drawOn(c, ML, y - ch)
    
    # Footer
    draw_footer(c, 11 - (0 if getattr(cfg, "HAS_PAGE6", True) else 1), cfg.TOTAL_PAGES)
    
    c.save()
    os.remove(chart_path)
    return output_path


if __name__ == "__main__":
    os.makedirs("/home/claude/adviser-monthly-reports/output", exist_ok=True)
    path = draw_section11("/home/claude/adviser-monthly-reports/output/section11_sample.pdf")
    print(f"\u2705 {path} ({os.path.getsize(path) / 1024:.0f} KB)")
