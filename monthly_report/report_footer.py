"""report_footer.py — shared footer renderer for all report sections."""
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.pagesizes import A4

W, H = A4
GREY_TEXT = "#717680"

DISCLAIMER_1 = (
    "Beta Version: This report is an early release and is based on your CRM recorded activity."
)
DISCLAIMER_2 = (
    "Data should be considered directional and may evolve as we refine and validate inputs through your feedback."
)


def draw_footer(c, page_num, total_pages):
    """Draw standardised footer on the current canvas page."""
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.HexColor(GREY_TEXT))
    c.drawCentredString(
        W / 2, 21 * mm,
        f"Private & Confidential  |  Page {page_num} of {total_pages}  |  Version 1.0"
    )
    c.setFont("Helvetica", 6)
    c.drawCentredString(W / 2, 15.5 * mm, DISCLAIMER_1)
    c.drawCentredString(W / 2, 11.5 * mm, DISCLAIMER_2)
