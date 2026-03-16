"""
tests/create_test_pdf.py
------------------------
Generates a realistic sample IEPF PDF using ReportLab, mimicking the
Tech Mahindra shareholder data format.

Usage
-----
    python tests/create_test_pdf.py

Output
------
    tests/sample_pdfs/TechMahindra_IEPF_2017-2018.pdf

Install reportlab first if not already installed:
    pip install reportlab
"""

from __future__ import annotations

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


# ---------------------------------------------------------------------------
# Sample IEPF shareholder data — Tech Mahindra format
# ---------------------------------------------------------------------------

COLUMNS = [
    "Sr No",
    "Folio No",
    "Name",
    "Current Holding",
    "Final Dividend Amount\nFY 2017-2018 (₹)",
]

ROWS = [
    ["1", "TM00012345", "RAJESH KUMAR SHARMA",         "250",  "1,875.00"],
    ["2", "TM00023456", "PRIYA VENKATARAMAN",          "100",    "750.00"],
    ["3", "TM00034567", "SURESH BABU NAIR",            "500",  "3,750.00"],
    ["4", "TM00045678", "ANITA DEVI AGARWAL",          "75",     "562.50"],
    ["5", "TM00056789", "MOHAMMAD ARIF QURESHI",       "320",  "2,400.00"],
]

# Column widths (must sum to usable page width ≈ 25 cm in landscape A4)
COL_WIDTHS = [1.5 * cm, 3.5 * cm, 9 * cm, 3.5 * cm, 5.5 * cm]


def build_pdf(output_path: str | Path) -> Path:
    """Generate the sample IEPF PDF at *output_path*.

    Args:
        output_path: Destination file path (parent directories are created
                     automatically).

    Returns:
        The resolved :class:`Path` of the created PDF.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(out),
        pagesize=landscape(A4),
        rightMargin=1.5 * cm,
        leftMargin=1.5 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )

    styles = getSampleStyleSheet()

    # ----- Styles -----
    title_style = ParagraphStyle(
        "Title",
        parent=styles["Heading1"],
        fontSize=14,
        leading=18,
        spaceAfter=4,
        textColor=colors.HexColor("#1a3c5e"),
    )
    subtitle_style = ParagraphStyle(
        "Subtitle",
        parent=styles["Normal"],
        fontSize=10,
        leading=14,
        spaceAfter=2,
        textColor=colors.HexColor("#444444"),
    )
    cell_style = ParagraphStyle(
        "Cell",
        parent=styles["Normal"],
        fontSize=8,
        leading=11,
        wordWrap="CJK",
    )

    def cell(text: str) -> Paragraph:
        return Paragraph(str(text), cell_style)

    # ----- Header paragraphs -----
    elements = [
        Paragraph("Tech Mahindra Limited", title_style),
        Paragraph(
            "Investor Education and Protection Fund (IEPF) — "
            "Statement of Unclaimed / Unpaid Dividend",
            subtitle_style,
        ),
        Paragraph("Financial Year: 2017-2018", subtitle_style),
        Paragraph(
            "CIN: L64200MH1986PLC041370  |  "
            "Registered Office: Gateway Building, Apollo Bunder, Mumbai - 400 001",
            subtitle_style,
        ),
        Spacer(1, 0.5 * cm),
    ]

    # ----- Table -----
    header_row = [cell(col) for col in COLUMNS]
    data_rows = [[cell(v) for v in row] for row in ROWS]
    table_data = [header_row] + data_rows

    table = Table(table_data, colWidths=COL_WIDTHS, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                # Header background
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a3c5e")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                ("VALIGN", (0, 0), (-1, 0), "MIDDLE"),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                ("TOPPADDING", (0, 0), (-1, 0), 8),
                # Alternating row colours
                ("ROWBACKGROUNDS", (0, 1), (-1, -1),
                 [colors.white, colors.HexColor("#eaf0f6")]),
                # Borders
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#c0c0c0")),
                ("LINEBELOW", (0, 0), (-1, 0), 1.5, colors.HexColor("#1a3c5e")),
                # Data alignment
                ("ALIGN", (0, 1), (1, -1), "CENTER"),   # Sr No, Folio
                ("ALIGN", (3, 1), (-1, -1), "RIGHT"),   # Holding, Amount
                ("VALIGN", (0, 1), (-1, -1), "MIDDLE"),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("TOPPADDING", (0, 1), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 1), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    elements.append(table)

    # ----- Footer note -----
    elements.append(Spacer(1, 0.5 * cm))
    note_style = ParagraphStyle(
        "Note",
        parent=styles["Normal"],
        fontSize=7,
        textColor=colors.HexColor("#888888"),
    )
    elements.append(
        Paragraph(
            "Note: This statement is generated for testing purposes only. "
            "Shareholders may claim their dividend by contacting the Company's "
            "Registrar & Share Transfer Agent.",
            note_style,
        )
    )

    doc.build(elements)
    return out


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    output = Path("tests/sample_pdfs/TechMahindra_IEPF_2017-2018.pdf")
    result = build_pdf(output)
    print(f"✅ PDF created: {result.resolve()}")
    print(f"   Size: {result.stat().st_size / 1024:.1f} KB")
