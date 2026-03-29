import csv
import io
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT


def export_to_csv(data: list[dict], filepath: str) -> None:
    if not data:
        raise ValueError("No data to export.")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


def export_to_csv_string(data: list[dict]) -> str:
    if not data:
        raise ValueError("No data to export.")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def _build_pdf_table(data: list[dict]) -> Table:
    headers = list(data[0].keys())
    table_data = [headers]
    for row in data:
        table_data.append([str(v) for v in row.values()])

    table = Table(table_data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2C3E50")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING", (0, 0), (-1, 0), 8),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("ALIGN", (0, 1), (-1, -1), "CENTER"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F2F3F4")]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#BDC3C7")),
        ("TOPPADDING", (0, 1), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 5),
    ]))
    return table


def export_to_pdf(
    data: list[dict],
    filepath: str,
    title: str = "Report",
    subtitle: str = None,
) -> None:
    if not data:
        raise ValueError("No data to export.")

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontSize=16,
        textColor=colors.HexColor("#2C3E50"),
        alignment=TA_CENTER,
        spaceAfter=4,
    )
    subtitle_style = ParagraphStyle(
        "ReportSubtitle",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#7F8C8D"),
        alignment=TA_CENTER,
        spaceAfter=16,
    )
    footer_style = ParagraphStyle(
        "Footer",
        parent=styles["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#95A5A6"),
        alignment=TA_LEFT,
    )

    doc = SimpleDocTemplate(
        filepath,
        pagesize=A4,
        rightMargin=1.5 * cm,
        leftMargin=1.5 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )

    elements = []
    elements.append(Paragraph(title, title_style))

    if subtitle:
        elements.append(Paragraph(subtitle, subtitle_style))
    else:
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elements.append(Paragraph(f"Generated at {generated_at}", subtitle_style))

    elements.append(_build_pdf_table(data))
    elements.append(Spacer(1, 0.5 * cm))
    elements.append(Paragraph(f"Total records: {len(data)}", footer_style))

    doc.build(elements)


def export_to_pdf_bytes(
    data: list[dict],
    title: str = "Report",
    subtitle: str = None,
) -> bytes:
    if not data:
        raise ValueError("No data to export.")

    buffer = io.BytesIO()
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontSize=16,
        textColor=colors.HexColor("#2C3E50"),
        alignment=TA_CENTER,
        spaceAfter=4,
    )
    subtitle_style = ParagraphStyle(
        "ReportSubtitle",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#7F8C8D"),
        alignment=TA_CENTER,
        spaceAfter=16,
    )
    footer_style = ParagraphStyle(
        "Footer",
        parent=styles["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#95A5A6"),
        alignment=TA_LEFT,
    )

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=1.5 * cm,
        leftMargin=1.5 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )

    elements = []
    elements.append(Paragraph(title, title_style))

    if subtitle:
        elements.append(Paragraph(subtitle, subtitle_style))
    else:
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elements.append(Paragraph(f"Generated at {generated_at}", subtitle_style))

    elements.append(_build_pdf_table(data))
    elements.append(Spacer(1, 0.5 * cm))
    elements.append(Paragraph(f"Total records: {len(data)}", footer_style))

    doc.build(elements)
    return buffer.getvalue()