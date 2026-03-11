from datetime import date
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, PageBreak
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from typing import Dict, Any, List


# ── colour palette ──────────────────────────────────────────────────────────
NAVY   = colors.HexColor("#1B2A4A")
TEAL   = colors.HexColor("#0E7C7B")
GREEN  = colors.HexColor("#2E7D32")
AMBER  = colors.HexColor("#E65100")
LIGHT  = colors.HexColor("#F5F7FA")
BORDER = colors.HexColor("#C8D0DC")


def _styles():
    base = getSampleStyleSheet()
    s = {}
    s["title"] = ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=20,
                                 textColor=NAVY, spaceAfter=6, spaceBefore=0, leading=24)
    s["meta"]  = ParagraphStyle("meta",  fontName="Helvetica", fontSize=9,
                                 textColor=colors.HexColor("#555555"), spaceAfter=6, leading=13)
    s["headline"] = ParagraphStyle("headline", fontName="Helvetica-Bold", fontSize=13,
                                    textColor=TEAL, spaceAfter=8, spaceBefore=4, leading=18)
    s["h2"]    = ParagraphStyle("h2", fontName="Helvetica-Bold", fontSize=12,
                                 textColor=NAVY, spaceBefore=16, spaceAfter=6, leading=16)
    s["body"]  = ParagraphStyle("body", fontName="Helvetica", fontSize=10,
                                 textColor=colors.black, leading=15, spaceAfter=6)
    s["small"] = ParagraphStyle("small", fontName="Helvetica", fontSize=8,
                                 textColor=colors.HexColor("#666666"), leading=11)
    s["conc"]  = ParagraphStyle("conc", fontName="Helvetica-Oblique", fontSize=10,
                                 textColor=colors.HexColor("#333333"), leading=14)
    s["footer"]= ParagraphStyle("footer", fontName="Helvetica", fontSize=7,
                                 textColor=colors.HexColor("#999999"), alignment=TA_CENTER)
    return s


def _esc(s: str) -> str:
    """Escape ampersands for ReportLab XML paragraph rendering."""
    return s.replace("&", "&amp;") if s else s


def _table_style_base():
    return [
        ("BACKGROUND",  (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR",   (0, 0), (-1, 0), colors.white),
        ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, 0), 9),
        ("FONTNAME",    (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",    (0, 1), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
        ("GRID",        (0, 0), (-1, -1), 0.4, BORDER),
        ("LEFTPADDING",  (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING",   (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 6),
        ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
    ]


class PDFGenerator:
    def __init__(self, region: str = "US", output_path: str = "output.pdf"):
        self.output_path = output_path
        self.pagesize = landscape(letter)
        self.width, self.height = self.pagesize
        self.margin = 0.65 * inch

    def generate_memo(self, data: Dict[str, Any]) -> str:
        doc = SimpleDocTemplate(
            self.output_path,
            pagesize=self.pagesize,
            leftMargin=self.margin, rightMargin=self.margin,
            topMargin=self.margin, bottomMargin=self.margin,
        )
        S = _styles()
        W = self.width - 2 * self.margin
        story = []

        # ══════════════════════════════════════════════════════════════════════
        # PAGE 1 — Executive Memo (1 page, CEO/CFO audience)
        # ══════════════════════════════════════════════════════════════════════

        # Memo header block
        story.append(Paragraph("MEMORANDUM", S["title"]))
        story.append(HRFlowable(width=W, thickness=2, color=NAVY, spaceAfter=10))

        header_label = ParagraphStyle("hl", fontName="Helvetica-Bold", fontSize=10,
                                      textColor=NAVY, leading=16)
        header_value = ParagraphStyle("hv", fontName="Helvetica", fontSize=10,
                                      textColor=colors.black, leading=16)

        meta_rows = [
            [Paragraph("TO:", header_label),   Paragraph("CEO &amp; CFO", header_value)],
            [Paragraph("FROM:", header_label),  Paragraph("VP of Operations", header_value)],
            [Paragraph("DATE:", header_label),  Paragraph(date.today().strftime("%d %B %Y"), header_value)],
            [Paragraph("RE:", header_label),    Paragraph(data.get("subject", "Vendor Integration Assessment"), header_value)],
        ]
        meta_table = Table(meta_rows, colWidths=[0.7*inch, W - 0.7*inch], hAlign='LEFT')
        meta_table.setStyle(TableStyle([
            ("VALIGN",       (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING",   (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 3),
            ("LEFTPADDING",  (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ]))
        story.append(meta_table)
        story.append(Spacer(1, 4))
        story.append(HRFlowable(width=W, thickness=0.5, color=BORDER, spaceAfter=12))

        # Data quality note (only if synthetic data)
        if data.get("data_note"):
            note_style = ParagraphStyle("note", fontName="Helvetica-Oblique", fontSize=9,
                                        textColor=AMBER, leading=13, spaceAfter=10)
            story.append(Paragraph(f"⚠ {data['data_note']}", note_style))

        section_label = ParagraphStyle("sl", fontName="Helvetica-Bold", fontSize=10,
                                        textColor=NAVY, spaceBefore=12, spaceAfter=4, leading=14,
                                        borderPad=0)

        # Findings
        story.append(Paragraph("FINDINGS", section_label))
        story.append(Paragraph(_esc(data.get("findings", "")), S["body"]))

        # Recommended Actions (bullet list)
        story.append(Paragraph("RECOMMENDED ACTIONS", section_label))
        bullet_style = ParagraphStyle("bullet", fontName="Helvetica", fontSize=10,
                                      textColor=colors.black, leading=15, leftIndent=10,
                                      firstLineIndent=-10, spaceAfter=4)
        rec_actions = data.get("recommended_actions", [])
        if isinstance(rec_actions, list):
            for action in rec_actions:
                story.append(Paragraph(f"• {_esc(action)}", bullet_style))
        else:
            story.append(Paragraph(_esc(rec_actions), S["body"]))
        story.append(Spacer(1, 4))

        # Risks
        story.append(Paragraph("RISKS", section_label))
        story.append(Paragraph(_esc(data.get("risks", "")), S["body"]))

        # Conclusion
        story.append(Spacer(1, 10))
        story.append(HRFlowable(width=W, thickness=0.5, color=BORDER, spaceAfter=8))
        story.append(Paragraph(_esc(data.get("conclusion", "")), S["conc"]))

        # ══════════════════════════════════════════════════════════════════════
        # PAGE 2 — Top 3 Strategic Opportunities
        # ══════════════════════════════════════════════════════════════════════
        top_opps = data.get("top_opportunity_rows", [])
        if top_opps:
            story.append(PageBreak())
            story.append(Paragraph("Top 3 Strategic Opportunities", S["title"]))
            story.append(Paragraph(
                f"Total Spend Reviewed: <b>{data.get('total_spend', '')}</b>  &nbsp;|&nbsp;  "
                f"Vendors Assessed: <b>{data.get('total_vendors', '')}</b>  &nbsp;|&nbsp;  "
                f"Date: {date.today().strftime('%d %b %Y')}",
                S["meta"]
            ))
            story.append(HRFlowable(width=W, thickness=1.5, color=NAVY, spaceAfter=16))

            opp_title_style = ParagraphStyle("ot", fontName="Helvetica-Bold", fontSize=13,
                                              textColor=NAVY, spaceBefore=0, spaceAfter=4, leading=18)
            savings_style = ParagraphStyle("sv", fontName="Helvetica-Bold", fontSize=12,
                                            textColor=GREEN, spaceAfter=6, leading=16)
            expl_style = ParagraphStyle("ex", fontName="Helvetica", fontSize=10,
                                         textColor=colors.black, leading=15, spaceAfter=4)

            for i, opp in enumerate(top_opps):
                story.append(Paragraph(f"{i+1}.  {_esc(opp['title'])}", opp_title_style))
                story.append(Paragraph(f"Estimated Annual Savings: {_esc(opp['annual_savings_usd'])}", savings_style))
                story.append(Paragraph(_esc(opp["explanation"]), expl_style))
                if i < len(top_opps) - 1:
                    story.append(HRFlowable(width=W, thickness=0.5, color=BORDER,
                                            spaceBefore=10, spaceAfter=10))

        # ══════════════════════════════════════════════════════════════════════
        # PAGE 3 — Supporting Data: Decision Rollup + Vendor Spend + Department
        # ══════════════════════════════════════════════════════════════════════
        story.append(PageBreak())
        story.append(Paragraph("Supporting Data", S["title"]))
        story.append(Paragraph(
            f"Total Spend: <b>{data.get('total_spend', '')}</b>  &nbsp;|&nbsp;  "
            f"Date: {date.today().strftime('%d %b %Y')}",
            S["meta"]
        ))
        story.append(HRFlowable(width=W, thickness=1.5, color=NAVY, spaceAfter=10))

        # Decision Rollup
        decision_rows = data.get("decision_rows", [])
        if decision_rows:
            story.append(Paragraph("Integration Decision Rollup", S["h2"]))
            tdata = [["Decision", "Vendors", "Spend"]]
            for r in decision_rows:
                tdata.append([r["decision"], str(r["vendor_count"]), r["spend"]])
            col_w = [1.6*inch, 1.0*inch, 1.4*inch]
            t = Table(tdata, colWidths=col_w, hAlign='LEFT')
            ts = _table_style_base()
            ts += [
                ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
            ]
            t.setStyle(TableStyle(ts))
            story.append(t)
            story.append(Spacer(1, 14))

        # Top Integration Recommendations (operational detail)
        opp_rows = data.get("opportunity_rows", [])
        if opp_rows:
            story.append(Paragraph("Detailed Integration Recommendations", S["h2"]))
            tdata = [["#", "Target", "Action", "Why", "Spend", "Implementation Note"]]
            for r in opp_rows:
                tdata.append([
                    str(r["priority"]),
                    Paragraph(_esc(r["target"]), S["small"]),
                    Paragraph(f"<b>{r['action'].upper()}</b><br/>{_esc(r['recommendation'])}", S["small"]),
                    Paragraph(_esc(r["why"]), S["small"]),
                    r["savings"],
                    Paragraph(_esc(r["note"]), S["small"]),
                ])
            col_w = [0.25*inch, 1.4*inch, 1.5*inch, 2.1*inch, 0.85*inch, 3.6*inch]
            t = Table(tdata, colWidths=col_w, repeatRows=1, hAlign='LEFT')
            ts = _table_style_base()
            ts += [
                ("ALIGN",    (0, 0), (0, -1), "CENTER"),
                ("ALIGN",    (4, 1), (4, -1), "RIGHT"),
                ("TEXTCOLOR",(2, 1), (2, -1), TEAL),
                ("TEXTCOLOR",(4, 1), (4, -1), GREEN),
                ("FONTNAME", (4, 1), (4, -1), "Helvetica-Bold"),
                ("VALIGN",   (0, 1), (-1, -1), "TOP"),
            ]
            t.setStyle(TableStyle(ts))
            story.append(t)
            story.append(Spacer(1, 14))

        # Top Vendor Spend
        vendor_rows = data.get("vendor_rows", [])
        if vendor_rows:
            story.append(Paragraph("Top Vendor Spend", S["h2"]))
            tdata = [["#", "Vendor", "Total Spend", "% of Total", "Records"]]
            for r in vendor_rows:
                tdata.append([str(r["rank"]), r["vendor"], r["spend"], r["pct"], str(r["transactions"])])
            col_w = [0.35*inch, 2.4*inch, 1.1*inch, 0.9*inch, 1.0*inch]
            t = Table(tdata, colWidths=col_w, hAlign='LEFT')
            ts = _table_style_base()
            ts += [("ALIGN", (0, 0), (0, -1), "CENTER"), ("ALIGN", (2, 1), (4, -1), "RIGHT")]
            t.setStyle(TableStyle(ts))
            story.append(t)
            story.append(Spacer(1, 14))

        # Spend by Department
        department_rows = data.get("department_rows", [])
        if department_rows:
            story.append(Paragraph("Spend by Department", S["h2"]))
            tdata = [["Department", "Total Spend", "% of Total", "# Vendors"]]
            for r in department_rows:
                tdata.append([r["department"], r["spend"], r["pct"], str(r["vendor_count"])])
            col_w = [1.8*inch, 1.2*inch, 0.9*inch, 0.8*inch]
            t = Table(tdata, colWidths=col_w, hAlign='LEFT')
            ts = _table_style_base()
            ts += [("ALIGN", (1, 1), (3, -1), "RIGHT")]
            t.setStyle(TableStyle(ts))
            story.append(t)

        # ══════════════════════════════════════════════════════════════════════
        # PAGE 2 — Full Classified Vendor List
        # ══════════════════════════════════════════════════════════════════════
        classified_vendor_rows = data.get("classified_vendor_rows", [])
        if classified_vendor_rows:
            story.append(PageBreak())
            story.append(Paragraph("Vendor Classification — Full List", S["title"]))
            story.append(Paragraph(
                f"All vendors classified by department, description, and integration decision  &nbsp;|&nbsp; "
                f"Date: {date.today().strftime('%d %b %Y')}",
                S["meta"]
            ))
            story.append(HRFlowable(width=W, thickness=1.5, color=NAVY, spaceAfter=10))

            ACTION_COLORS = {
                "KEEP":      colors.HexColor("#1B5E20"),
                "CENTRALIZE":   colors.HexColor("#0D47A1"),
                "ELIMINATE": colors.HexColor("#B71C1C"),
                "AUTOMATE":  colors.HexColor("#E65100"),
            }

            tdata = [["#", "Vendor", "Department", "Description", "Decision", "Spend"]]
            for i, r in enumerate(classified_vendor_rows):
                tdata.append([
                    str(i + 1),
                    Paragraph(_esc(r["vendor"]), S["small"]),
                    Paragraph(_esc(r["department"]), S["small"]),
                    Paragraph(_esc(r["description"]), S["small"]),
                    r["decision"],
                    r["spend"],
                ])

            # Vendor and Department both wrap; Description gets the most space
            col_w = [0.3*inch, 1.6*inch, 1.1*inch, 4.2*inch, 0.85*inch, 0.9*inch]
            t = Table(tdata, colWidths=col_w, repeatRows=1, hAlign='LEFT')
            ts = _table_style_base()
            ts += [
                ("ALIGN",    (0, 0), (0, -1), "CENTER"),
                ("ALIGN",    (5, 1), (5, -1), "RIGHT"),
                ("VALIGN",   (0, 1), (-1, -1), "TOP"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("TOPPADDING",    (0, 1), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
            ]
            # Colour-code the Decision column per action type
            for row_idx, r in enumerate(classified_vendor_rows, start=1):
                action = r["decision"].upper()
                c = ACTION_COLORS.get(action, colors.black)
                ts.append(("TEXTCOLOR",  (4, row_idx), (4, row_idx), c))
                ts.append(("FONTNAME",   (4, row_idx), (4, row_idx), "Helvetica-Bold"))
            t.setStyle(TableStyle(ts))
            story.append(t)

        # ══════════════════════════════════════════════════════════════════════
        # PAGE 3 — Data Audit & Cross-Check
        # ══════════════════════════════════════════════════════════════════════
        audit = data.get("audit", {})
        if audit:
            story.append(PageBreak())
            story.append(Paragraph("Data Audit & Cross-Check", S["title"]))
            story.append(Paragraph(
                f"Run ID: {audit.get('run_id', '—')}  &nbsp;|&nbsp; "
                f"Source file: <b>{audit.get('source_file', '—')}</b>  &nbsp;|&nbsp; "
                f"Generated: {date.today().strftime('%d %b %Y')}",
                S["meta"]
            ))
            story.append(HRFlowable(width=W, thickness=1.5, color=NAVY, spaceAfter=10))

            # ── Reconciliation summary ─────────────────────────────────────
            story.append(Paragraph("Reconciliation Summary", S["h2"]))
            rec = audit.get("reconciliation", [])
            if rec:
                tdata = [["Item", "Value", "Notes"]]
                for row in rec:
                    tdata.append([row["item"], row["value"], row.get("notes", "")])
                col_w = [2.2*inch, 1.4*inch, 2.6*inch]
                t = Table(tdata, colWidths=col_w, hAlign='LEFT')
                ts = _table_style_base()
                ts += [("ALIGN", (1, 1), (1, -1), "RIGHT")]
                # Highlight the checksum row
                for i, row in enumerate(rec, start=1):
                    if row.get("highlight"):
                        ts.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor("#FFF9C4")))
                        ts.append(("FONTNAME", (0, i), (-1, i), "Helvetica-Bold"))
                t.setStyle(TableStyle(ts))
                story.append(t)
                story.append(Spacer(1, 12))

            # ── Vendor × Department spend matrix ─────────────────────────
            story.append(Paragraph("Vendor × Department Spend Matrix ($)", S["h2"]))
            matrix_data = audit.get("matrix", {})
            vendors = matrix_data.get("vendors", [])
            categories = matrix_data.get("categories", [])
            cells = matrix_data.get("cells", {})   # {vendor: {category: spend_str}}
            row_totals = matrix_data.get("row_totals", {})
            col_totals = matrix_data.get("col_totals", {})
            grand_total = matrix_data.get("grand_total", "")

            if vendors and categories:
                # Header row: blank, then each category, then "TOTAL"
                hrow = ["Vendor"] + [c[:12] for c in categories] + ["TOTAL"]
                tdata = [hrow]
                for v in vendors:
                    row = [v[:18]]
                    for c in categories:
                        row.append(cells.get(v, {}).get(c, "—"))
                    row.append(row_totals.get(v, "—"))
                    tdata.append(row)
                # Totals footer row
                foot = ["TOTAL"]
                for c in categories:
                    foot.append(col_totals.get(c, "—"))
                foot.append(grand_total)
                tdata.append(foot)

                n_cols = len(hrow)
                # Distribute column widths: vendor col wider, rest equal
                vendor_col_w = 1.3 * inch
                remaining = W - vendor_col_w
                other_col_w = remaining / (n_cols - 1)
                col_w = [vendor_col_w] + [other_col_w] * (n_cols - 1)

                t = Table(tdata, colWidths=col_w, repeatRows=1, hAlign='LEFT')
                ts = _table_style_base()
                last_row = len(tdata) - 1
                last_col = n_cols - 1
                ts += [
                    ("ALIGN",      (1, 0), (-1, -1), "RIGHT"),
                    # Totals column highlighted
                    ("BACKGROUND", (last_col, 0), (last_col, -1), colors.HexColor("#E8EEF6")),
                    ("FONTNAME",   (last_col, 1), (last_col, -1), "Helvetica-Bold"),
                    # Totals row highlighted
                    ("BACKGROUND", (0, last_row), (-1, last_row), colors.HexColor("#E8EEF6")),
                    ("FONTNAME",   (0, last_row), (-1, last_row), "Helvetica-Bold"),
                    # Corner cell (grand total) extra emphasis
                    ("BACKGROUND", (last_col, last_row), (last_col, last_row), NAVY),
                    ("TEXTCOLOR",  (last_col, last_row), (last_col, last_row), colors.white),
                    ("FONTSIZE",   (0, 0), (-1, -1), 8),
                    ("FONTSIZE",   (0, 0), (-1, 0), 8),
                    ("LEFTPADDING",  (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ]
                t.setStyle(TableStyle(ts))
                story.append(t)
                story.append(Spacer(1, 12))

            # ── Top transactions sample ───────────────────────────────────
            top_txns = audit.get("top_transactions", [])
            if top_txns:
                story.append(Paragraph("Top 10 Vendor Records by Annual Spend", S["h2"]))
                tdata = [["Vendor", "Department", "Amount"]]
                for tx in top_txns:
                    tdata.append([
                        tx.get("vendor", ""),
                        tx.get("department", ""),
                        tx.get("amount", ""),
                    ])
                col_w = [2.5*inch, 1.5*inch, 1.2*inch]
                t = Table(tdata, colWidths=col_w, hAlign='LEFT')
                ts = _table_style_base()
                ts += [("ALIGN", (2, 1), (2, -1), "RIGHT")]
                t.setStyle(TableStyle(ts))
                story.append(t)
                story.append(Spacer(1, 12))

            story.append(HRFlowable(width=W, thickness=0.5, color=BORDER, spaceAfter=6))
            story.append(Paragraph(
                "Cross-check: sum of all Vendor × Department cells must equal the grand total above and the Total Spend on page 1.",
                S["footer"]
            ))

        doc.build(story)
        return self.output_path
