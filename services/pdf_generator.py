from datetime import date
from reportlab.lib.pagesizes import letter
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
        self.pagesize = letter
        self.width, self.height = self.pagesize
        self.margin = 0.65 * inch

    def generate_memo(self, data: Dict[str, Any]) -> str:
        """
        Generates the vendor spend executive memo PDF.

        Expected data keys:
          headline, executive_summary, conclusion  — LLM-written strings
          total_spend                              — formatted string e.g. "$1,240,581"
          company                                  — string
          vendor_rows     — list of {rank, vendor, spend, pct, transactions}
          category_rows   — list of {category, spend, vendors, top_vendor, top_pct}
          opportunity_rows— list of {priority, target, action, savings, rationale}
          data_note       — optional warning string
        """
        doc = SimpleDocTemplate(
            self.output_path,
            pagesize=self.pagesize,
            leftMargin=self.margin, rightMargin=self.margin,
            topMargin=self.margin, bottomMargin=self.margin,
        )
        S = _styles()
        W = self.width - 2 * self.margin
        story = []

        # ── Header ────────────────────────────────────────────────────────────
        story.append(Paragraph("Vendor Spend Analysis", S["title"]))
        story.append(Paragraph(
            f"Prepared for: {data.get('company', 'Executive Team')}  &nbsp;|&nbsp; "
            f"Total Spend Reviewed: <b>{data.get('total_spend', '')}</b>  &nbsp;|&nbsp; "
            f"Date: {date.today().strftime('%d %b %Y')}",
            S["meta"]
        ))
        story.append(HRFlowable(width=W, thickness=1.5, color=NAVY, spaceAfter=8))

        # ── Data quality note ─────────────────────────────────────────────────
        if data.get("data_note"):
            note_style = ParagraphStyle("note", fontName="Helvetica-Oblique", fontSize=9,
                                        textColor=AMBER, leading=13, spaceAfter=8)
            story.append(Paragraph(f"⚠ {data['data_note']}", note_style))

        # ── Headline ──────────────────────────────────────────────────────────
        if data.get("headline"):
            story.append(Paragraph(data["headline"], S["headline"]))
            story.append(Spacer(1, 4))

        # ── Executive Summary ─────────────────────────────────────────────────
        story.append(Paragraph("Executive Summary", S["h2"]))
        story.append(Paragraph(data.get("executive_summary", ""), S["body"]))
        story.append(Spacer(1, 6))

        # ── Vendor Spend Table ────────────────────────────────────────────────
        vendor_rows = data.get("vendor_rows", [])
        if vendor_rows:
            story.append(Paragraph("Vendor Spend Breakdown", S["h2"]))
            tdata = [["#", "Vendor", "Total Spend", "% of Total", "Transactions"]]
            for r in vendor_rows:
                tdata.append([
                    str(r["rank"]),
                    r["vendor"],
                    r["spend"],
                    r["pct"],
                    str(r["transactions"]),
                ])
            col_w = [0.35*inch, 2.4*inch, 1.1*inch, 0.9*inch, 1.0*inch]
            t = Table(tdata, colWidths=col_w, hAlign='LEFT')
            ts = _table_style_base()
            ts += [
                ("ALIGN", (0, 0), (0, -1), "CENTER"),
                ("ALIGN", (2, 1), (4, -1), "RIGHT"),
            ]
            t.setStyle(TableStyle(ts))
            story.append(t)
            story.append(Spacer(1, 10))

        # ── Category Spend Table ──────────────────────────────────────────────
        cat_rows = data.get("category_rows", [])
        if cat_rows:
            story.append(Paragraph("Spend by Category", S["h2"]))
            tdata = [["Category", "Total Spend", "# Vendors", "Largest Vendor", "Vendor Share"]]
            for r in cat_rows:
                tdata.append([
                    r["category"],
                    r["spend"],
                    str(r["vendors"]),
                    r.get("top_vendor", "—"),
                    r.get("top_pct", "—"),
                ])
            col_w = [1.6*inch, 1.1*inch, 0.75*inch, 1.7*inch, 0.95*inch]
            t = Table(tdata, colWidths=col_w, hAlign='LEFT')
            ts = _table_style_base()
            ts += [("ALIGN", (1, 1), (-1, -1), "RIGHT")]
            t.setStyle(TableStyle(ts))
            story.append(t)
            story.append(Spacer(1, 10))

        # ── Opportunities Table ───────────────────────────────────────────────
        opp_rows = data.get("opportunity_rows", [])
        if opp_rows:
            story.append(Paragraph("Savings Opportunities", S["h2"]))
            tdata = [["#", "Target", "Action", "Est. Savings", "Rationale"]]
            for r in opp_rows:
                tdata.append([
                    str(r["priority"]),
                    r["target"],
                    r["action"].upper(),
                    r["savings"],
                    Paragraph(r["rationale"], S["small"]),
                ])
            col_w = [0.3*inch, 1.3*inch, 0.85*inch, 1.15*inch, 2.55*inch]
            t = Table(tdata, colWidths=col_w, repeatRows=1, hAlign='LEFT')
            ts = _table_style_base()
            ts += [
                ("ALIGN",  (0, 0), (2, -1), "CENTER"),
                ("ALIGN",  (3, 1), (3, -1), "RIGHT"),
                ("TEXTCOLOR", (2, 1), (2, -1), TEAL),
                ("FONTNAME",  (2, 1), (2, -1), "Helvetica-Bold"),
                ("TEXTCOLOR", (3, 1), (3, -1), GREEN),
                ("FONTNAME",  (3, 1), (3, -1), "Helvetica-Bold"),
                ("VALIGN",  (0, 1), (-1, -1), "TOP"),
            ]
            t.setStyle(TableStyle(ts))
            story.append(t)
            story.append(Spacer(1, 10))

        # ── Conclusion ────────────────────────────────────────────────────────
        if data.get("conclusion"):
            story.append(HRFlowable(width=W, thickness=0.5, color=BORDER, spaceAfter=6))
            story.append(Paragraph("Conclusion", S["h2"]))
            story.append(Paragraph(data["conclusion"], S["conc"]))

        # ── Footer note ───────────────────────────────────────────────────────
        story.append(Spacer(1, 20))
        story.append(Paragraph(
            "Full vendor data, drill-down by category, and Q&A analysis available via the MCP server at vendor.ssc.one/mcp",
            S["footer"]
        ))

        # ══════════════════════════════════════════════════════════════════════
        # PAGE 2 — Data Audit & Cross-Check
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

            # ── Vendor × Category spend matrix ────────────────────────────
            story.append(Paragraph("Vendor × Category Spend Matrix ($)", S["h2"]))
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
                story.append(Paragraph("Top 10 Transactions (by spend)", S["h2"]))
                tdata = [["Vendor", "Category", "Amount", "Date"]]
                for tx in top_txns:
                    tdata.append([
                        tx.get("vendor", ""),
                        tx.get("category", ""),
                        tx.get("amount", ""),
                        tx.get("date", ""),
                    ])
                col_w = [2.0*inch, 1.5*inch, 1.1*inch, 1.1*inch]
                t = Table(tdata, colWidths=col_w, hAlign='LEFT')
                ts = _table_style_base()
                ts += [("ALIGN", (2, 1), (3, -1), "RIGHT")]
                t.setStyle(TableStyle(ts))
                story.append(t)
                story.append(Spacer(1, 12))

            story.append(HRFlowable(width=W, thickness=0.5, color=BORDER, spaceAfter=6))
            story.append(Paragraph(
                "Cross-check: sum of all Vendor × Category cells must equal the grand total above and the Total Spend on page 1.",
                S["footer"]
            ))

        doc.build(story)
        return self.output_path

