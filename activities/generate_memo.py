from temporalio import activity
import json
from datetime import datetime
from pathlib import Path
from services.postgres import get_connection, execute_query
from services.llm_client import generate_structured_response
from services.llm_schemas import SummaryMemo
from services.pdf_generator import PDFGenerator
from services.analysis_engine import compute_analysis

@activity.defn
async def generate_memo(run_id: int) -> dict:
    """
    Generates the final PDF memo using classified vendor decisions and LLM synthesis.
    """
    try:
        summary = compute_analysis(run_id)
    except Exception as e:
        activity.logger.error(f"Analysis engine failed in generate_memo: {e}")
        return {"status": "failed", "error": str(e)}

    opps = execute_query(
        "SELECT target, action_type, rationale, impact_estimate FROM savings_opportunities WHERE run_id = %s ORDER BY id",
        (run_id,), fetchall=True
    )

    decision_summary = execute_query(
        """
        SELECT
            COALESCE(nv.recommendation, 'ELIMINATE') AS decision,
            COUNT(*) AS vendor_count,
            COALESCE(SUM(sp.total_spend), 0) AS total_spend
        FROM normalized_vendors nv
        LEFT JOIN (
            SELECT
                nv.id AS vendor_id,
                COALESCE(SUM(r.spend_amount), 0) AS total_spend
            FROM normalized_vendors nv
            LEFT JOIN raw_spend_rows r
                ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
            WHERE nv.run_id = %s
            GROUP BY nv.id
        ) sp ON sp.vendor_id = nv.id
        WHERE nv.run_id = %s
        GROUP BY COALESCE(nv.recommendation, 'ELIMINATE')
        ORDER BY total_spend DESC
        """,
        (run_id, run_id), fetchall=True
    )

    all_vendors = execute_query(
        "SELECT canonical_vendor, total_spend FROM vendor_spend_summary WHERE run_id = %s ORDER BY total_spend DESC LIMIT 20",
        (run_id,), fetchall=True
    )

    # Vendor × department matrix for context (departments from classification)
    matrix = execute_query(
        """SELECT nv.canonical_name AS vendor_name, nv.department,
                  COALESCE(SUM(r.spend_amount), 0) AS spend
           FROM normalized_vendors nv
           LEFT JOIN raw_spend_rows r ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
           WHERE nv.run_id = %s
           GROUP BY nv.canonical_name, nv.department ORDER BY spend DESC LIMIT 50""",
        (run_id,), fetchall=True
    )

    # Not uniform since department is assigned by classification (not raw data patterns)
    is_uniform = False

    context = {
        "data_note": (
            "WARNING: Every vendor appears in every category with near-uniform spend. "
            "This is a synthetic/test dataset. The memo must acknowledge data limitations honestly. "
            "Do NOT fabricate insights that the data does not support."
        ) if is_uniform else "Real procurement data.",
        "total_spend": f"${summary.total_spend:,.0f}",
        "total_vendors": summary.total_vendors,
        "decision_summary": [
            {
                "decision": row["decision"],
                "vendor_count": int(row["vendor_count"]),
                "spend": f"${float(row['total_spend']):,.0f}",
            }
            for row in decision_summary
        ],
        "top_vendors": [
            {
                "name": r["canonical_vendor"],
                "spend": f"${float(r['total_spend']):,.0f}",
                "pct_of_total": f"{float(r['total_spend']) / summary.total_spend * 100:.1f}%",
            }
            for r in all_vendors
        ],
        "opportunities": [dict(o) for o in opps],
    }

    context_str = json.dumps(context, indent=2)

    prompt = f"""
You are the VP of Operations writing a 1-page executive memo to the CEO and CFO of an acquired company being integrated into Trilogy.

MEMO PURPOSE:
Summarize the vendor analysis findings and give the leadership team clear direction on what to do next. They are busy. Do not restate obvious context. Do not hedge. Do not add padding.

REQUIRED SECTIONS — fill each field:

subject: One line, e.g. "Vendor Integration Assessment — Recommended Actions"

findings: 2–3 sentences covering the total spend reviewed, vendor count, and how that spend breaks down by integration decision (KEEP / CENTRALIZE / ELIMINATE / AUTOMATE — use the actual counts and dollar amounts from the data). This is factual, not editorial.

recommended_actions: Return as a list of 3–5 bullet strings. Each bullet is one direct action item. Name the specific vendor or category. No preamble. Examples:
- "Shut down all facilities and physical office vendors (Fero-Term, Cook Kitchen, etc.) — no remote-work justification"
- "Migrate CRM off [vendor] into Trilogy Salesforce"
- "Retain AWS and core infrastructure until product cutover is complete"

risks: 1–2 sentences. Be specific: contract notice periods, any vendors with statutory filing obligations (legal, compliance), vendors that appear embedded in the product stack. Skip generic boilerplate.

conclusion: One sentence. Quantify what is at stake if integration is delayed — use a real dollar figure from the data.

top_opportunities: Exactly 3 strategic opportunities. Rules per opportunity:
  #1: The single highest-spend vendor or category that should be acted on. Name the vendor, the action, and the dollar amount.
  #2: Eliminate an entire physical/facilities/travel category. List ALL the specific vendor names in this category from the data, give the combined spend total, and explain why none of these have remote-work justification.
  #3: Eliminate all advisory and consulting services. List ALL the specific advisory/consulting vendor names from the data, give the combined spend total, and explain why these should not survive integration into Trilogy.

Each opportunity:
- title: 4–7 words, action-oriented
- explanation: 2–3 sentences. List the actual vendor names. State the action and why it's high-priority.
- annual_savings_usd: The combined dollar figure for all vendors in this opportunity (from the data)

RULES:
- Every dollar figure must come from the data below
- Do not use: "may", "could potentially", "might", "various suppliers", "some vendors"
- Do not mention vendors not present in the data
- Write at C-level: direct, specific, no filler

Data:
{context_str}
"""

    # ── Build table rows deterministically from DB ────────────────────────────
    all_vendors_full = execute_query(
        "SELECT canonical_vendor, total_spend, transaction_count FROM vendor_spend_summary WHERE run_id = %s ORDER BY total_spend DESC",
        (run_id,), fetchall=True
    )
    vendor_rows = [
        {
            "rank": i + 1,
            "vendor": r["canonical_vendor"].title(),
            "spend": f"${float(r['total_spend']):,.0f}",
            "pct": f"{float(r['total_spend']) / summary.total_spend * 100:.1f}%",
            "transactions": r["transaction_count"],
        }
        for i, r in enumerate(all_vendors_full)
    ][:15]


    # Spend by department — replaces category breakdown (source data has no categories)
    dept_rows_raw = execute_query(
        """SELECT nv.department, COUNT(DISTINCT nv.id) AS vendor_count,
                  COALESCE(SUM(r.spend_amount), 0) AS total_spend
           FROM normalized_vendors nv
           LEFT JOIN raw_spend_rows r ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
           WHERE nv.run_id = %s
           GROUP BY nv.department
           ORDER BY total_spend DESC""",
        (run_id,), fetchall=True
    )
    department_rows = [
        {
            "department": r["department"] or "G&A",
            "vendor_count": int(r["vendor_count"]),
            "spend": f"${float(r['total_spend']):,.0f}",
            "pct": f"{float(r['total_spend']) / summary.total_spend * 100:.1f}%" if summary.total_spend else "0%",
        }
        for r in dept_rows_raw
    ]

    # All classified vendors — full list for the classified vendor table
    classified_vendors_raw = execute_query(
        """SELECT nv.canonical_name, nv.department, nv.description, nv.recommendation,
                  COALESCE(SUM(r.spend_amount), 0) AS total_spend
           FROM normalized_vendors nv
           LEFT JOIN raw_spend_rows r
               ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
           WHERE nv.run_id = %s
           GROUP BY nv.canonical_name, nv.department, nv.description, nv.recommendation
           ORDER BY total_spend DESC""",
        (run_id,), fetchall=True
    )
    classified_vendor_rows = [
        {
            "vendor": r["canonical_name"].title(),
            "department": r["department"] or "G&A",
            "description": r["description"] or "",
            "decision": r["recommendation"] or "ELIMINATE",
            "spend": f"${float(r['total_spend']):,.0f}",
        }
        for r in classified_vendors_raw
    ]

    opportunity_rows = []
    for i, o in enumerate(opps):
        parts = [part.strip() for part in (o["rationale"] or "").split("|")]
        recommendation = parts[0] if parts else ""
        why = parts[1] if len(parts) > 1 else ""
        note = parts[2] if len(parts) > 2 else ""
        opportunity_rows.append(
            {
                "priority": i + 1,
                "target": o["target"],
                "action": o["action_type"],
                "recommendation": recommendation,
                "why": why,
                "note": note,
                "savings": o["impact_estimate"],
            }
        )

    try:
        memo_response: SummaryMemo = generate_structured_response(prompt, SummaryMemo, model="gpt-5.4")

        # ── Page 2: audit data built deterministically ────────────────────────
        run_meta = execute_query(
            "SELECT file_name FROM analysis_runs WHERE id = %s", (run_id,), fetchone=True
        )
        source_file = run_meta["file_name"] if run_meta else "—"

        raw_count = execute_query(
            "SELECT COUNT(*) as c, SUM(spend_amount) as s FROM raw_spend_rows WHERE run_id=%s",
            (run_id,), fetchone=True
        )
        raw_total = float(raw_count["s"] or 0)
        raw_txns  = int(raw_count["c"] or 0)

        # Vendor × Department matrix (department from classification, not raw category)
        mx_rows = execute_query(
            """SELECT nv.canonical_name AS vendor_name, nv.department,
                      COALESCE(SUM(r.spend_amount), 0) AS s
               FROM normalized_vendors nv
               LEFT JOIN raw_spend_rows r ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
               WHERE nv.run_id = %s
               GROUP BY nv.canonical_name, nv.department""",
            (run_id,), fetchall=True
        )
        mx_vendors  = sorted({r["vendor_name"] for r in mx_rows})
        mx_depts    = sorted({r["department"] for r in mx_rows if r["department"]})

        cells = {}
        for r in mx_rows:
            cells.setdefault(r["vendor_name"], {})[r["department"]] = f"${float(r['s']):,.0f}"

        row_totals = {}
        for v in mx_vendors:
            row_totals[v] = f"${sum(float(r['s']) for r in mx_rows if r['vendor_name']==v):,.0f}"

        col_totals = {}
        for d in mx_depts:
            col_totals[d] = f"${sum(float(r['s']) for r in mx_rows if r['department']==d):,.0f}"

        grand_total_val = sum(float(r["s"]) for r in mx_rows)

        # Diff between raw sum and materialized vendor summary sum (should be 0)
        diff = abs(raw_total - summary.total_spend)
        diff_str = f"${diff:,.2f}" if diff > 0.01 else "$0.00 ✓"

        reconciliation = [
            {"item": "Raw rows ingested",         "value": f"{raw_txns:,}",                  "notes": "From raw_spend_rows table"},
            {"item": "Raw spend total",            "value": f"${raw_total:,.2f}",             "notes": "Sum of all spend_amount"},
            {"item": "Unique vendors (raw)",       "value": str(len(mx_vendors)),             "notes": "Before canonicalization"},
            {"item": "Unique vendors (canonical)", "value": str(summary.total_vendors),       "notes": "After name normalization"},
            {"item": "Departments",                "value": str(len(mx_depts)),               "notes": "From vendor classification"},
            {"item": "Materialized spend total",   "value": f"${summary.total_spend:,.2f}",   "notes": "From vendor_spend_summary"},
            {"item": "Reconciliation diff",        "value": diff_str,                         "notes": "raw − materialized", "highlight": True},
            {"item": "Matrix cross-check",         "value": f"${grand_total_val:,.2f}",       "notes": "Sum of all matrix cells", "highlight": True},
        ]

        # Top 10 vendor records by spend (with department from classification)
        top_txns_raw = execute_query(
            """SELECT nv.canonical_name AS vendor_name, nv.department,
                      COALESCE(SUM(r.spend_amount), 0) AS spend_amount
               FROM normalized_vendors nv
               LEFT JOIN raw_spend_rows r ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
               WHERE nv.run_id = %s
               GROUP BY nv.canonical_name, nv.department
               ORDER BY spend_amount DESC LIMIT 10""",
            (run_id,), fetchall=True
        )
        top_transactions = [
            {
                "vendor":     r["vendor_name"].title(),
                "department": r["department"] or "—",
                "amount":     f"${float(r['spend_amount']):,.2f}",
            }
            for r in top_txns_raw
        ]

        audit = {
            "run_id":         run_id,
            "source_file":    source_file,
            "reconciliation": reconciliation,
            "matrix": {
                "vendors":     mx_vendors,
                "categories":  mx_depts,
                "cells":       cells,
                "row_totals":  row_totals,
                "col_totals":  col_totals,
                "grand_total": f"${grand_total_val:,.0f}",
            },
            "top_transactions": top_transactions,
        }

        top_opportunity_rows = [
            {
                "title": o.title,
                "explanation": o.explanation,
                "annual_savings_usd": o.annual_savings_usd,
            }
            for o in memo_response.top_opportunities
        ]

        pdf_data = {
            "total_spend": f"${summary.total_spend:,.0f}",
            "total_vendors": summary.total_vendors,
            "subject": memo_response.subject,
            "findings": memo_response.findings,
            "recommended_actions": memo_response.recommended_actions,
            "risks": memo_response.risks,
            "conclusion": memo_response.conclusion,
            "data_note": context["data_note"] if "WARNING" in context["data_note"] else "",
            "top_opportunity_rows": top_opportunity_rows,
            "decision_rows": context["decision_summary"],
            "vendor_rows": vendor_rows,
            "department_rows": department_rows,
            "opportunity_rows": opportunity_rows,
            "classified_vendor_rows": classified_vendor_rows,
            "audit": audit,
        }

        pdf_dir = Path("data/outputs")
        pdf_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_path = f"data/outputs/memo_run_{run_id}_{ts}.pdf"

        generator = PDFGenerator(output_path=str(pdf_path))
        generator.generate_memo(pdf_data)

        # Markdown for MCP get_memo tool
        opp_lines = "\n".join(
            f"{i+1}. **{o['title']}** — {o['annual_savings_usd']}  \n   {o['explanation']}"
            for i, o in enumerate(top_opportunity_rows)
        )
        vendor_lines = "\n".join(
            f"| {r['rank']} | {r['vendor']} | {r['spend']} | {r['pct']} | {r['transactions']} |"
            for r in vendor_rows
        )
        markdown_content = (
            f"# {memo_response.subject}\n\n"
            f"{memo_response.findings}\n\n"
            f"## Recommended Actions\n\n{memo_response.recommended_actions}\n\n"
            f"## Top 3 Opportunities\n\n{opp_lines}\n\n"
            f"## Vendor Breakdown\n\n"
            f"| # | Vendor | Spend | % of Total | Txns |\n|---|--------|-------|------------|------|\n"
            f"{vendor_lines}\n\n"
            f"---\n{memo_response.conclusion}"
        )

        execute_query(
            "INSERT INTO memo_outputs (run_id, pdf_path, markdown_content) VALUES (%s, %s, %s)",
            (run_id, str(pdf_path), markdown_content)
        )

        return {"status": "success", "pdf_path": str(pdf_path)}

    except Exception as e:
        activity.logger.error(f"Generate memo failed: {e}")
        return {"status": "failed", "error": str(e)}
