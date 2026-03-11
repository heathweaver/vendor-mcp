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
    Generates the final PDF memo using pre-computed analysis metrics and LLM synthesis.
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

    # Per-vendor per-category matrix — top 50 by spend to stay within LLM context
    matrix = execute_query(
        """SELECT vendor_name, category, SUM(spend_amount) as spend
           FROM raw_spend_rows WHERE run_id = %s
           GROUP BY vendor_name, category ORDER BY spend DESC LIMIT 50""",
        (run_id,), fetchall=True
    )
    all_vendors = execute_query(
        "SELECT canonical_vendor, total_spend FROM vendor_spend_summary WHERE run_id = %s ORDER BY total_spend DESC LIMIT 20",
        (run_id,), fetchall=True
    )
    all_cats = execute_query(
        "SELECT category, total_spend, vendor_count FROM category_spend_summary WHERE run_id = %s ORDER BY total_spend DESC LIMIT 15",
        (run_id,), fetchall=True
    )

    # Flag synthetic/uniform data
    cat_names = [r["category"] for r in all_cats]
    vendor_names = [r["canonical_vendor"] for r in all_vendors]
    matrix_keys = {(r["vendor_name"], r["category"]) for r in matrix}
    is_uniform = len(matrix_keys) == len(cat_names) * len(vendor_names)

    context = {
        "data_note": (
            "WARNING: Every vendor appears in every category with near-uniform spend. "
            "This is a synthetic/test dataset. The memo must acknowledge data limitations honestly. "
            "Do NOT fabricate insights that the data does not support."
        ) if is_uniform else "Real procurement data.",
        "total_spend": f"${summary.total_spend:,.0f}",
        "total_vendors": summary.total_vendors,
        "vendors": [
            {
                "name": r["canonical_vendor"],
                "spend": f"${float(r['total_spend']):,.0f}",
                "pct_of_total": f"{float(r['total_spend']) / summary.total_spend * 100:.1f}%",
            }
            for r in all_vendors
        ],
        "categories": [
            {
                "name": r["category"],
                "spend": f"${float(r['total_spend']):,.0f}",
                "vendor_count": r["vendor_count"],
            }
            for r in all_cats
        ],
        "vendor_category_matrix": [
            {
                "vendor": r["vendor_name"],
                "category": r["category"],
                "spend": f"${float(r['spend']):,.0f}",
            }
            for r in matrix
        ],
        "opportunities": [dict(o) for o in opps],
    }

    context_str = json.dumps(context, indent=2)

    prompt = f"""
You are writing a CEO/CFO procurement memo. Your output will be rendered into a formal document.

MEMO STRUCTURE — follow this order exactly:
1. SCOPE (headline): One sentence. What was reviewed: total spend, number of vendors, period if known.
2. FINDINGS (executive_summary): 3–4 sentences. Lead with the single most important finding by dollar impact. Then the second finding. Then one sentence on what the data cannot tell us (be honest about limitations). Do NOT start with scope — the headline covers that.
3. IMMEDIATE ACTIONS: Built from the opportunities list — leave immediate_actions empty, the table is rendered separately.
4. CONCLUSION: One sentence on consequence of inaction. Must cite a specific dollar figure from the opportunities. Do NOT include the MCP server reference here — it goes in the footer.

DATA QUALITY HANDLING:
- data_note = "{context.get('data_note', 'Real procurement data.')}"
- If data_note flags synthetic/uniform data: findings must include one sentence acknowledging the structural limitation (e.g. "vendor specialization cannot be assessed from this dataset"). Recommendations remain valid structurally even if dollar precision is limited.
- If real data: write with full confidence, no hedging language.

QUALITY RULES:
- Every dollar figure in executive_summary must come from the opportunities or vendor list — label assumption-based figures as "[benchmark assumption]"
- Do not use: "may", "could potentially", "might", "various suppliers", "some vendors"
- Do not mention vendors not present in the data
- The headline states scope, not savings — savings go in executive_summary

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
    ]

    # Per-category: find largest vendor by spend in that category
    cat_rows = []
    for r in all_cats:
        top = execute_query(
            """SELECT vendor_name, SUM(spend_amount) as s
               FROM raw_spend_rows WHERE run_id=%s AND category=%s
               GROUP BY vendor_name ORDER BY s DESC LIMIT 1""",
            (run_id, r["category"]), fetchone=True
        )
        cat_spend = float(r["total_spend"])
        top_vendor = top["vendor_name"] if top else "—"
        top_pct = f"{float(top['s']) / cat_spend * 100:.0f}%" if top and cat_spend else "—"
        cat_rows.append({
            "category": r["category"],
            "spend": f"${cat_spend:,.0f}",
            "vendors": r["vendor_count"],
            "top_vendor": top_vendor,
            "top_pct": top_pct,
        })

    opportunity_rows = [
        {
            "priority": i + 1,
            "target": o["target"],
            "action": o["action_type"],
            "savings": o["impact_estimate"],
            "rationale": o["rationale"],
        }
        for i, o in enumerate(opps)
    ]

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

        # Vendor×category matrix
        mx_rows = execute_query(
            """SELECT vendor_name, category, SUM(spend_amount) as s, COUNT(*) as txns
               FROM raw_spend_rows WHERE run_id=%s
               GROUP BY vendor_name, category""",
            (run_id,), fetchall=True
        )
        mx_vendors = sorted({r["vendor_name"] for r in mx_rows})
        mx_cats    = [r["category"] for r in all_cats]  # already sorted by spend desc

        cells = {}
        for r in mx_rows:
            cells.setdefault(r["vendor_name"], {})[r["category"]] = f"${float(r['s']):,.0f}"

        row_totals = {}
        for v in mx_vendors:
            row_totals[v] = f"${sum(float(r['s']) for r in mx_rows if r['vendor_name']==v):,.0f}"

        col_totals = {}
        for c in mx_cats:
            col_totals[c] = f"${sum(float(r['s']) for r in mx_rows if r['category']==c):,.0f}"

        grand_total_val = sum(float(r["s"]) for r in mx_rows)

        # Diff between raw sum and materialized vendor summary sum (should be 0)
        diff = abs(raw_total - summary.total_spend)
        diff_str = f"${diff:,.2f}" if diff > 0.01 else "$0.00 ✓"

        reconciliation = [
            {"item": "Raw rows ingested",        "value": f"{raw_txns:,}",                   "notes": "From raw_spend_rows table"},
            {"item": "Raw spend total",           "value": f"${raw_total:,.2f}",              "notes": "Sum of all spend_amount"},
            {"item": "Unique vendors (raw)",      "value": str(len(mx_vendors)),              "notes": "Before canonicalization"},
            {"item": "Unique vendors (canonical)","value": str(summary.total_vendors),        "notes": "After name normalization"},
            {"item": "Categories",                "value": str(len(mx_cats)),                 "notes": "From category_spend_summary"},
            {"item": "Materialized spend total",  "value": f"${summary.total_spend:,.2f}",    "notes": "From vendor_spend_summary"},
            {"item": "Reconciliation diff",       "value": diff_str,                          "notes": "raw − materialized", "highlight": True},
            {"item": "Matrix cross-check",        "value": f"${grand_total_val:,.2f}",        "notes": "Sum of all matrix cells", "highlight": True},
        ]

        # Top 10 transactions
        top_txns_raw = execute_query(
            """SELECT vendor_name, category, spend_amount, spend_date
               FROM raw_spend_rows WHERE run_id=%s
               ORDER BY spend_amount DESC LIMIT 10""",
            (run_id,), fetchall=True
        )
        top_transactions = [
            {
                "vendor":   r["vendor_name"],
                "category": r["category"],
                "amount":   f"${float(r['spend_amount']):,.2f}",
                "date":     str(r["spend_date"])[:10] if r["spend_date"] else "—",
            }
            for r in top_txns_raw
        ]

        audit = {
            "run_id":         run_id,
            "source_file":    source_file,
            "reconciliation": reconciliation,
            "matrix": {
                "vendors":     mx_vendors,
                "categories":  mx_cats,
                "cells":       cells,
                "row_totals":  row_totals,
                "col_totals":  col_totals,
                "grand_total": f"${grand_total_val:,.0f}",
            },
            "top_transactions": top_transactions,
        }

        pdf_data = {
            "company": "Executive Team",
            "total_spend": f"${summary.total_spend:,.0f}",
            "headline": memo_response.headline,
            "executive_summary": memo_response.executive_summary,
            "conclusion": memo_response.conclusion,
            "data_note": context["data_note"] if "WARNING" in context["data_note"] else "",
            "vendor_rows": vendor_rows,
            "category_rows": cat_rows,
            "opportunity_rows": opportunity_rows,
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
            f"{r['priority']}. **[{r['action'].upper()}] {r['target']}** — {r['savings']}  \n   {r['rationale']}"
            for r in opportunity_rows
        )
        vendor_lines = "\n".join(
            f"| {r['rank']} | {r['vendor']} | {r['spend']} | {r['pct']} | {r['transactions']} |"
            for r in vendor_rows
        )
        markdown_content = (
            f"# {memo_response.headline}\n\n"
            f"{memo_response.executive_summary}\n\n"
            f"## Vendor Breakdown\n\n"
            f"| # | Vendor | Spend | % of Total | Txns |\n|---|--------|-------|------------|------|\n"
            f"{vendor_lines}\n\n"
            f"## Savings Opportunities\n\n{opp_lines}\n\n"
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
