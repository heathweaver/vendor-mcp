from temporalio import activity
import json
from services.postgres import get_connection, execute_query
from services.llm_client import generate_structured_response
from services.llm_schemas import OpportunitiesResponse
from services.analysis_engine import compute_analysis

@activity.defn
async def analyze_opportunities(run_id: int) -> dict:
    """
    Feeds pre-computed deterministic metrics to the LLM to get specific,
    dollar-quantified recommendations (renegotiate, consolidate, eliminate, automate).
    """
    try:
        summary = compute_analysis(run_id)
    except Exception as e:
        activity.logger.error(f"Analysis engine failed: {e}")
        return {"status": "failed", "error": str(e)}

    if summary.total_vendors == 0:
        return {"status": "skipped", "reason": "no data to analyze"}

    # Top 20 vendors by spend (enough for renegotiation analysis without overflow)
    all_vendors = execute_query(
        "SELECT canonical_vendor, total_spend, transaction_count FROM vendor_spend_summary WHERE run_id = %s ORDER BY total_spend DESC LIMIT 20",
        (run_id,), fetchall=True
    )
    # Top 15 categories by spend
    all_cats = execute_query(
        "SELECT category, total_spend, vendor_count FROM category_spend_summary WHERE run_id = %s ORDER BY total_spend DESC LIMIT 15",
        (run_id,), fetchall=True
    )
    # Per-vendor per-category breakdown — top 50 rows by spend to stay within LLM context
    matrix = execute_query(
        """SELECT vendor_name, category, SUM(spend_amount) as spend, COUNT(*) as txns
           FROM raw_spend_rows WHERE run_id = %s
           GROUP BY vendor_name, category ORDER BY spend DESC LIMIT 50""",
        (run_id,), fetchall=True
    )
    tail_count = execute_query(
        "SELECT COUNT(*) as c FROM tail_spend_summary WHERE run_id = %s",
        (run_id,), fetchone=True
    )

    # Detect uniform distribution (every vendor appears in every category) — synthetic data flag
    cat_names = [r["category"] for r in all_cats]
    vendor_names = [r["canonical_vendor"] for r in all_vendors]
    matrix_keys = {(r["vendor_name"], r["category"]) for r in matrix}
    is_uniform = all(
        any(v.lower() in vn.lower() or vn.lower() in v.lower() for vn in [m[0] for m in matrix_keys if m[1] == c])
        for v in vendor_names for c in cat_names
    )

    context = {
        "total_spend": f"${summary.total_spend:,.0f}",
        "total_vendors": summary.total_vendors,
        "data_note": (
            "WARNING: Every vendor appears in every category with near-uniform distribution. "
            "This pattern indicates synthetic or test data. Consolidation recommendations "
            "should reflect this — do not recommend consolidating categories where all vendors "
            "sell the same mix of goods."
        ) if is_uniform else "Real procurement data.",
        "vendors": [
            {
                "name": r["canonical_vendor"],
                "total_spend": f"${float(r['total_spend']):,.0f}",
                "pct_of_total": f"{float(r['total_spend']) / summary.total_spend * 100:.1f}%",
                "transactions": r["transaction_count"],
                "renegotiation_range_5_to_12pct": f"${float(r['total_spend']) * 0.05:,.0f}–${float(r['total_spend']) * 0.12:,.0f}",
            }
            for r in all_vendors
        ],
        "categories": [
            {
                "name": r["category"],
                "total_spend": f"${float(r['total_spend']):,.0f}",
                "vendor_count": r["vendor_count"],
                "consolidation_savings_range_15_to_25pct": f"${float(r['total_spend']) * 0.15:,.0f}–${float(r['total_spend']) * 0.25:,.0f}",
            }
            for r in all_cats
        ],
        "vendor_category_matrix": [
            {
                "vendor": r["vendor_name"],
                "category": r["category"],
                "spend": f"${float(r['spend']):,.0f}",
                "transactions": r["txns"],
            }
            for r in matrix
        ],
        "tail_vendor_count": int(tail_count["c"]),
    }

    context_str = json.dumps(context, indent=2)

    CATEGORY_BENCHMARKS = {
        "Electronics":   ("6–14%", "competitive rebid"),
        "Software":      ("8–20%", "renewal negotiation or competitive alternative"),
        "IT":            ("8–18%", "competitive rebid"),
        "Logistics":     ("5–12%", "volume consolidation and lane rebid"),
        "Indirect":      ("10–20%", "preferred-vendor consolidation"),
        "Direct":        ("4–10%", "volume commitment and competitive sourcing"),
        "Marketing":     ("10–25%", "agency review and scope reduction"),
        "Professional Services": ("8–15%", "rate card renegotiation"),
        "Facilities":    ("6–12%", "bundled services contract"),
        "Travel":        ("8–15%", "policy enforcement and preferred-supplier program"),
        "Other":         ("5–12%", "general competitive review"),
    }

    # Build benchmark hint for context
    cat_benchmarks = {}
    for cat in all_cats:
        name = cat["category"]
        for key, (range_, method) in CATEGORY_BENCHMARKS.items():
            if key.lower() in name.lower():
                cat_benchmarks[name] = {"benchmark_range": range_, "method": method}
                break
        if name not in cat_benchmarks:
            cat_benchmarks[name] = {"benchmark_range": "5–12%", "method": "general competitive review"}

    context["category_benchmarks"] = cat_benchmarks

    context_str = json.dumps(context, indent=2)

    prompt = f"""
You are a senior procurement analyst producing findings for a CEO/CFO review.

Your job is to identify the 3–6 highest-value savings opportunities from the spend data below.
Rank them by ease of execution (fastest to implement first), not just dollar size.

DATA QUALITY:
{context.get('data_note', 'Real procurement data.')}

For each opportunity, produce three levels of analysis:
1. PATTERN: What does the data show? (cite specific numbers)
2. IMPLICATION: What does this mean operationally?
3. RECOMMENDATION: What is the specific action, owner role, and timeline?

LABELLING RULES — apply one of these tags to every claim in the rationale:
- [data] — directly computed from the numbers provided
- [benchmark] — based on industry benchmarks (cite the source assumption explicitly)
- [structural] — an observation about the shape/distribution of data, not a value claim

SAVINGS ESTIMATE RULES:
- Use the pre-computed renegotiation_range_5_to_12pct for renegotiation targets
- Use the pre-computed consolidation_savings_range_15_to_25pct for consolidation targets  
- Use category_benchmarks[category].benchmark_range when a category-specific benchmark applies — label it [benchmark: "source assumption"]
- Never present a range as if it came from the data when it is a benchmark assumption

STRUCTURAL OBSERVATIONS (always include at least one even if data is weak):
- If all vendors appear in all categories equally: this is a procurement-control problem (no preferred-vendor discipline), not a supplier fragmentation problem
- If spend is concentrated in top 2 vendors: primary lever is renegotiation, not consolidation
- If buyer field is available and dispersed: flag as governance/compliance issue, not just a vendor issue
- If a vendor dominates a category (>40% of category spend): flag as single-vendor dependency risk

FORBIDDEN:
- Do not invent vendor names, category names, or figures not in the data
- Do not recommend consolidating a category where all vendors appear equally — the problem is buyer controls
- Do not produce more than 6 opportunities; fewer is better if data doesn't support more

Data:
{context_str}
"""

    try:
        response: OpportunitiesResponse = generate_structured_response(
            prompt, OpportunitiesResponse, model="gpt-5.4"
        )

        with get_connection() as conn:
            with conn.cursor() as cur:
                for opp in response.opportunities:
                    cur.execute(
                        "INSERT INTO savings_opportunities (run_id, target, action_type, rationale, impact_estimate) VALUES (%s, %s, %s, %s, %s)",
                        (run_id, opp.target, opp.action_type, opp.rationale, opp.impact_estimate)
                    )
            conn.commit()

        return {"status": "success", "opportunities_count": len(response.opportunities)}

    except Exception as e:
        activity.logger.error(f"LLM Opportunities analysis failed: {e}")
        return {"status": "failed", "error": str(e)}
