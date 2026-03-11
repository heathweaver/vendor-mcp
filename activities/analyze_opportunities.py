from temporalio import activity

from services.postgres import get_connection
from services.llm_client import generate_structured_response
from services.llm_schemas import OpportunitiesResponse


def _money(value: float) -> str:
    return f"${value:,.0f}"


@activity.defn
async def analyze_opportunities(run_id: int) -> dict:
    """
    Generate tight integration recommendations from classified vendors.

    The output is intentionally concise:
    - recommendation: short action
    - rationale: short why
    - implementation_note: one sentence with timing/constraint
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    nv.canonical_name AS vendor_name,
                    COALESCE(nv.department, 'G&A') AS department,
                    COALESCE(nv.description, '') AS description,
                    COALESCE(nv.recommendation, 'ELIMINATE') AS decision,
                    COALESCE(SUM(r.spend_amount), 0) AS total_spend,
                    COUNT(r.id) AS transaction_count
                FROM normalized_vendors nv
                LEFT JOIN raw_spend_rows r
                    ON r.run_id = nv.run_id AND r.vendor_name = nv.original_name
                WHERE nv.run_id = %s
                GROUP BY nv.canonical_name, nv.department, nv.description, nv.recommendation
                ORDER BY total_spend DESC, nv.canonical_name
                """,
                (run_id,),
            )
            vendors = [dict(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT
                    COALESCE(recommendation, 'ELIMINATE') AS decision,
                    COUNT(*) AS vendor_count,
                    COALESCE(SUM(spend.total_spend), 0) AS total_spend
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
                ) spend ON spend.vendor_id = nv.id
                WHERE nv.run_id = %s
                GROUP BY COALESCE(recommendation, 'ELIMINATE')
                ORDER BY total_spend DESC
                """,
                (run_id, run_id),
            )
            decision_rollup = [dict(row) for row in cur.fetchall()]

            cur.execute(
                """
                DELETE FROM savings_opportunities
                WHERE run_id = %s
                """,
                (run_id,),
            )
        conn.commit()

    if not vendors:
        return {"status": "skipped", "reason": "no classified vendors found"}

    total_spend = sum(float(v["total_spend"] or 0) for v in vendors)
    decision_counts = {row["decision"]: int(row["vendor_count"]) for row in decision_rollup}
    decision_spend = {row["decision"]: float(row["total_spend"] or 0) for row in decision_rollup}

    ranked_vendors = {
        decision: [
            {
                "vendor_name": row["vendor_name"],
                "department": row["department"],
                "description": row["description"],
                "spend": _money(float(row["total_spend"] or 0)),
                "transaction_count": int(row["transaction_count"] or 0),
            }
            for row in vendors
            if row["decision"] == decision
        ][:8]
        for decision in ("CENTRALIZE", "ELIMINATE", "AUTOMATE", "KEEP")
    }

    context = {
        "run_id": run_id,
        "total_spend": _money(total_spend),
        "decision_rollup": {
            decision: {
                "vendor_count": decision_counts.get(decision, 0),
                "spend": _money(decision_spend.get(decision, 0.0)),
            }
            for decision in ("KEEP", "CENTRALIZE", "ELIMINATE", "AUTOMATE")
        },
        "top_vendors_by_decision": ranked_vendors,
    }

    prompt = f"""
You are the integration lead for a newly acquired software company being absorbed into Trilogy.

Write 4 to 6 top integration recommendations from the classified vendor list below.

Operating rules:
- KEEP means mission-critical product infrastructure. These are not primary savings recommendations unless there is a clear short transition note.
- CENTRALIZE means duplicate business tooling that should move into Trilogy's platform.
- ELIMINATE means non-core spend that should be shut down.
- AUTOMATE means work that should be replaced by internal AI or automation.
- Prefer CENTRALIZE, ELIMINATE, and AUTOMATE over KEEP.
- Do not waste a recommendation on generic renegotiation language.
- Focus on what action happens and why in very few words.

Output requirements:
- `recommendation` must be 3 to 8 words and start with a verb.
- `rationale` must be 2 to 6 words and read like a label, not a sentence.
- `implementation_note` must be exactly one sentence and mention dependency, timing, or contract constraints.
- `impact_estimate` should usually equal the spend being retired or migrated off-platform; for KEEP items use '$0 while retained' unless there is a justified savings figure in the data.
- Use action_type values only from: keep, migrate, eliminate, automate.
- Use only vendors present in the data.
- Rank by strategic importance and spend.

Context:
{context}
"""

    try:
        response: OpportunitiesResponse = generate_structured_response(
            prompt,
            OpportunitiesResponse,
            system_prompt="You are a Trilogy integration operator. Be terse, concrete, and operational.",
            model="gpt-5.4",
        )
    except Exception as e:
        activity.logger.error(f"Integration recommendation generation failed: {e}")
        return {"status": "failed", "error": str(e)}

    with get_connection() as conn:
        with conn.cursor() as cur:
            for opp in response.opportunities:
                rationale = f"{opp.recommendation} | {opp.rationale}"
                if opp.implementation_note:
                    rationale = f"{rationale} | {opp.implementation_note}"
                cur.execute(
                    """
                    INSERT INTO savings_opportunities (run_id, target, action_type, rationale, impact_estimate)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (run_id, opp.target, opp.action_type, rationale, opp.impact_estimate),
                )
        conn.commit()

    return {"status": "success", "opportunities_count": len(response.opportunities)}
