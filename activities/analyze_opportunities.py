from temporalio import activity
import json
from services.postgres import get_connection, execute_query
from services.llm_client import generate_structured_response
from services.llm_schemas import OpportunitiesResponse

@activity.defn
async def analyze_opportunities(run_id: int) -> dict:
    """
    Feeds aggregated SQL views to the LLM to get strategic recommendations
    (renegotiate, consolidate, eliminate, automate).
    """
    # 1. Fetch vendor spend
    vendors = execute_query(
        "SELECT canonical_vendor, total_spend, transaction_count FROM vendor_spend_summary WHERE run_id = %s ORDER BY total_spend DESC LIMIT 20",
        (run_id,), fetchall=True
    )
    
    # 2. Fetch categories
    cats = execute_query(
        "SELECT category, total_spend, vendor_count FROM category_spend_summary WHERE run_id = %s ORDER BY total_spend DESC",
        (run_id,), fetchall=True
    )
    
    if not vendors or not cats:
        return {"status": "skipped", "reason": "no data to analyze"}
        
    context_str = json.dumps({
        "top_vendors": [dict(v) for v in vendors],
        "category_breakdown": [dict(c) for c in cats]
    }, indent=2, default=str)
    
    prompt = f"""
    Review this company's aggregated vendor spend.
    Identify 3-5 specific opportunities using one of these actions: renegotiate, consolidate, eliminate, or automate.
    Focus on fragmented categories or remarkably high concentration vendors.
    
    Data Context:
    {context_str}
    """
    
    try:
        response: OpportunitiesResponse = generate_structured_response(prompt, OpportunitiesResponse)
        
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
