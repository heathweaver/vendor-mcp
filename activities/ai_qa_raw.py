from temporalio import activity
import json
from services.postgres import get_connection, execute_query
from services.llm_client import generate_structured_response
from services.llm_schemas import QAResponse

@activity.defn
async def ai_qa_raw(run_id: int) -> dict:
    # 1. Fetch a sample of raw rows (up to 50 for cost/context limits)
    rows = execute_query(
        "SELECT vendor_name, spend_amount, category, description FROM raw_spend_rows WHERE run_id = %s LIMIT 50",
        (run_id,), fetchall=True
    )
    if not rows:
        return {"status": "skipped", "reason": "no data"}
        
    data_str = json.dumps([dict(r) for r in rows], indent=2, default=str)
    prompt = f"Review this raw spend data sample for obvious data quality issues (e.g. missing amounts, impossible dates, encoding errors):\n{data_str}"
    
    try:
        response: QAResponse = generate_structured_response(prompt, QAResponse)
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                for finding in response.findings:
                    cur.execute(
                        "INSERT INTO qa_findings (run_id, issue_type, description, severity) VALUES (%s, %s, %s, %s)",
                        (run_id, finding.issue_type, finding.description, finding.severity)
                    )
            conn.commit()
            
        return {"status": "success", "findings_count": len(response.findings)}
        
    except Exception as e:
        activity.logger.error(f"LLM QA failed: {e}")
        return {"status": "failed", "error": str(e)}
